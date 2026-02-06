//! Property filter expression language.
//!
//! ```
//! <expr>   := <expr> || <expr>
//!           | <expr> && <expr>
//!           | !(<expr>)
//!           | (<expr>)
//!           | <key> <op> <value>
//!           | <key> in [<value>, ...]
//!           | <key> like <string>
//!           | has? <key>
//!
//! <op>     := = | != | < | <= | > | >=
//! <key>    := <word> | <string>
//! <value>  := <word> | <string> | <number> | true | false
//! <string> := "..." | '...'
//! <word>   := [a-zA-Z0-9_]+
//! ```

use std::str::{Chars, FromStr};

use anyhow::{Result, anyhow};
use rusqlite::ToSql;
use rusqlite::types::ToSqlOutput;
use serde::{Deserialize, Deserializer};

#[derive(Debug, Clone)]
pub struct PropertyFilter {
    expr: FilterExpr,
}

impl PropertyFilter {
    pub fn to_sql(&self) -> (String, Vec<&dyn ToSql>) {
        let mut str = String::with_capacity(128);
        let mut params = Vec::new();

        self.expr.to_sql(&mut str, &mut params);
        (str, params)
    }
}

#[derive(Debug, Clone)]
enum Value {
    String(String),
    Number(f64),
    Bool(bool),
}

#[derive(Debug, Copy, Clone)]
enum ComparisonOp {
    Lt,
    Lte,
    Gt,
    Gte,
    Eq,
    Neq,
}

#[derive(Debug, Clone)]
enum FilterExpr {
    Comparison(String, ComparisonOp, Value),
    OneOf(String, Vec<Value>),
    HasKey(String),
    Like(String, String),
    And(Box<FilterExpr>, Box<FilterExpr>),
    Or(Box<FilterExpr>, Box<FilterExpr>),
    Not(Box<FilterExpr>),
}

impl FilterExpr {
    fn parse(input: &str) -> Result<Self> {
        let mut parser = FilterParser::new(input);

        let expr = parser
            .read_or_expr()
            .map_err(|err| anyhow!("parse error at position {}: {:?}", parser.pos, err))?;

        if parser.peek().is_some() {
            anyhow::bail!("unexpected: {:?}", parser.rest());
        }

        Ok(expr)
    }

    fn to_sql<'a>(&'a self, str: &mut String, params: &mut Vec<&'a dyn ToSql>) {
        str.push('(');
        match self {
            FilterExpr::And(lhs, rhs) => {
                lhs.to_sql(str, params);
                str.push_str(" AND ");
                rhs.to_sql(str, params);
            }
            FilterExpr::Or(lhs, rhs) => {
                lhs.to_sql(str, params);
                str.push_str(" OR ");
                rhs.to_sql(str, params);
            }
            FilterExpr::Not(expr) => {
                str.push_str("NOT ");
                expr.to_sql(str, params);
            }
            FilterExpr::Comparison(key, op, value) => {
                let op_str = match op {
                    ComparisonOp::Lt => "<",
                    ComparisonOp::Lte => "<=",
                    ComparisonOp::Gt => ">",
                    ComparisonOp::Gte => ">=",
                    ComparisonOp::Eq => "=",
                    ComparisonOp::Neq => "!=",
                };

                str.push_str("properties ->> ? ");
                str.push_str(op_str);
                str.push_str(" ?");

                params.push(key);
                params.push(value);
            }
            FilterExpr::OneOf(key, values) => {
                str.push_str("properties ->> ? IN (");
                params.push(key);

                for (i, value) in values.iter().enumerate() {
                    if i > 0 {
                        str.push_str(", ");
                    }
                    str.push('?');
                    params.push(value);
                }
                str.push(')');
            }
            FilterExpr::HasKey(key) => {
                str.push_str("properties ->> ? IS NOT NULL");
                params.push(key);
            }
            FilterExpr::Like(key, pattern) => {
                str.push_str("properties ->> ? LIKE ?");
                params.push(key);
                params.push(pattern);
            }
        }
        str.push(')');
    }
}

impl ToSql for Value {
    fn to_sql(&self) -> rusqlite::Result<ToSqlOutput<'_>> {
        Ok(match self {
            Value::String(s) => ToSqlOutput::from(s.as_str()),
            Value::Number(n) => (*n).into(),
            Value::Bool(b) => (*b).into(),
        })
    }
}

#[derive(Debug)]
struct FilterParser<'a> {
    chars: Chars<'a>,
    pos: usize,
}

impl<'a> FilterParser<'a> {
    fn new(input: &'a str) -> Self {
        FilterParser {
            chars: input.chars(),
            pos: 0,
        }
    }

    fn rest(&self) -> &'a str {
        self.chars.as_str()
    }

    fn read_or_expr(&mut self) -> Result<FilterExpr> {
        let mut lhs = self.read_and_expr()?;
        while self.consume("||") {
            let rhs = self.read_and_expr()?;
            lhs = FilterExpr::Or(Box::new(lhs), Box::new(rhs));
        }
        Ok(lhs)
    }

    fn read_and_expr(&mut self) -> Result<FilterExpr> {
        let mut lhs = self.read_unary()?;
        while self.consume("&&") {
            let rhs = self.read_unary()?;
            lhs = FilterExpr::And(Box::new(lhs), Box::new(rhs));
        }
        Ok(lhs)
    }

    fn read_unary(&mut self) -> Result<FilterExpr> {
        self.skip_whitespace();

        if self.consume("!") {
            self.expect("(")?;
            let expr = self.read_or_expr()?;
            self.expect(")")?;
            Ok(FilterExpr::Not(Box::new(expr)))
        } else if self.consume("has?") {
            let key = self.read_key()?;
            Ok(FilterExpr::HasKey(key))
        } else {
            self.read_expr()
        }
    }

    fn read_expr(&mut self) -> Result<FilterExpr> {
        self.skip_whitespace();

        if self.consume("(") {
            let expr = self.read_or_expr()?;
            self.expect(")")?;
            return Ok(expr);
        }

        let key = self.read_key()?;

        let expr = if self.consume("in") {
            let list = self.read_list()?;
            FilterExpr::OneOf(key, list)
        } else if self.consume("like") {
            let pattern = self.read_string()?;
            FilterExpr::Like(key, pattern)
        } else {
            let op = self.read_binary_op()?;
            let rhs = self.read_value()?;
            FilterExpr::Comparison(key, op, rhs)
        };

        Ok(expr)
    }

    fn read_key(&mut self) -> Result<String> {
        self.skip_whitespace();
        match self.read_value()? {
            Value::String(s) => Ok(s),
            x => anyhow::bail!("expected key, got {:?}", x),
        }
    }

    fn read_list(&mut self) -> Result<Vec<Value>> {
        self.skip_whitespace();
        self.expect("[")?;

        let mut items = vec![];
        while let Some(ch) = self.peek()
            && ch != ']'
        {
            items.push(self.read_value()?);
            if !self.consume(",") {
                break;
            }
        }
        self.expect("]")?;

        Ok(items)
    }

    fn read_value(&mut self) -> Result<Value> {
        self.skip_whitespace();

        Ok(match self.peek() {
            Some(ch) if ch == '"' || ch == '\'' => Value::String(self.read_string()?),
            Some(ch) if ch.is_numeric() || ch == '-' => Value::Number(self.read_number()?),
            Some(_) => {
                let word = self.read_word()?;
                match word.as_str() {
                    "true" => Value::Bool(true),
                    "false" => Value::Bool(false),
                    _ => Value::String(word),
                }
            }
            None => anyhow::bail!("expected value, hit eof"),
        })
    }

    fn read_binary_op(&mut self) -> Result<ComparisonOp> {
        self.skip_whitespace();

        match (self.next(), self.peek()) {
            (Some('<'), Some('=')) => {
                self.drop(1);
                Ok(ComparisonOp::Lte)
            }
            (Some('>'), Some('=')) => {
                self.drop(1);
                Ok(ComparisonOp::Gte)
            }
            (Some('!'), Some('=')) => {
                self.drop(1);
                Ok(ComparisonOp::Neq)
            }
            (Some('='), _) => Ok(ComparisonOp::Eq),
            (Some('<'), _) => Ok(ComparisonOp::Lt),
            (Some('>'), _) => Ok(ComparisonOp::Gt),
            (_, _) => anyhow::bail!("unknown operator"),
        }
    }

    fn read_word(&mut self) -> Result<String> {
        self.skip_whitespace();

        let word: String = self
            .chars
            .clone()
            .take_while(|ch| ch.is_alphanumeric() || *ch == '_')
            .collect();

        if word.is_empty() {
            anyhow::bail!("empty identifier");
        }

        self.drop(word.chars().count());
        Ok(word)
    }

    fn read_string(&mut self) -> Result<String> {
        self.skip_whitespace();
        let Some(delim) = self.next() else {
            return Err(anyhow!("expected string"));
        };

        let str: String = self.chars.clone().take_while(|ch| *ch != delim).collect();

        if !str.is_empty() {
            self.drop(str.chars().count());
        }

        // Next char is the delimiter. if not present, we hit end too soon
        if self.next().is_none() {
            anyhow::bail!("unterminated string");
        }

        Ok(str)
    }

    fn read_number(&mut self) -> Result<f64> {
        self.skip_whitespace();

        let slice = self.chars.as_str();
        let len = slice
            .chars()
            .take_while(|ch| ch.is_numeric() || *ch == '.' || *ch == '-')
            .count();

        self.drop(len);

        // This is supposed to take in byte len (rather than chars), but we're
        // only selecting [0-9.-] here so they'll be equivalent
        slice[..len]
            .parse()
            .map_err(|_| anyhow!("invalid number: {:?}", &slice[..len]))
    }

    fn skip_whitespace(&mut self) {
        while let Some(ch) = self.peek()
            && ch.is_whitespace()
        {
            self.next();
        }
    }

    fn expect(&mut self, s: &str) -> Result<()> {
        if !self.consume(s) {
            anyhow::bail!("expected {:?}, got {:?}", s, self.rest());
        }

        Ok(())
    }

    fn consume(&mut self, s: &str) -> bool {
        self.skip_whitespace();
        if self.rest().starts_with(s) {
            self.drop(s.len());
            return true;
        }

        false
    }

    fn peek(&self) -> Option<char> {
        self.chars.clone().next()
    }

    fn next(&mut self) -> Option<char> {
        self.chars.next().inspect(|_| self.pos += 1)
    }

    fn drop(&mut self, sz: usize) {
        self.pos += sz;
        let _ = self.chars.nth(sz - 1);
    }
}

impl FromStr for PropertyFilter {
    type Err = anyhow::Error;

    fn from_str(s: &str) -> Result<Self> {
        let expr = FilterExpr::parse(s)?;
        Ok(PropertyFilter { expr })
    }
}

impl<'de> Deserialize<'de> for PropertyFilter {
    fn deserialize<D>(deserializer: D) -> Result<PropertyFilter, D::Error>
    where
        D: Deserializer<'de>,
    {
        String::deserialize(deserializer)?
            .parse()
            .map_err(|err| serde::de::Error::custom(format!("invalid filter: {:?}", err)))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_invalid_filters() {
        let filters = [
            "",
            "key =",
            "key = '",
            "1.23 < 4",
            "key < 4.1.2",
            "key < 1-2",
            "key < 123abc",
            "!key = value",
            "key",
            "k in [",
            "k in [1 2]",
            "x=y&&",
        ];

        for &f in filters.iter() {
            FilterExpr::parse(f).expect_err(f);
        }
    }

    #[test]
    fn test_parse_comparisons() {
        let valid_filters = [
            // Comparisons
            "key = value",
            " key  =  value ",
            r#""key" = 'value'"#,
            r#""key" = ''"#,
            "key!=true",
            "key!=false",
            "key < 1",
            "key <= 1.234",
            "key >= value",
            "key > -1.234",
            // key in [value]
            "key in [val1, 'val2', 1.23, false]",
            "key in []",
            // has? key
            "has? my_key_1",
            "has? 'my key 2'",
            // key like "foo"
            r#"name like "foo%""#,
            // && and || chains
            "key = value && key2 = 3.14",
            "a = 1 && b = 2 && c = 3",
            "(a = 1 && (b = 2 || c = 3))",
            r#"((key = value)) || x = 1"#,
            "a = 1 || b = 2 || c = 3",
            // negations
            "!(key = value)",
            "!(a = 1 && b = 2)",
        ];

        // Just check if they parse, assertions about the AST shape are
        // implicitly tested by the SQL generation tests below
        for &filter in valid_filters.iter() {
            FilterExpr::parse(filter).expect(filter);
        }
    }

    macro_rules! assert_sql {
        ($input:expr, $expected_sql:expr, [$($param:expr),* $(,)?]) => {
            let expr = FilterExpr::parse($input)
                .expect(&format!("expected valid filter {:?}", $input));

            let mut sql = String::new();
            let mut params: Vec<&dyn ToSql> = Vec::new();
            expr.to_sql(&mut sql, &mut params);
            assert_eq!(sql, $expected_sql);

            let expected = vec![$(ToSqlOutput::from($param)),*];
            let actual: Vec<_> = params.iter().map(|p| p.to_sql().unwrap()).collect();
            assert_eq!(actual, expected);
        };
    }

    #[test]
    fn test_sql_generation() {
        assert_sql!(
            "avg_speed > 18",
            "(properties ->> ? > ?)",
            ["avg_speed", 18.0]
        );
        assert_sql!(
            "activity = ride",
            "(properties ->> ? = ?)",
            ["activity", "ride"]
        );
        assert_sql!(
            r#"city = "東京""#,
            "(properties ->> ? = ?)",
            ["city", "東京"]
        );
        assert_sql!(
            "名前 = 'こんにちは'",
            "(properties ->> ? = ?)",
            ["名前", "こんにちは"]
        );
        assert_sql!(
            "commute = true",
            "(properties ->> ? = ?)",
            ["commute", true]
        );
        assert_sql!(
            "activity_type in [gravel, 'road']",
            "(properties ->> ? IN (?, ?))",
            ["activity_type", "gravel", "road"]
        );
        assert_sql!(
            "has? heart_rate_data",
            "(properties ->> ? IS NOT NULL)",
            ["heart_rate_data"]
        );
        assert_sql!(
            r#"activity_type like "virtual%""#,
            "(properties ->> ? LIKE ?)",
            ["activity_type", "virtual%"]
        );
        assert_sql!(
            "avg_speed > 18 && distance >= 100",
            "((properties ->> ? > ?) AND (properties ->> ? >= ?))",
            ["avg_speed", 18.0, "distance", 100.0]
        );
        assert_sql!(
            "avg_speed < 13 || avg_speed > 65",
            "((properties ->> ? < ?) OR (properties ->> ? > ?))",
            ["avg_speed", 13.0, "avg_speed", 65.0]
        );
        assert_sql!(
            "!(gear = fixed)",
            "(NOT (properties ->> ? = ?))",
            ["gear", "fixed"]
        );
        assert_sql!(
            "(avg_speed > 18 && gear = fixed) || commute = true",
            "(((properties ->> ? > ?) AND (properties ->> ? = ?)) OR (properties ->> ? = ?))",
            ["avg_speed", 18.0, "gear", "fixed", "commute", true]
        );
        // && binds tighter than ||
        assert_sql!(
            "a = 1 && b = 2 || c = 3",
            "(((properties ->> ? = ?) AND (properties ->> ? = ?)) OR (properties ->> ? = ?))",
            ["a", 1.0, "b", 2.0, "c", 3.0]
        );
        assert_sql!(
            "a = 1 || b = 2 && c = 3",
            "((properties ->> ? = ?) OR ((properties ->> ? = ?) AND (properties ->> ? = ?)))",
            ["a", 1.0, "b", 2.0, "c", 3.0]
        );
    }
}
