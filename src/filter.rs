#![allow(unused)]
use std::{borrow::Cow, str::{Chars, FromStr}};

use anyhow::{Result, anyhow};
use ouroboros::self_referencing;
use rusqlite::ToSql;
use serde::{Deserialize, Deserializer};

#[derive(Debug)]
enum Value<'a> {
    String(&'a str),
    Number(f64),
    True,
    False,
}

impl<'a> Value<'a> {
    fn as_sql_param(&'a self) -> &'a dyn ToSql {
        match self {
            Value::String(s) => s,
            Value::Number(n) => n,
            Value::True => &true,
            Value::False => &false,
        }
    }
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

#[derive(Debug)]
enum FilterExpr<'a> {
    Comparison(&'a str, ComparisonOp, Value<'a>),
    OneOf(&'a str, Vec<Value<'a>>),
    HasKey(&'a str),
    Like(&'a str, &'a str),
    And(Box<FilterExpr<'a>>, Box<FilterExpr<'a>>),
    Or(Box<FilterExpr<'a>>, Box<FilterExpr<'a>>),
    Not(Box<FilterExpr<'a>>),
}

impl<'a> FilterExpr<'a> {
    pub fn to_sql(&'a self) -> (String, Vec<&'a dyn ToSql>) {
        let mut str = String::with_capacity(256);
        let mut params = Vec::new();

        self.to_sql_inner(&mut str, &mut params);
        (str, params)
    }
    fn to_sql_inner<'b: 'a>(&'a self, str: &mut String, params: &mut Vec<&'a dyn ToSql>) {
        str.push('(');
        match self {
            FilterExpr::And(lhs, rhs) => {
                lhs.to_sql_inner(str, params);
                str.push_str(" AND ");
                rhs.to_sql_inner(str, params);
            }
            FilterExpr::Or(lhs, rhs) => {
                lhs.to_sql_inner(str, params);
                str.push_str(" OR ");
                rhs.to_sql_inner(str, params);
            }
            FilterExpr::Not(expr) => {
                str.push_str("NOT ");
                expr.to_sql_inner(str, params);
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
                params.push(value.as_sql_param());
            }
            FilterExpr::OneOf(key, values) => {
                str.push_str("properties ->> ? IN (");
                params.push(key);

                for (i, value) in values.iter().enumerate() {
                    if i > 0 {
                        str.push_str(", ");
                    }
                    str.push('?');
                    params.push(value.as_sql_param());
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

impl<'a> TryFrom<&'a str> for FilterExpr<'a> {
    type Error = anyhow::Error;

    fn try_from(input: &'a str) -> Result<Self, Self::Error> {
        let mut parser = FilterParser {
            chars: input.chars(),
            pos: 0,
        };

        let expr = parser
            .read_clause()
            .map_err(|err| anyhow!("parse error at position {}: {:?}", parser.pos, err))?;

        if parser.peek().is_some() {
            anyhow::bail!("unexpected: {:?}", parser.chars.as_str());
        }

        Ok(expr)
    }
}

#[derive(Debug)]
struct FilterParser<'a> {
    chars: Chars<'a>,
    pos: usize,
}

impl<'a> FilterParser<'a> {
    fn read_clause(&mut self) -> Result<FilterExpr<'a>> {
        let mut lhs = self.read_unary()?;

        loop {
            if self.consume("||") {
                let rhs = self.read_clause()?;
                lhs = FilterExpr::Or(Box::new(lhs), Box::new(rhs));
            } else if self.consume("&&") {
                let rhs = self.read_clause()?;
                lhs = FilterExpr::And(Box::new(lhs), Box::new(rhs));
            } else {
                break;
            }
        }
        Ok(lhs)
    }

    fn read_unary(&mut self) -> Result<FilterExpr<'a>> {
        self.skip_whitespace();

        if self.consume("!") {
            self.expect("(")?;
            let expr = self.read_clause()?;
            self.expect(")")?;
            Ok(FilterExpr::Not(Box::new(expr)))
        } else if self.consume("has?") {
            let key = self.read_key()?;
            Ok(FilterExpr::HasKey(key))
        } else {
            self.read_expr()
        }
    }

    fn read_expr(&mut self) -> Result<FilterExpr<'a>> {
        self.skip_whitespace();

        if self.consume("(") {
            let expr = self.read_clause()?;
            self.expect(")")?;
            return Ok(expr);
        }

        let key = self.read_key()?;

        let expr = if self.consume("in") {
            let list = self.read_list()?;
            FilterExpr::OneOf(key, list)
        } else if self.consume("like") {
            let str = self.read_string()?;
            FilterExpr::Like(key, str)
        } else {
            let op = self.read_binary_op()?;
            let rhs = self.read_value()?;
            FilterExpr::Comparison(key, op, rhs)
        };

        Ok(expr)
    }

    fn read_key(&mut self) -> Result<&'a str> {
        self.skip_whitespace();
        match self.read_value()? {
            Value::String(s) => Ok(s),
            x => anyhow::bail!("expected key, got {:?}", x),
        }
    }

    fn read_list(&mut self) -> Result<Vec<Value<'a>>> {
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

    fn read_value(&mut self) -> Result<Value<'a>> {
        self.skip_whitespace();

        Ok(match self.peek() {
            Some(ch) if ch == '"' || ch == '\'' => Value::String(self.read_string()?),
            Some(ch) if ch.is_numeric() || ch == '-' => Value::Number(self.read_number()?),
            Some(_) => match self.read_word()? {
                "true" => Value::True,
                "false" => Value::False,
                str => Value::String(str),
            },
            None => anyhow::bail!("expected value, hit eof"),
        })
    }

    fn read_binary_op(&mut self) -> Result<ComparisonOp> {
        self.skip_whitespace();

        match (self.next(), self.peek()) {
            (Some('<'), Some('=')) => {
                self.skip(1);
                Ok(ComparisonOp::Lte)
            }
            (Some('>'), Some('=')) => {
                self.skip(1);
                Ok(ComparisonOp::Gte)
            }
            (Some('!'), Some('=')) => {
                self.skip(1);
                Ok(ComparisonOp::Neq)
            }
            (Some('='), _) => Ok(ComparisonOp::Eq),
            (Some('<'), _) => Ok(ComparisonOp::Lt),
            (Some('>'), _) => Ok(ComparisonOp::Gt),
            (_, _) => anyhow::bail!("unknown operator"),
        }
    }

    fn read_word(&mut self) -> Result<&'a str> {
        self.skip_whitespace();
        let slice = self.chars.as_str();
        let len = slice
            .chars()
            .take_while(|ch| ch.is_alphanumeric() || *ch == '_')
            .count();

        if len == 0 {
            anyhow::bail!("empty identifier");
        }

        self.skip(len);
        Ok(&slice[..len])
    }

    fn read_string(&mut self) -> Result<&'a str> {
        self.skip_whitespace();
        let delim = self.next().ok_or_else(|| anyhow!("expected str"))?;
        let slice = self.chars.as_str();

        let len = slice.chars().take_while(|ch| *ch != delim).count();
        if len > 0 {
            self.skip(len);
        }

        if self.next().is_none() {
            anyhow::bail!("unexpected eof");
        }

        Ok(&slice[..len])
    }

    fn read_number(&mut self) -> Result<f64> {
        self.skip_whitespace();

        let slice = self.chars.as_str();
        let len = slice
            .chars()
            .take_while(|ch| ch.is_numeric() || *ch == '.' || *ch == '-')
            .count();
        self.skip(len);

        slice[..len]
            .parse()
            .map_err(|e| anyhow!("invalid num: {:?}", e))
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
            let str = self.chars.as_str();
            anyhow::bail!("expected {:?}, got {:?}", s, str);
        }

        Ok(())
    }

    fn consume(&mut self, s: &str) -> bool {
        self.skip_whitespace();
        if self.chars.as_str().starts_with(s) {
            self.skip(s.len());
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

    fn skip(&mut self, sz: usize) {
        self.pos += sz;
        let _ = self.chars.nth(sz - 1);
    }
}

#[self_referencing]
#[derive(Debug)]
pub struct PropertyFilter {
    source: String,
    #[borrows(source)]
    #[covariant]
    expr: FilterExpr<'this>,
}

impl PropertyFilter {
    fn from_string(s: String) -> Result<Self> {
        PropertyFilterTryBuilder {
            source: s,
            expr_builder: |source: &String| {
                FilterExpr::try_from(source.as_str())
            },
        }.try_build()
    }

    pub fn to_query<'a>(&'a self, clauses: &mut Vec<Cow<'a, str>>, params: &mut Vec<&'a dyn ToSql>) {
        self.with_expr(|expr| {
            let (sql, filter_params) = expr.to_sql();
            clauses.push(sql.into());
            params.extend(filter_params);
        });
    }
}

impl FromStr for PropertyFilter {
    type Err = anyhow::Error;

    fn from_str(s: &str) -> Result<Self> {
        PropertyFilter::from_string(s.to_string())
    }
}

impl<'de> Deserialize<'de> for PropertyFilter {
    fn deserialize<D>(deserializer: D) -> Result<PropertyFilter, D::Error>
    where
        D: Deserializer<'de>,
    {
        let s = String::deserialize(deserializer)?;
        PropertyFilter::from_string(s).map_err(|err| {
            serde::de::Error::custom(format!("invalid filter expression: {}", err))
        })
    }
}

impl Clone for PropertyFilter {
    fn clone(&self) -> Self {
        let source = self.borrow_source().clone();
        PropertyFilter::from_string(source).expect("cloning valid PropertyFilter should succeed")
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
            FilterExpr::try_from(f).expect_err(f);
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
            FilterExpr::try_from(filter).expect(filter);
        }
    }

    macro_rules! assert_params {
        ($params:expr, [$($val:expr),* $(,)?]) => {
            let expected: Vec<rusqlite::types::ToSqlOutput> = vec![
                $(rusqlite::types::ToSqlOutput::from($val)),*
            ];
            let actual: Vec<_> = $params.iter()
                .map(|p| p.to_sql().unwrap())
                .collect();
            assert_eq!(actual, expected);
        };
    }

    macro_rules! assert_sql {
        ($input:expr, $expected_sql:expr, [$($param:expr),* $(,)?]) => {
            let expr = FilterExpr::try_from($input).unwrap();
            let (sql, params) = expr.to_sql();
            assert_eq!(sql, $expected_sql);
            assert_params!(params, [$($param),*]);
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
    }
}
