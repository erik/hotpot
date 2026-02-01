use assert_cmd::assert::Assert;
use assert_cmd::cargo_bin;
use assert_cmd::prelude::*;
use std::fs;
use std::path::Path;
use std::process::Command;
use tempfile::tempdir;

fn build_subcommand(db_path: &Path, subcommand: &str, args: &[&str]) -> Assert {
    Command::new(cargo_bin!())
        .arg("--db")
        .arg(db_path)
        .arg(subcommand)
        .args(args)
        .assert()
}

fn get_activity_count(db_path: &Path) -> usize {
    let assert = build_subcommand(db_path, "activities", &["--count"]);
    let result = assert.success();
    let output = result.get_output();
    let count = String::from_utf8_lossy(&output.stdout);
    count.trim().parse().expect("valid count")
}

const TEST_DATA_DIR: &str = "tests/fixtures/";

#[test]
fn test_import_and_activities_count() {
    let temp_dir = tempdir().unwrap();
    let db_path = temp_dir.path().join("test.sqlite3");

    build_subcommand(
        &db_path,
        "import",
        &[
            TEST_DATA_DIR,
            "--join",
            &format!("{}/metadata.csv", TEST_DATA_DIR),
        ],
    )
    .success();

    assert_eq!(get_activity_count(&db_path), 3);

    let filtered_assert = build_subcommand(
        &db_path,
        "activities",
        &[
            "--filter",
            r#"{"activity_type": {"any_of": ["ride", "run"]}}"#,
            "--before=2025-09-01",
        ],
    );
    let filtered_result = filtered_assert.success();
    let filtered_output = filtered_result.get_output();

    let filtered_output_str = String::from_utf8_lossy(&filtered_output.stdout);
    let lines: Vec<&str> = filtered_output_str.lines().collect();
    assert_eq!(lines.len(), 1, "Should match only one activity");
    assert!(lines[0].contains("sample.gpx"));
}

#[test]
fn test_import_fit_file() {
    let temp_dir = tempdir().unwrap();
    let db_path = temp_dir.path().join("test.sqlite3");

    build_subcommand(
        &db_path,
        "import",
        &[&format!("{}activities/sample.fit", TEST_DATA_DIR)],
    )
    .success();

    assert_eq!(get_activity_count(&db_path), 1);

    let filtered_assert = build_subcommand(&db_path, "activities", &[]);
    let filtered_result = filtered_assert.success();
    let filtered_output = filtered_result.get_output();

    let filtered_output_str = String::from_utf8_lossy(&filtered_output.stdout);
    let lines: Vec<&str> = filtered_output_str.lines().collect();

    // Not a great test, but check that we've pulled something out of both the
    // FileId/Session blocks
    assert!(lines[0].contains(r#""manufacturer":"garmin","#));
    assert!(lines[0].contains(r#""total_calories":1188,"#));
}

#[test]
fn test_import_deduplication() {
    let temp_dir = tempdir().unwrap();
    let db_path = temp_dir.path().join("test.sqlite3");

    // Import the activities twice
    build_subcommand(&db_path, "import", &[TEST_DATA_DIR]).success();
    build_subcommand(&db_path, "import", &[TEST_DATA_DIR]).success();

    assert_eq!(
        get_activity_count(&db_path),
        3,
        "Should deduplicate identical imports"
    );
}

#[test]
fn test_tile_rendering_with_empty_db() {
    let temp_dir = tempdir().unwrap();
    let db_path = temp_dir.path().join("test.sqlite3");
    let output_file = temp_dir.path().join("tile.png");

    // Ensure the DB exists, but doesn't actually contain any activities.
    build_subcommand(
        &db_path,
        "import",
        &[&format!("{}activities/no_track_data.tcx", TEST_DATA_DIR)],
    )
    .success();

    build_subcommand(
        &db_path,
        "tile",
        &[
            "0/0/0",
            "--width=256",
            "--output",
            output_file.to_str().unwrap(),
        ],
    )
    .success();

    assert!(
        output_file.exists(),
        "Tile should be rendered even with empty DB"
    );
}

#[test]
fn test_import_and_tile_rendering() {
    let temp_dir = tempdir().unwrap();
    let db_path = temp_dir.path().join("test.sqlite3");
    let output_file = temp_dir.path().join("tile.png");

    build_subcommand(&db_path, "import", &[TEST_DATA_DIR]).success();

    build_subcommand(
        &db_path,
        "tile",
        &[
            "0/0/0",
            "--width=256",
            "--output",
            output_file.to_str().unwrap(),
        ],
    )
    .success();

    assert!(output_file.exists(), "Tile should be rendered");
    let metadata = fs::metadata(&output_file).unwrap();
    assert!(metadata.len() > 0, "Tile should have content");
}

#[test]
fn test_filter_by_date_after() {
    let temp_dir = tempdir().unwrap();
    let db_path = temp_dir.path().join("test.sqlite3");

    build_subcommand(&db_path, "import", &[TEST_DATA_DIR]).success();

    let assert = build_subcommand(&db_path, "activities", &["--before", "2023-07-01"]);
    let result = assert.success();
    let output = result.get_output();

    let activities = String::from_utf8_lossy(&output.stdout);
    assert_eq!(activities.lines().count(), 1);
    assert!(activities.contains("sample.fit"));
}

#[test]
fn test_virtual_activities_skipped() {
    let temp_dir = tempdir().unwrap();
    let db_path = temp_dir.path().join("test.sqlite3");

    // Import the virtual ride GPX/FIT files
    build_subcommand(
        &db_path,
        "import",
        &[&format!("{}/activities/virtual/", TEST_DATA_DIR)],
    )
    .success();

    // The virtual activity should be skipped, so count should be 0
    assert_eq!(
        get_activity_count(&db_path),
        0,
        "Virtual GPX activities should be skipped"
    );
}

#[test]
fn test_add_mask() {
    let temp_dir = tempdir().unwrap();
    let db_path = temp_dir.path().join("test.sqlite3");

    let assert = build_subcommand(
        &db_path,
        "mask",
        &[
            "add",
            "home",
            "--latlng",
            "52.5200,13.4050",
            "--radius",
            "500",
        ],
    );
    let result = assert.success();
    let output = result.get_output();

    let stdout = String::from_utf8_lossy(&output.stdout);
    assert!(stdout.contains("Added masked area 'home'"));
    assert!(stdout.contains("13.405"));
    assert!(stdout.contains("52.520"));
    assert!(stdout.contains("500m"));
}

#[test]
fn test_remove_mask() {
    let temp_dir = tempdir().unwrap();
    let db_path = temp_dir.path().join("test.sqlite3");

    build_subcommand(&db_path, "mask", &["add", "home", "--latlng", "10,10"]).success();
    build_subcommand(&db_path, "mask", &["add", "work", "--latlng", "0,0"]).success();

    let assert = build_subcommand(&db_path, "mask", &["remove", "home"]);
    let result = assert.success();
    let output = result.get_output();

    let stdout = String::from_utf8_lossy(&output.stdout);
    assert!(stdout.contains("Removed masked area 'home'"));

    let list_assert = build_subcommand(&db_path, "mask", &[]);
    let list_result = list_assert.success();
    let list_output = list_result.get_output();
    let list_stdout = String::from_utf8_lossy(&list_output.stdout);

    assert!(list_stdout.contains("work"));
    assert!(!list_stdout.contains("home"));
}

#[test]
fn test_mask_remove_nonexistent() {
    let temp_dir = tempdir().unwrap();
    let db_path = temp_dir.path().join("test.sqlite3");

    let assert = build_subcommand(&db_path, "mask", &["remove", "nonexistent"]);
    assert.failure();
}

#[test]
fn test_mask_invalid_coordinates() {
    let temp_dir = tempdir().unwrap();
    let db_path = temp_dir.path().join("test.sqlite3");

    build_subcommand(&db_path, "mask", &["add", "test", "--latlng", "91,13.4050"]).failure();
    build_subcommand(&db_path, "mask", &["add", "test", "--latlng", "52,181"]).failure();
    build_subcommand(&db_path, "mask", &["add", "test", "--latlng", "13 52"]).failure();
}
