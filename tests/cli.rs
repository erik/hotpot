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
            "activity_type in [ride, run]",
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
        &["add", "home", "--latlng=52.5200,13.4050", "--radius=500"],
    );
    let result = assert.success();
    let output = result.get_output();

    let stdout = String::from_utf8_lossy(&output.stdout);
    assert!(stdout.contains("home - 52.52000,13.40500 (radius: 500m)"));
}

#[test]
fn test_remove_mask() {
    let temp_dir = tempdir().unwrap();
    let db_path = temp_dir.path().join("test.sqlite3");

    build_subcommand(&db_path, "mask", &["add", "home", "--latlng=10,10"]).success();
    build_subcommand(&db_path, "mask", &["add", "work", "--latlng=0,0"]).success();

    build_subcommand(&db_path, "mask", &["remove", "home"]).success();

    let list_assert = build_subcommand(&db_path, "mask", &["list"]);
    let list_result = list_assert.success();
    let list_output = list_result.get_output();
    let list_stdout = String::from_utf8_lossy(&list_output.stdout);

    assert!(list_stdout.contains("work"));
    assert!(!list_stdout.contains("home"));
}

#[test]
fn test_mask_duplicate_name_updates() {
    let temp_dir = tempdir().unwrap();
    let db_path = temp_dir.path().join("test.sqlite3");

    build_subcommand(
        &db_path,
        "mask",
        &["add", "home", "--latlng=52.5200,13.4050"],
    )
    .success();

    // Add same name with different coordinates - should replace
    build_subcommand(
        &db_path,
        "mask",
        &["add", "home", "--latlng=51.5074,0.1278", "--radius=1000"],
    )
    .success();

    // List masks - should only have one
    let list_result = build_subcommand(&db_path, "mask", &["list"]).success();
    let list_output = String::from_utf8_lossy(&list_result.get_output().stdout);
    assert_eq!(list_output.matches("home").count(), 1);
    assert!(list_output.contains("home - 51.50740,0.12780 (radius: 1000m)"));
}

#[test]
fn test_filter_not_equal() {
    let temp_dir = tempdir().unwrap();
    let db_path = temp_dir.path().join("test.sqlite3");

    // Import without metadata join - activities won't have activity_type
    build_subcommand(&db_path, "import", &[TEST_DATA_DIR]).success();

    let assert = build_subcommand(
        &db_path,
        "activities",
        &["--filter", "manufacturer != garmin"],
    );
    let result = assert.success();
    let output = result.get_output();

    let activities = String::from_utf8_lossy(&output.stdout);
    let lines: Vec<&str> = activities.lines().collect();

    // Should return gpx and tcx (no manufacturer property), but not fit (manufacturer=garmin)
    assert_eq!(
        lines.len(),
        2,
        "Should match activities without the property"
    );
    assert!(
        activities.contains("sample.gpx"),
        "Should contain gpx (no manufacturer)"
    );
    assert!(
        activities.contains("sample.tcx"),
        "Should contain tcx (no manufacturer)"
    );
    assert!(
        !activities.contains("sample.fit"),
        "Should not contain fit (manufacturer=garmin)"
    );
}
