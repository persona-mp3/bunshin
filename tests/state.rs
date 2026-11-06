use anyhow::{Context, Result};
use bunshin::{Command, Entry, State};
use serde_json;
use std::fs;
use std::io::{BufRead, BufReader};

mod test_utils;
use test_utils::{TEST_NODE_ID, remove_file};

#[test]
fn test_logs_are_persisted() -> Result<()> {
  let mut state = State::new(TEST_NODE_ID);
  let initial_entries: Vec<Entry> = vec![
    Entry::default(),
    Entry {
      index: 1,
      term: 1,
      command: Command::default(),
    },
    Entry {
      index: 2,
      term: 2,
      command: Command::default(),
    },
  ];

  remove_file(&state.file_name)?;

  state.entries = initial_entries;

  let stop_index = 2;
  state.persist_till(stop_index).unwrap();

  // parse the contents of the file
  let f = fs::File::options()
    .read(true)
    .open(state.file_name.clone())
    .with_context(|| format!("failed to open node's persistent file"))
    .unwrap();

  let reader = BufReader::new(f);

  let mut entries: Vec<Entry> = Vec::new();

  for line in reader.lines() {
    let line = line.unwrap(); // reading the line can fail
    let entry: Entry = serde_json::from_str(&line).unwrap();
    entries.push(entry);
  }

  assert_eq!(state.entries, entries);
  remove_file(&state.file_name)?;
  Ok(())
}
