// Copyright 2022 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

pub fn split_properties(properties: Vec<String>) -> Vec<(String, String)> {
    properties
        .iter()
        .map(|p| {
            // split_once: a value may itself contain '=' (base64 passwords do).
            let (k, v) = p.split_once('=').expect("properties must be key=value");
            (k.to_string(), v.to_string())
        })
        .collect()
}

/// Reads librdkafka properties from `path`, one `key=value` per line.
///
/// Blank lines and lines starting with `#` are skipped. A file keeps
/// secrets such as `sasl.password` off the command line, where `ps` shows
/// them to every user on the host. Errors name the line, never its content.
pub fn read_properties_file(path: &str) -> Result<Vec<String>, String> {
    let content = std::fs::read_to_string(path)
        .map_err(|e| format!("cannot read properties file {path}: {e}"))?;
    let mut properties = vec![];
    for (i, line) in content.lines().enumerate() {
        let trimmed = line.trim();
        if trimmed.is_empty() || trimmed.starts_with('#') {
            continue;
        }
        if !line.contains('=') {
            return Err(format!("{path}:{}: expected key=value", i + 1));
        }
        properties.push(line.to_string());
    }
    Ok(properties)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn write_temp(name: &str, content: &str) -> String {
        let path = std::env::temp_dir().join(format!("client-swarm-{}-{name}", std::process::id()));
        std::fs::write(&path, content).unwrap();
        path.to_str().unwrap().to_string()
    }

    #[test]
    fn split_keeps_equals_in_value() {
        let props = split_properties(vec!["sasl.password=YWJj==".to_string()]);
        assert_eq!(
            props,
            vec![("sasl.password".to_string(), "YWJj==".to_string())]
        );
    }

    #[test]
    fn file_skips_blank_and_comment_lines() {
        let path = write_temp(
            "ok",
            "# SASL\nsecurity.protocol=SASL_SSL\n\n  \nsasl.password=p=w d\r\n",
        );
        assert_eq!(
            read_properties_file(&path).unwrap(),
            vec!["security.protocol=SASL_SSL", "sasl.password=p=w d"]
        );
    }

    #[test]
    fn file_line_without_equals_names_the_line_only() {
        let path = write_temp("bad", "security.protocol=SASL_SSL\nhunter2\n");
        let err = read_properties_file(&path).unwrap_err();
        assert!(err.ends_with(":2: expected key=value"), "{err}");
        assert!(!err.contains("hunter2"), "{err}");
    }

    #[test]
    fn missing_file_is_an_error() {
        let err = read_properties_file("/nonexistent/client-swarm.properties").unwrap_err();
        assert!(err.starts_with("cannot read properties file"), "{err}");
    }
}
