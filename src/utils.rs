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
        .map(|p| p.split("=").collect::<Vec<&str>>())
        .map(|parts| (parts[0].to_string(), parts[1].to_string()))
        .collect()
}
