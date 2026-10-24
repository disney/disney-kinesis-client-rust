// Copyright 2024-2025 Sam Halliday, Rob Pickerill (The Walt Disney Company)
//
// Licensed under the Tomorrow Open Source Technology License, Version 1.0 (the
// "License"); you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//  https://disneystreaming.github.io/TOST-1.0.txt
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
// WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
// License for the specific language governing permissions and limitations under
// the License.

// support for the KPL's aggregated record format.
// https://docs.rs/protobuf/latest/protobuf/

use prost::Message;

// As defined at
//
// https://github.com/awslabs/amazon-kinesis-producer/blob/master/aggregation-format.md
//
// which is:
//
// 4 magic bytes: 0xF3 0x89 0x9A 0xC2
// a valid AggregatedRecord
// 16 byte checksum of the protobuf bytes
//
// The protobuf part of the message was originally created with prost-build but
// it was so fiddly to get it working, and the definitions so simple, we just
// inline it.
#[derive(Clone, PartialEq, Message)]
pub struct AggregatedRecord {
    #[prost(string, repeated, tag = "1")]
    pub partition_key_table: Vec<String>,
    #[prost(string, repeated, tag = "2")]
    pub explicit_hash_key_table: Vec<String>,
    #[prost(message, repeated, tag = "3")]
    pub records: Vec<Record>,
}
#[derive(Clone, PartialEq, Message)]
pub struct Record {
    #[prost(uint64, required, tag = "1")]
    pub partition_key_index: u64,
    #[prost(uint64, optional, tag = "2")]
    pub explicit_hash_key_index: Option<u64>,
    #[prost(bytes = "vec", required, tag = "3")]
    pub data: Vec<u8>,
    // removed all the "tags" stuff from index 4 because it's unused
}

pub fn deaggregate(data: &[u8]) -> Option<AggregatedRecord> {
    if data.len() <= 20
        || data[0] != MAGIC[0]
        || data[1] != MAGIC[1]
        || data[2] != MAGIC[2]
        || data[3] != MAGIC[3]
    {
        return None;
    }

    // prost continues reading past the object encoded section, so we have to
    // manually slice out the data we want to read.
    let end = data.len() - 16;
    let result = AggregatedRecord::decode(&data[4..end]).ok()?;

    // bounds checking
    if result.records.is_empty() {
        return None;
    }
    for record in &result.records {
        if record.partition_key_index as usize >= result.partition_key_table.len() {
            return None;
        }
        if let Some(v) = record.explicit_hash_key_index {
            if v as usize >= result.explicit_hash_key_table.len() {
                return None;
            }
        }
    }

    // we could validate the checksum, but it is superfluous
    return Some(result);
}

const MAGIC: [u8; 4] = [0xF3, 0x89, 0x9A, 0xC2];

#[allow(dead_code)]
pub fn aggregate(aggregated: &AggregatedRecord) -> Vec<u8> {
    let mut buf = aggregated.encode_to_vec();
    let digest = md5::compute(&buf);

    let mut msg = Vec::new();
    for b in MAGIC {
        msg.push(b);
    }
    msg.append(&mut buf);
    for b in digest.0 {
        msg.push(b);
    }

    return msg;
}

#[cfg(test)]
mod tests {
    use crate::aggregated::*;

    #[test]
    fn test_round_trip() {
        let record = AggregatedRecord {
            partition_key_table: vec!["foo".to_string(), "bar".to_string(), "baz".to_string()],
            explicit_hash_key_table: vec!["a".to_string(), "b".to_string()],
            records: vec![
                Record {
                    partition_key_index: 0,
                    explicit_hash_key_index: Some(0),
                    data: From::from("wibble".to_string()),
                },
                Record {
                    partition_key_index: 1,
                    explicit_hash_key_index: Some(1),
                    data: From::from("wobble".to_string()),
                },
                Record {
                    partition_key_index: 2,
                    explicit_hash_key_index: Some(1),
                    data: From::from("fish".to_string()),
                },
            ],
        };

        let encoded = aggregate(&record);
        let decoded = deaggregate(&encoded);

        assert_eq!(Some(record), decoded);
    }
}
