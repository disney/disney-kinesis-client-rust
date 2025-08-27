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

use aws_sdk_cloudwatch::operation::put_metric_data::PutMetricDataError;
use aws_sdk_dynamodb::operation::{
    create_table::CreateTableError, delete_item::DeleteItemError,
    describe_table::DescribeTableError, get_item::GetItemError, put_item::PutItemError,
    scan::ScanError, update_item::UpdateItemError,
};
use aws_sdk_kinesis::operation::{
    describe_stream_summary::DescribeStreamSummaryError, get_records::GetRecordsError,
    get_shard_iterator::GetShardIteratorError, list_shards::ListShardsError,
};
use aws_smithy_runtime_api::client::orchestrator::HttpResponse;
use aws_smithy_runtime_api::client::result::SdkError;
use derive_more::{Display, Error};

use std::fmt::Debug;

use crate::waiter::DynamoDbWaitError;

/// The error type returned by all user-facing code, splitting the errors into
/// those that are the responsibility of the consumer library and those that
/// originated in the AWS SDK.
// https://users.rust-lang.org/t/how-to-wrap-errors-from-a-third-party-library/121803
#[derive(Debug, Display, Error)]
pub enum Fail {
    Kcl(#[error(not(source))] String),
    Cloudwatch {
        source: SdkError<aws_sdk_cloudwatch::Error, HttpResponse>,
    },
    DynamoDB {
        source: SdkError<aws_sdk_dynamodb::Error, HttpResponse>,
    },
    Kinesis {
        source: SdkError<aws_sdk_kinesis::Error, HttpResponse>,
    },
    DynamoDBWaiter {
        source: DynamoDbWaitError,
    },
}

fn cw_error<E: Into<aws_sdk_cloudwatch::Error>>(e: SdkError<E, HttpResponse>) -> Fail {
    Fail::Cloudwatch {
        source: e.map_service_error(|s| s.into()),
    }
}

fn ddb_error<E: Into<aws_sdk_dynamodb::Error>>(e: SdkError<E, HttpResponse>) -> Fail {
    Fail::DynamoDB {
        source: e.map_service_error(|s| s.into()),
    }
}

fn kinesis_error<E: Into<aws_sdk_kinesis::Error>>(e: SdkError<E, HttpResponse>) -> Fail {
    Fail::Kinesis {
        source: e.map_service_error(|s| s.into()),
    }
}

impl From<SdkError<PutMetricDataError, HttpResponse>> for Fail {
    fn from(e: SdkError<PutMetricDataError, HttpResponse>) -> Fail {
        cw_error(e)
    }
}

impl From<SdkError<DescribeStreamSummaryError, HttpResponse>> for Fail {
    fn from(e: SdkError<DescribeStreamSummaryError, HttpResponse>) -> Fail {
        kinesis_error(e)
    }
}

impl From<SdkError<GetShardIteratorError, HttpResponse>> for Fail {
    fn from(e: SdkError<GetShardIteratorError, HttpResponse>) -> Fail {
        kinesis_error(e)
    }
}

impl From<SdkError<ListShardsError, HttpResponse>> for Fail {
    fn from(e: SdkError<ListShardsError, HttpResponse>) -> Fail {
        kinesis_error(e)
    }
}

impl From<SdkError<GetRecordsError, HttpResponse>> for Fail {
    fn from(e: SdkError<GetRecordsError, HttpResponse>) -> Fail {
        kinesis_error(e)
    }
}

impl From<SdkError<CreateTableError, HttpResponse>> for Fail {
    fn from(e: SdkError<CreateTableError, HttpResponse>) -> Fail {
        ddb_error(e)
    }
}

impl From<SdkError<DescribeTableError, HttpResponse>> for Fail {
    fn from(e: SdkError<DescribeTableError, HttpResponse>) -> Fail {
        ddb_error(e)
    }
}

impl From<SdkError<ScanError, HttpResponse>> for Fail {
    fn from(e: SdkError<ScanError, HttpResponse>) -> Fail {
        ddb_error(e)
    }
}

impl From<SdkError<GetItemError, HttpResponse>> for Fail {
    fn from(e: SdkError<GetItemError, HttpResponse>) -> Fail {
        ddb_error(e)
    }
}

impl From<SdkError<PutItemError, HttpResponse>> for Fail {
    fn from(e: SdkError<PutItemError, HttpResponse>) -> Fail {
        ddb_error(e)
    }
}

impl From<SdkError<DeleteItemError, HttpResponse>> for Fail {
    fn from(e: SdkError<DeleteItemError, HttpResponse>) -> Fail {
        ddb_error(e)
    }
}

impl From<SdkError<UpdateItemError, HttpResponse>> for Fail {
    fn from(e: SdkError<UpdateItemError, HttpResponse>) -> Fail {
        ddb_error(e)
    }
}

impl From<DynamoDbWaitError> for Fail {
    fn from(e: DynamoDbWaitError) -> Fail {
        Fail::DynamoDBWaiter { source: e }
    }
}
