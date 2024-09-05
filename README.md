# rust-timestream-utils
Crate to simplify interacting with the AWS Timestream SDK.

There are two main usages of this crate:
1. `TimestreamTable`, which represents a real table, and you can use to write data, or query and deserialize into custom types.
2. Behind the `test-utils` feature flag, a `DroppableTable` which you can use to write tests that interact with a real DB table, which is created and destroyed within the lifecycle of the test. Note that the DB must already exist though.