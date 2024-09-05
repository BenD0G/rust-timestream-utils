use crate::table::TimestreamClients;
use crate::TimestreamTable;
use anyhow::Result;
use aws_sdk_timestreamwrite::types::{MagneticStoreWriteProperties, RetentionProperties, Tag};
use aws_sdk_timestreamwrite::Client as WriteClient;
use tokio::runtime::Handle;

/// Wrapper around a Timestream table that deletes the table when it is dropped.
/// Intended to be used in tests.
pub struct DroppableTable<'a> {
    pub table: TimestreamTable<'a>,
    table_name: String,
    db_name: String,
    write_client: &'a WriteClient,
}

impl<'a> Drop for DroppableTable<'a> {
    fn drop(&mut self) {
        let handle = Handle::current();
        tokio::task::block_in_place(|| {
            handle.block_on(destroy_table(
                &self.table_name,
                &self.db_name,
                self.write_client,
            ))
        })
        .unwrap();
    }
}

/// Creates a new DroppableTable table with a random name, which is deleted once this object is dropped.
/// Note that the table has 20 years of magnetic store retention.
/// The DB must already exist before calling this function.
pub async fn make_droppable_table<'a>(
    clients: &'a TimestreamClients,
    db_name: &str,
) -> Result<DroppableTable<'a>> {
    let table_name = format!("c2f-test-{}", uuid::Uuid::new_v4());

    let project_tag = Tag::builder().key("project").value("c2f").build()?;
    let junk_tag = Tag::builder().key("is_junk").value("true").build()?;

    let retention_properties = RetentionProperties::builder()
        .memory_store_retention_period_in_hours(1)
        .magnetic_store_retention_period_in_days(20 * 365)
        .build()?;

    let magnetic_store_properties = MagneticStoreWriteProperties::builder()
        .enable_magnetic_store_writes(true)
        .build()?;

    clients
        .write_client
        .create_table()
        .database_name(db_name)
        .table_name(&table_name)
        .retention_properties(retention_properties)
        .magnetic_store_write_properties(magnetic_store_properties)
        .set_tags(Some(vec![project_tag, junk_tag]))
        .send()
        .await?;

    let table = TimestreamTable::new(&table_name, db_name, clients);

    Ok(DroppableTable {
        table,
        table_name,
        db_name: db_name.to_string(),
        write_client: &clients.write_client,
    })
}

async fn destroy_table<'a>(
    table_name: &str,
    db_name: &str,
    write_client: &'a WriteClient,
) -> Result<()> {
    write_client
        .delete_table()
        .database_name(db_name)
        .table_name(table_name)
        .send()
        .await
        // I *think* we need to panic here, as this is used in a destructor
        .unwrap();

    Ok(())
}

#[cfg(test)]
mod tests {
    use crate::{load_timestream_clients, utc_datetime_from_timestamp};
    use aws_sdk_timestreamwrite::types::{MeasureValue, MeasureValueType, TimeUnit};
    use chrono::{DateTime, Utc};
    use serde::Deserialize;

    use super::*;

    const DB_NAME: &str = "c2f-test";

    fn make_timestream_records() -> Result<Vec<aws_sdk_timestreamwrite::types::Record>> {
        let measure_1 = MeasureValue::builder()
            .r#type(MeasureValueType::Double)
            .name("measure_1")
            .value("1.0")
            .build()?;
        let measure_2 = MeasureValue::builder()
            .r#type(MeasureValueType::Varchar)
            .name("measure_2")
            .value("poop")
            .build()?;

        let record = aws_sdk_timestreamwrite::types::Record::builder()
            .dimensions(
                aws_sdk_timestreamwrite::types::Dimension::builder()
                    .name("test")
                    .value("test")
                    .build()
                    .unwrap(),
            )
            .measure_name("test_measures")
            .measure_value_type(MeasureValueType::Multi)
            .measure_values(measure_1)
            .measure_values(measure_2)
            .build();
        Ok(vec![record])
    }

    async fn row_count(table: &TimestreamTable<'_>) -> u64 {
        #[derive(Deserialize)]
        struct Count {
            #[serde(rename = "_col0")]
            count: u64,
        }
        let results = table
            .query_data::<Count>(&format!(
                r#"SELECT COUNT(*) FROM {}"#,
                table.full_table_name()
            ))
            .await
            .unwrap();

        results[0].count
    }

    #[ignore]
    #[tokio::test(flavor = "multi_thread")]
    async fn test_row_count() {
        let clients = load_timestream_clients().await.unwrap();
        let table = make_droppable_table(&clients, DB_NAME).await.unwrap();
        assert_eq!(row_count(&table.table).await, 0);

        let records = make_timestream_records().unwrap();

        let common_attributes = aws_sdk_timestreamwrite::types::Record::builder()
            .dimensions(
                aws_sdk_timestreamwrite::types::Dimension::builder()
                    .name("common_dimension")
                    .value("test_dimension")
                    .build()
                    .unwrap(),
            )
            .time(
                chrono::Utc::now()
                    .timestamp_nanos_opt()
                    .unwrap()
                    .to_string(),
            )
            .time_unit(TimeUnit::Nanoseconds)
            .build();

        table
            .table
            .write_records(&records, Some(&common_attributes))
            .await
            .unwrap();

        assert_eq!(row_count(&table.table).await, 1);
    }

    #[ignore]
    #[tokio::test(flavor = "multi_thread")]
    async fn test_query_data() {
        let clients = load_timestream_clients().await.unwrap();
        let table = make_droppable_table(&clients, DB_NAME).await.unwrap();

        let query_string = format!("SELECT * FROM {}", table.table.full_table_name());

        // Dummy type
        let data: Vec<u64> = table.table.query_data(&query_string).await.unwrap();

        assert!(data.is_empty(), "{:#?}", data);

        let records = make_timestream_records().unwrap();

        let now = chrono::Utc::now();

        let common_attributes = aws_sdk_timestreamwrite::types::Record::builder()
            .dimensions(
                aws_sdk_timestreamwrite::types::Dimension::builder()
                    .name("common_dimension")
                    .value("test_dimension")
                    .build()
                    .unwrap(),
            )
            .time(now.timestamp_nanos_opt().unwrap().to_string())
            .time_unit(TimeUnit::Nanoseconds)
            .build();

        table
            .table
            .write_records(&records, Some(&common_attributes))
            .await
            .unwrap();

        #[derive(Deserialize, PartialEq, Debug)]
        struct DummyData {
            test: String,
            common_dimension: String,
            measure_name: String,
            #[serde(deserialize_with = "utc_datetime_from_timestamp")]
            time: DateTime<Utc>,
            measure_2: String,
            measure_1: f64,
        }

        let data: Vec<DummyData> = table.table.query_data(&query_string).await.unwrap();

        assert_eq!(
            data,
            vec![DummyData {
                test: "test".to_string(),
                common_dimension: "test_dimension".to_string(),
                measure_name: "test_measures".to_string(),
                time: now,
                measure_2: "poop".to_string(),
                measure_1: 1.0
            }]
        );
    }
}
