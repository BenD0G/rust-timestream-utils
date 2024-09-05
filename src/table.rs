use anyhow::{anyhow, bail, Context, Result};
use aws_sdk_timestreamquery::Client as ReadClient;
use aws_sdk_timestreamquery::{operation::query::QueryOutput, types::ScalarType};
use aws_sdk_timestreamwrite::types::builders::RecordBuilder;
use aws_sdk_timestreamwrite::types::{Record, TimeUnit};
use aws_sdk_timestreamwrite::Client as WriteClient;
use chrono::{DateTime, NaiveDate, NaiveDateTime, TimeZone, Utc};
use futures::future::try_join_all;
use serde::{de::DeserializeOwned, Deserialize, Deserializer};
use serde_json::{json, Value};

pub struct TimestreamClients {
    pub read_client: ReadClient,
    pub write_client: WriteClient,
}

/// Load a default AWS config for eu-west-1, and create read+write Timestream clients.
pub async fn load_timestream_clients() -> Result<TimestreamClients> {
    let config = aws_config::from_env().region("eu-west-1").load().await;
    let (read_client, _) = ReadClient::new(&config)
        .with_endpoint_discovery_enabled()
        .await
        .map_err(|e| anyhow!(e))?;
    let (write_client, _) = WriteClient::new(&config)
        .with_endpoint_discovery_enabled()
        .await
        .map_err(|e| anyhow!(e))?;
    Ok(TimestreamClients {
        read_client,
        write_client,
    })
}

pub struct TimestreamTable<'a> {
    table_name: String,
    db_name: String,
    read_client: &'a aws_sdk_timestreamquery::Client,
    write_client: &'a aws_sdk_timestreamwrite::Client,
}

impl<'a> TimestreamTable<'a> {
    /// Represent an existing table. Note that this function does not interact with AWS at all.
    /// To get the clients, call load_timestream_clients.
    pub fn new(table_name: &str, db_name: &str, clients: &'a TimestreamClients) -> Self {
        TimestreamTable {
            table_name: table_name.to_string(),
            db_name: db_name.to_string(),
            read_client: &clients.read_client,
            write_client: &clients.write_client,
        }
    }

    /// Get the full table name, in the format "db_name"."table_name", for use in Timestream queries.
    pub fn full_table_name(&self) -> String {
        format!("\"{}\".\"{}\"", self.db_name, self.table_name)
    }

    /// Write to Timestream, in a batched fashion.
    pub async fn write_records(
        &self,
        records: &[Record],
        common_attributes: Option<&Record>,
    ) -> Result<()> {
        let futures = records.chunks(100).map(|chunk| {
            let records_chunk = chunk.to_vec();
            async move {
                let builder = self
                    .write_client
                    .write_records()
                    .database_name(&self.db_name)
                    .table_name(&self.table_name);

                let builder = if let Some(common_attributes) = common_attributes {
                    builder.common_attributes(common_attributes.clone())
                } else {
                    builder
                };

                match builder.set_records(Some(records_chunk)).send().await {
                    Ok(_) => Ok(()),
                    Err(e) => Err(anyhow!("{:#?}", e)),
                }
            }
        });

        try_join_all(futures).await?;

        Ok(())
    }

    /// Query this Timestream table, and deserialize the results into your requested type.
    pub async fn query_data<T>(&self, query_string: &str) -> Result<Vec<T>>
    where
        T: DeserializeOwned,
    {
        let query_output = self
            .read_client
            .query()
            .query_string(query_string)
            .send()
            .await?;

        deserialize_query_output(&query_output).with_context(|| {
            format!(
                "Failed to deserialize query output. Output:\n{:#?}",
                &query_output
            )
        })
    }
}

/// Returns error variant on unknown datatype or failed parsing.
/// Note that the "interval" type seems bizarrely represented in the SDK, so I haven't done it yet.
fn make_json_value(timestream_value: &str, scalar_type: &ScalarType) -> Result<Value> {
    match scalar_type {
        ScalarType::Bigint => Ok(json!(timestream_value.parse::<i64>()?)),
        ScalarType::Boolean => Ok(json!(timestream_value.parse::<bool>()?)),
        ScalarType::Double => Ok(json!(timestream_value.parse::<f64>()?)),
        ScalarType::Integer => Ok(json!(timestream_value.parse::<i64>()?)),
        ScalarType::Timestamp => Ok(json!(timestream_value)),
        ScalarType::Varchar => Ok(json!(timestream_value)),
        _ => Err(anyhow::anyhow!("Unknown scalar type: {:?}", scalar_type)),
    }
}

/// Return an error if there are any errors, otherwise just return all of the values.
fn process_results<T>(results: Vec<Result<T>>) -> Result<Vec<T>> {
    let (oks, errs): (Vec<_>, Vec<_>) = results.into_iter().partition(Result::is_ok);

    if !errs.is_empty() {
        let messages = errs
            .into_iter()
            .map(|e| match e {
                Err(err) => format!("{}", err),
                _ => unreachable!(),
            })
            .collect::<Vec<_>>()
            .join(", ");
        bail!(messages);
    }

    Ok(oks.into_iter().map(Result::unwrap).collect())
}

/// Turns the row and column information from Timestream into a JSON object ready for deserialization.
fn convert_query_output_to_json(query_output: &QueryOutput) -> Result<Value> {
    if query_output.rows().is_empty() {
        return Ok(json!([]));
    }

    let mut ret = json!(vec![Value::Null; query_output.rows().len()]);

    let column_name_and_type = process_results(
        query_output
            .column_info()
            .iter()
            .map(|column_info| {
                Ok((
                    column_info
                        .name()
                        .with_context(|| format!("No column name. Info: {:#?}", column_info))?,
                    column_info
                        .r#type()
                        .with_context(|| format!("No column type. Info: {:#?}", column_info))?
                        .scalar_type()
                        .with_context(|| {
                            format!("No column scalar_type. Info: {:#?}", column_info)
                        })?,
                ))
            })
            .collect::<Vec<Result<(&str, &ScalarType)>>>(),
    )?;

    for (i, row) in query_output.rows().iter().enumerate() {
        let mut row_json = json!({});
        for (j, datum) in row.data().iter().enumerate() {
            let (column_name, scalar_type) = column_name_and_type[j];
            row_json[column_name] = make_json_value(
                datum
                    .scalar_value()
                    .with_context(|| format!("Datum missing scalar value. Datum: {:?}", datum))?,
                scalar_type,
            )?;
        }
        ret[i] = row_json;
    }

    Ok(ret)
}

fn deserialize_query_output<T>(query_output: &QueryOutput) -> Result<Vec<T>>
where
    T: DeserializeOwned,
{
    Ok(serde_json::from_value(convert_query_output_to_json(
        query_output,
    )?)?)
}

/// Deserialize a Timestream timestamp into a DateTime<Utc>.
/// This is intended to be used on fields as #[serde(deserialize_with = "utc_datetime_from_timestamp")],
/// hence the weird signature.
pub fn utc_datetime_from_timestamp<'de, D>(
    deserializer: D,
) -> std::result::Result<DateTime<Utc>, D::Error>
where
    D: Deserializer<'de>,
{
    let s = String::deserialize(deserializer)?;
    NaiveDateTime::parse_from_str(&s, "%Y-%m-%d %H:%M:%S.%f")
        .map(|x| Utc.from_utc_datetime(&x))
        .map_err(serde::de::Error::custom)
}

/// Deserialize a Timestream timestamp into a NaiveDate.
/// This is intended to be used on fields as #[serde(deserialize_with = "naive_date_from_timestamp")],
/// hence the weird signature.
pub fn naive_date_from_timestamp<'de, D>(
    deserializer: D,
) -> std::result::Result<NaiveDate, D::Error>
where
    D: Deserializer<'de>,
{
    let s = String::deserialize(deserializer)?;
    let naive_datetime = NaiveDateTime::parse_from_str(&s, "%Y-%m-%d %H:%M:%S%.f")
        .map_err(serde::de::Error::custom)?;
    Ok(naive_datetime.date())
}

/// Utility for setting the time on a record, so you don't have to think about it.
/// *Technically* it can error if the datetime is out of bounds, but that's unlikely.
pub fn make_record_with_time(time: &DateTime<Utc>) -> Result<RecordBuilder> {
    Ok(Record::builder()
        .time(
            time.timestamp_nanos_opt()
                .with_context(|| format!("Could not turn datetime into nanos... {}", time))?
                .to_string(),
        )
        .time_unit(TimeUnit::Nanoseconds))
}

#[cfg(test)]
mod tests {
    use aws_sdk_timestreamquery::types::{ColumnInfo, Datum, Row, Type};
    use chrono::{TimeZone, Timelike};
    use pretty_assertions::assert_eq;
    use serde::Deserialize;

    use super::*;

    #[test]
    fn test_empty_output() {
        let query_output = QueryOutput::builder()
            .query_id("poop")
            .set_rows(Some(vec![]))
            .set_column_info(Some(vec![]))
            .build()
            .unwrap();
        let json = convert_query_output_to_json(&query_output).unwrap();
        assert_eq!(json, json!([]));
    }

    fn dummy_query_output() -> QueryOutput {
        QueryOutput::builder()
            .query_id("AEDQCANVE6VT6AANCNRYFO6UNUXUWNQ6PUKZ6DPYIY3WMW6S5OAKDN2TU4UAIWI")
            .rows(
                Row::builder()
                    .set_data(Some(vec![
                        Datum::builder().scalar_value("test").build(),
                        Datum::builder().scalar_value("test_dimension").build(),
                        Datum::builder().scalar_value("test_measures").build(),
                        Datum::builder()
                            .scalar_value("2024-06-21 09:47:13.433381844")
                            .build(),
                        Datum::builder()
                            .scalar_value("2024-06-21 09:47:13.433381844")
                            .build(),
                        Datum::builder().scalar_value("poop").build(),
                        Datum::builder().scalar_value("1.0").build(),
                    ]))
                    .build()
                    .unwrap(),
            )
            .set_column_info(Some(vec![
                ColumnInfo::builder()
                    .name("test")
                    .r#type(Type::builder().scalar_type(ScalarType::Varchar).build())
                    .build(),
                ColumnInfo::builder()
                    .name("common_dimension")
                    .r#type(Type::builder().scalar_type(ScalarType::Varchar).build())
                    .build(),
                ColumnInfo::builder()
                    .name("measure_name")
                    .r#type(Type::builder().scalar_type(ScalarType::Varchar).build())
                    .build(),
                ColumnInfo::builder()
                    .name("time")
                    .r#type(Type::builder().scalar_type(ScalarType::Timestamp).build())
                    .build(),
                ColumnInfo::builder()
                    .name("date")
                    .r#type(Type::builder().scalar_type(ScalarType::Timestamp).build())
                    .build(),
                ColumnInfo::builder()
                    .name("measure_2")
                    .r#type(Type::builder().scalar_type(ScalarType::Varchar).build())
                    .build(),
                ColumnInfo::builder()
                    .name("measure_1")
                    .r#type(Type::builder().scalar_type(ScalarType::Double).build())
                    .build(),
            ]))
            .build()
            .unwrap()
    }

    /// From a dummy test I ran.
    #[test]
    fn test_full_convert() {
        let query_output = dummy_query_output();

        let json = convert_query_output_to_json(&query_output).unwrap();

        assert_eq!(
            json,
            json!([
                {
                    "test": "test",
                    "common_dimension": "test_dimension",
                    "measure_name": "test_measures",
                    "time": "2024-06-21 09:47:13.433381844",
                    "date": "2024-06-21 09:47:13.433381844",
                    "measure_2": "poop",
                    "measure_1": 1.0
                }
            ])
        );
    }

    #[test]
    fn test_full_deserialize() {
        #[derive(Deserialize, PartialEq, Debug)]
        struct DummyData {
            test: String,
            common_dimension: String,
            measure_name: String,
            #[serde(deserialize_with = "utc_datetime_from_timestamp")]
            time: DateTime<Utc>,
            measure_2: String,
            measure_1: f64,
            #[serde(deserialize_with = "naive_date_from_timestamp")]
            date: NaiveDate,
        }
        let query_output = dummy_query_output();

        let data: Vec<DummyData> = deserialize_query_output(&query_output).unwrap();

        assert_eq!(
            data,
            vec![DummyData {
                test: "test".to_string(),
                common_dimension: "test_dimension".to_string(),
                measure_name: "test_measures".to_string(),
                time: Utc
                    .with_ymd_and_hms(2024, 6, 21, 9, 47, 13)
                    .unwrap()
                    .with_nanosecond(433381844)
                    .unwrap(),
                date: NaiveDate::from_ymd_opt(2024, 6, 21).unwrap(),
                measure_2: "poop".to_string(),
                measure_1: 1.0
            }]
        );
    }
}
