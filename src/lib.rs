mod table;
#[cfg(feature = "test-utils")]
mod test_utils;

pub use table::{
    load_timestream_clients, make_record_with_time, naive_date_from_timestamp,
    utc_datetime_from_timestamp, TimestreamClients, TimestreamTable,
};

#[cfg(feature = "test-utils")]
pub use test_utils::make_droppable_table;
