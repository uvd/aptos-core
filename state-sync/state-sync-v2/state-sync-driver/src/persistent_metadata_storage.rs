// Copyright (c) Aptos
// SPDX-License-Identifier: Apache-2.0

use crate::{
    error::Error,
    persistent_metadata_storage::database_schema::{MetadataKey, MetadataSchema, MetadataValue},
};
use anyhow::{format_err, Result};
use aptos_crypto::HashValue;
use aptos_logger::prelude::*;
use aptos_types::transaction::Version;
use num_derive::{FromPrimitive, ToPrimitive};
use num_traits::{FromPrimitive, ToPrimitive};
use schemadb::{
    define_schema,
    schema::{KeyCodec, ValueCodec},
    Options, ReadOptions, SchemaBatch, DB,
};
use std::{collections::HashMap, iter::Iterator, mem::size_of, path::Path, time::Instant};
use tokio::io::AsyncReadExt;

/// The name of the state sync db file
pub const STATE_SYNC_DB_NAME: &str = "state_sync_db";

/// This struct offers a simple interface to persist state sync metadata.
/// This is required to handle failures and reboots during critical parts
/// of the synchronization process.
pub struct PersistentMetadataStorage {
    database: DB,
}

impl PersistentMetadataStorage {
    pub fn new<P: AsRef<Path> + Clone>(db_root_path: P) -> Self {
        // Set the options to create the database if it's missing
        let mut options = Options::default();
        options.create_if_missing(true);
        options.create_missing_column_families(true);

        // Open the database
        let state_sync_db_path = db_root_path.as_ref().join(STATE_SYNC_DB_NAME);
        let instant = Instant::now();
        let database = DB::open(
            state_sync_db_path.clone(),
            "state_sync",
            vec![METADATA_CF_NAME],
            &options,
        )
        .expect(
            "Failed to open/create the state sync database at: {:?}",
            state_sync_db_path,
        );
        info!(
            "Opened the state sync database at: {:?}, in {:?} ms",
            state_sync_db_path,
            instant.elapsed().as_millis()
        );

        Self { database }
    }

    /// Returns true iff a state snapshot was successfully synced for the given version
    pub fn is_snapshot_sync_complete(&self, version: Version) -> Result<bool, Error> {
        let snapshot_sync_progress = self.get_snapshot_sync_progress_at_version(version)?;
        let result = match snapshot_sync_progress {
            Some(snapshot_sync_progress) => snapshot_sync_progress.snapshot_sync_completed,
            None => false, // No snapshot sync progress was found
        };
        Ok(result)
    }

    /// Gets the last persisted state value index for the snapshot sync at the
    /// specified version. If no snapshot progress was found, an error is returned.
    pub fn get_last_persisted_state_value_index(&self, version: Version) -> Result<u64, Error> {
        let snapshot_sync_progress = self.get_snapshot_sync_progress_at_version(version)?;
        match snapshot_sync_progress {
            Some(snapshot_sync_progress) => {
                Ok(snapshot_sync_progress.last_persisted_state_value_index)
            }
            None => Err(Error::StorageError(format!(
                "No state snapshot progress was found for version: {:?}",
                version
            ))),
        }
    }

    /// Returns the snapshot sync progress recorded for the specified version
    fn get_snapshot_sync_progress_at_version(
        &self,
        version: Version,
    ) -> Result<Option<StateSnapshotSyncProgress>, Error> {
        Ok(self
            .database
            .get::<MetadataSchema>(&MetadataKey::StateSnapshotSync(version)?))
    }

    /// Updates the last persisted state value index for the state snapshot
    /// sync at the specified version.
    pub fn update_last_persisted_state_value_index(
        &self,
        version: Version,
        last_persisted_state_value_index: u64,
        snapshot_sync_completed: bool,
    ) -> Result<(), Error> {
        let mut batch = SchemaBatch::new();
        batch.put::<MetadataSchema>(
            &MetadataKey::StateSnapshotSync(version),
            &MetadataValue::StateSnapshotSync(StateSnapshotSyncProgress {
                last_persisted_state_value_index,
                snapshot_sync_completed,
            }),
        )?;
        self.commit(batch)
    }

    /// Write the schema batch to the database
    fn commit(&self, batch: SchemaBatch) -> Result<(), Error> {
        self.db.write_schemas(batch)
    }
}

/// A simple struct for recording the progress of a state snapshot sync
pub struct StateSnapshotSyncProgress {
    pub last_persisted_state_value_index: u64,
    pub snapshot_sync_completed: bool,
}

/// The raw schema format used by the database
mod database_schema {
    use super::*;
    use anyhow::ensure;

    /// This defines a physical storage schema for any metadata.
    ///
    /// The key will be a serialized MetadataKey type.
    /// The value will be a serialized MetadataValue type.
    ///
    /// ```text
    /// |<-------key------->|<-----value----->|
    /// |   metadata key    | metadata value  |
    /// ```
    define_schema!(MetadataSchema, MetadataKey, MetadataValue, METADATA_CF_NAME);

    /// A metadata key that can be inserted into the database
    #[derive(Debug, Eq, PartialEq, FromPrimitive, ToPrimitive)]
    #[repr(u8)]
    pub enum MetadataKey {
        StateSnapshotSync(Version), // A state snapshot sync that was executed at the specified version
    }

    /// A metadata value that can be inserted into the database
    #[derive(Debug, Eq, PartialEq, FromPrimitive, ToPrimitive)]
    #[repr(u8)]
    pub enum MetadataValue {
        StateSnapshotSync(StateSnapshotSyncProgress), // A state snapshot sync progress marker
    }

    impl KeyCodec<MetadataSchema> for MetadataKey {
        fn encode_key(&self) -> Result<Vec<u8>> {
            Ok(vec![self.to_u8().ok_or_else(|| {
                format_err!("ToPrimitive failed for MetadataKey!")
            })?])
        }

        fn decode_key(mut data: &[u8]) -> Result<Self> {
            ensure_slice_len_eq(data, size_of::<u8>())?;
            let metadata_key = data.read_u8()?;
            MetadataKey::from_u8(metadata_key)
                .ok_or_else(|| format_err!("FromPrimitive failed for MetadataKey!"))
        }
    }

    impl ValueCodec<MetadataSchema> for MetadataKey {
        fn encode_value(&self) -> Result<Vec<u8>> {
            Ok(vec![self.to_u8().ok_or_else(|| {
                format_err!("ToPrimitive failed for MetadataValue!")
            })?])
        }

        fn decode_value(data: &[u8]) -> Result<Self> {
            ensure_slice_len_eq(data, size_of::<u8>())?;
            let metadata_value = data.clone().read_u8()?;
            MetadataValue::from_u8(metadata_value)
                .ok_or_else(|| format_err!("FromPrimitive failed for MetadataKey!"))
        }
    }

    fn ensure_slice_len_eq(data: &[u8], len: usize) -> Result<()> {
        ensure!(
            data.len() == len,
            "Unexpected data len {}, expected {}.",
            data.len(),
            len
        );
        Ok(())
    }

    /// Simple unit tests to ensure encoding and decoding works
    #[cfg(test)]
    mod test {
        use super::*;

        #[test]
        fn test_metadata_schema_encode_decode() {
            assert_encode_decode::<MetadataSchema>(
                &MetadataKey::StateSnapshotSync(123456789),
                &vec![1u8, 2u8, 3u8],
            );
        }

        test_no_panic_decoding!(MetadataSchema);
    }
}
