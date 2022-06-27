use {
    crate::{
        accounts::Accounts,
        accounts_db::{
            AccountInfoAccountsIndex, AccountShrinkThreshold, AccountStorageEntry, AccountsDb,
            AccountsDbConfig, BankHashInfo, SlotStores, SnapshotStorage,
        },
        accounts_index::AccountSecondaryIndexes,
        accounts_update_notifier_interface::AccountsUpdateNotifier,
        append_vec::StoredMetaWriteVersion,
        bank::{Bank, BankFieldsToDeserialize, BankRc},
        blockhash_queue::BlockhashQueue,
        builtins::Builtins,
        epoch_stakes::EpochStakes,
        rent_collector::RentCollector,
        snapshot_utils::{self, BANK_SNAPSHOT_PRE_FILENAME_EXTENSION},
        stakes::Stakes,
    },
    bincode::{self, config::Options, Error},
    dashmap::DashMap,
    log::*,
    serde::{de::DeserializeOwned, Deserialize, Serialize},
    solana_measure::measure::Measure,
    solana_sdk::{
        clock::{Epoch, Slot, UnixTimestamp},
        deserialize_utils::default_on_eof,
        epoch_schedule::EpochSchedule,
        fee_calculator::{FeeCalculator, FeeRateGovernor},
        genesis_config::GenesisConfig,
        hard_forks::HardForks,
        hash::Hash,
        inflation::Inflation,
        pubkey::Pubkey,
    },
    std::{
        collections::{HashMap, HashSet},
        io::{self, BufReader, BufWriter, Read, Write},
        path::{Path, PathBuf},
        result::Result,
        sync::{
            atomic::{AtomicU32, Ordering},
            Arc,
        },
        thread::Builder,
    },
    storage::SerializableStorage,
};

mod newer;
mod storage;
mod tests;
mod utils;

// a number of test cases in accounts_db use this
#[cfg(test)]
pub(crate) use tests::reconstruct_accounts_db_via_serialization;

#[derive(Copy, Clone, Eq, PartialEq)]
pub(crate) enum SerdeStyle {
    Newer,
}

const MAX_STREAM_SIZE: u64 = 32 * 1024 * 1024 * 1024;

#[derive(Clone, Debug, Default, Deserialize, Serialize, AbiExample, PartialEq)]
struct AccountsDbFields<T>(
    HashMap<Slot, Vec<T>>,
    StoredMetaWriteVersion,
    Slot,
    BankHashInfo,
    /// all slots that were roots within the last epoch
    #[serde(deserialize_with = "default_on_eof")]
    Vec<Slot>,
    /// slots that were roots within the last epoch for which we care about the hash value
    #[serde(deserialize_with = "default_on_eof")]
    Vec<(Slot, Hash)>,
);

/// Helper type to wrap BufReader streams when deserializing and reconstructing from either just a
/// full snapshot, or both a full and incremental snapshot
pub struct SnapshotStreams<'a, R> {
    pub full_snapshot_stream: &'a mut BufReader<R>,
    pub incremental_snapshot_stream: Option<&'a mut BufReader<R>>,
}

/// Helper type to wrap AccountsDbFields when reconstructing AccountsDb from either just a full
/// snapshot, or both a full and incremental snapshot
#[derive(Debug)]
struct SnapshotAccountsDbFields<T> {
    full_snapshot_accounts_db_fields: AccountsDbFields<T>,
    incremental_snapshot_accounts_db_fields: Option<AccountsDbFields<T>>,
}

impl<T> SnapshotAccountsDbFields<T> {
    /// Collapse the SnapshotAccountsDbFields into a single AccountsDbFields.  If there is no
    /// incremental snapshot, this returns the AccountsDbFields from the full snapshot.
    /// Otherwise, use the AccountsDbFields from the incremental snapshot, and a combination
    /// of the storages from both the full and incremental snapshots.
    fn collapse_into(self) -> Result<AccountsDbFields<T>, Error> {
        match self.incremental_snapshot_accounts_db_fields {
            None => Ok(self.full_snapshot_accounts_db_fields),
            Some(AccountsDbFields(
                mut incremental_snapshot_storages,
                incremental_snapshot_version,
                incremental_snapshot_slot,
                incremental_snapshot_bank_hash_info,
                incremental_snapshot_historical_roots,
                incremental_snapshot_historical_roots_with_hash,
            )) => {
                let full_snapshot_storages = self.full_snapshot_accounts_db_fields.0;
                let full_snapshot_slot = self.full_snapshot_accounts_db_fields.2;

                // filter out incremental snapshot storages with slot <= full snapshot slot
                incremental_snapshot_storages.retain(|slot, _| *slot > full_snapshot_slot);

                // There must not be any overlap in the slots of storages between the full snapshot and the incremental snapshot
                incremental_snapshot_storages
                    .iter()
                    .all(|storage_entry| !full_snapshot_storages.contains_key(storage_entry.0)).then(|| ()).ok_or_else(|| {
                        io::Error::new(io::ErrorKind::InvalidData, "Snapshots are incompatible: There are storages for the same slot in both the full snapshot and the incremental snapshot!")
                    })?;

                let mut combined_storages = full_snapshot_storages;
                combined_storages.extend(incremental_snapshot_storages.into_iter());

                Ok(AccountsDbFields(
                    combined_storages,
                    incremental_snapshot_version,
                    incremental_snapshot_slot,
                    incremental_snapshot_bank_hash_info,
                    incremental_snapshot_historical_roots,
                    incremental_snapshot_historical_roots_with_hash,
                ))
            }
        }
    }
}

trait TypeContext<'a>: PartialEq {
    type SerializableAccountStorageEntry: Serialize
        + DeserializeOwned
        + From<&'a AccountStorageEntry>
        + SerializableStorage
        + Sync;

    fn serialize_bank_and_storage<S: serde::ser::Serializer>(
        serializer: S,
        serializable_bank: &SerializableBankAndStorage<'a, Self>,
    ) -> std::result::Result<S::Ok, S::Error>
    where
        Self: std::marker::Sized;

    #[cfg(test)]
    fn serialize_bank_and_storage_without_extra_fields<S: serde::ser::Serializer>(
        serializer: S,
        serializable_bank: &SerializableBankAndStorageNoExtra<'a, Self>,
    ) -> std::result::Result<S::Ok, S::Error>
    where
        Self: std::marker::Sized;

    fn serialize_accounts_db_fields<S: serde::ser::Serializer>(
        serializer: S,
        serializable_db: &SerializableAccountsDb<'a, Self>,
    ) -> std::result::Result<S::Ok, S::Error>
    where
        Self: std::marker::Sized;

    fn deserialize_bank_fields<R>(
        stream: &mut BufReader<R>,
    ) -> Result<
        (
            BankFieldsToDeserialize,
            AccountsDbFields<Self::SerializableAccountStorageEntry>,
        ),
        Error,
    >
    where
        R: Read;

    fn deserialize_accounts_db_fields<R>(
        stream: &mut BufReader<R>,
    ) -> Result<AccountsDbFields<Self::SerializableAccountStorageEntry>, Error>
    where
        R: Read;

    /// deserialize the bank from 'stream_reader'
    /// modify the accounts_hash
    /// reserialize the bank to 'stream_writer'
    fn reserialize_bank_fields_with_hash<R, W>(
        stream_reader: &mut BufReader<R>,
        stream_writer: &mut BufWriter<W>,
        accounts_hash: &Hash,
    ) -> std::result::Result<(), Box<bincode::ErrorKind>>
    where
        R: Read,
        W: Write;
}

fn deserialize_from<R, T>(reader: R) -> bincode::Result<T>
where
    R: Read,
    T: DeserializeOwned,
{
    bincode::options()
        .with_limit(MAX_STREAM_SIZE)
        .with_fixint_encoding()
        .allow_trailing_bytes()
        .deserialize_from::<R, T>(reader)
}

/// used by tests to compare contents of serialized bank fields
/// serialized format is not deterministic - likely due to randomness in structs like hashmaps
pub(crate) fn compare_two_serialized_banks(
    path1: impl AsRef<Path>,
    path2: impl AsRef<Path>,
) -> std::result::Result<bool, Error> {
    use std::fs::File;
    let file1 = File::open(path1)?;
    let mut stream1 = BufReader::new(file1);
    let file2 = File::open(path2)?;
    let mut stream2 = BufReader::new(file2);

    let fields1 = newer::Context::deserialize_bank_fields(&mut stream1)?;
    let fields2 = newer::Context::deserialize_bank_fields(&mut stream2)?;
    Ok(fields1 == fields2)
}

pub(crate) fn snapshot_stream_to_snapshot_storages<R>(
    serde_style: SerdeStyle,
    snapshot_stream: &mut BufReader<R>,
) -> std::result::Result<HashMap<Slot, HashMap<usize, usize>>, Error>
where
    R: Read,
{
    macro_rules! INTO {
        ($style:ident) => {{
            let (_, accounts_db_fields) =
                $style::Context::deserialize_bank_fields(snapshot_stream)?;

            let AccountsDbFields(snapshot_storages, ..) = accounts_db_fields;

            let snapshot_storages = snapshot_storages
                .into_iter()
                .map(|(slot, entries)| {
                    (
                        slot,
                        entries
                            .into_iter()
                            .map(|entry| (entry.id(), entry.current_len()))
                            .collect::<HashMap<_, _>>(),
                    )
                })
                .collect::<HashMap<_, _>>();

            Ok(snapshot_storages)
        }};
    }
    match serde_style {
        SerdeStyle::Newer => INTO!(newer),
    }
    .map_err(|err| {
        warn!("snapshot_stream_to error: {:?}", err);
        err
    })
}

#[allow(clippy::too_many_arguments)]
pub(crate) fn bank_from_streams<R>(
    serde_style: SerdeStyle,
    snapshot_streams: &mut SnapshotStreams<R>,
    account_paths: &[PathBuf],
    storage: DashMap<Slot, SlotStores>,
    uncleaned_pubkeys: DashMap<Slot, HashSet<Pubkey>>,
    accounts_data_len: u64,
    next_append_vec_id: AtomicU32,
    genesis_config: &GenesisConfig,
    debug_keys: Option<Arc<HashSet<Pubkey>>>,
    additional_builtins: Option<&Builtins>,
    accounts_index: AccountInfoAccountsIndex,
    account_secondary_indexes: AccountSecondaryIndexes,
    caching_enabled: bool,
    limit_load_slot_count_from_snapshot: Option<usize>,
    shrink_ratio: AccountShrinkThreshold,
    verify_index: bool,
    accounts_db_config: Option<AccountsDbConfig>,
    accounts_update_notifier: Option<AccountsUpdateNotifier>,
    accounts_db_skip_shrink: bool,
) -> std::result::Result<Bank, Error>
where
    R: Read,
{
    macro_rules! INTO {
        ($style:ident) => {{
            let (full_snapshot_bank_fields, full_snapshot_accounts_db_fields) =
                $style::Context::deserialize_bank_fields(snapshot_streams.full_snapshot_stream)?;
            let (incremental_snapshot_bank_fields, incremental_snapshot_accounts_db_fields) =
                if let Some(ref mut incremental_snapshot_stream) =
                    snapshot_streams.incremental_snapshot_stream
                {
                    let (bank_fields, accounts_db_fields) =
                        $style::Context::deserialize_bank_fields(incremental_snapshot_stream)?;
                    (Some(bank_fields), Some(accounts_db_fields))
                } else {
                    (None, None)
                };

            let snapshot_accounts_db_fields = SnapshotAccountsDbFields {
                full_snapshot_accounts_db_fields,
                incremental_snapshot_accounts_db_fields,
            };
            let bank = reconstruct_bank_from_fields(
                incremental_snapshot_bank_fields.unwrap_or(full_snapshot_bank_fields),
                snapshot_accounts_db_fields,
                genesis_config,
                account_paths,
                storage,
                uncleaned_pubkeys,
                accounts_data_len,
                next_append_vec_id,
                debug_keys,
                additional_builtins,
                accounts_index,
                account_secondary_indexes,
                caching_enabled,
                limit_load_slot_count_from_snapshot,
                shrink_ratio,
                verify_index,
                accounts_db_config,
                accounts_update_notifier,
                accounts_db_skip_shrink,
            )?;
            Ok(bank)
        }};
    }
    match serde_style {
        SerdeStyle::Newer => INTO!(newer),
    }
    .map_err(|err| {
        warn!("bankrc_from_stream error: {:?}", err);
        err
    })
}

pub(crate) fn bank_to_stream<W>(
    serde_style: SerdeStyle,
    stream: &mut BufWriter<W>,
    bank: &Bank,
    snapshot_storages: &[SnapshotStorage],
) -> Result<(), Error>
where
    W: Write,
{
    macro_rules! INTO {
        ($style:ident) => {
            bincode::serialize_into(
                stream,
                &SerializableBankAndStorage::<$style::Context> {
                    bank,
                    snapshot_storages,
                    phantom: std::marker::PhantomData::default(),
                },
            )
        };
    }
    match serde_style {
        SerdeStyle::Newer => INTO!(newer),
    }
    .map_err(|err| {
        warn!("bankrc_to_stream error: {:?}", err);
        err
    })
}

#[cfg(test)]
pub(crate) fn bank_to_stream_no_extra_fields<W>(
    serde_style: SerdeStyle,
    stream: &mut BufWriter<W>,
    bank: &Bank,
    snapshot_storages: &[SnapshotStorage],
) -> Result<(), Error>
where
    W: Write,
{
    macro_rules! INTO {
        ($style:ident) => {
            bincode::serialize_into(
                stream,
                &SerializableBankAndStorageNoExtra::<$style::Context> {
                    bank,
                    snapshot_storages,
                    phantom: std::marker::PhantomData::default(),
                },
            )
        };
    }
    match serde_style {
        SerdeStyle::Newer => INTO!(newer),
    }
    .map_err(|err| {
        warn!("bankrc_to_stream error: {:?}", err);
        err
    })
}

/// deserialize the bank from 'stream_reader'
/// modify the accounts_hash
/// reserialize the bank to 'stream_writer'
fn reserialize_bank_fields_with_new_hash<W, R>(
    stream_reader: &mut BufReader<R>,
    stream_writer: &mut BufWriter<W>,
    accounts_hash: &Hash,
) -> Result<(), Error>
where
    W: Write,
    R: Read,
{
    newer::Context::reserialize_bank_fields_with_hash(stream_reader, stream_writer, accounts_hash)
}

/// effectively updates the accounts hash in the serialized bank file on disk
/// read serialized bank from pre file
/// update accounts_hash
/// write serialized bank to post file
/// return true if pre file found
pub fn reserialize_bank_with_new_accounts_hash(
    bank_snapshots_dir: impl AsRef<Path>,
    slot: Slot,
    accounts_hash: &Hash,
) -> bool {
    let bank_post = snapshot_utils::get_bank_snapshots_dir(bank_snapshots_dir, slot);
    let bank_post = bank_post.join(snapshot_utils::get_snapshot_file_name(slot));
    let mut bank_pre = bank_post.clone();
    bank_pre.set_extension(BANK_SNAPSHOT_PRE_FILENAME_EXTENSION);

    let mut found = false;
    {
        let file = std::fs::File::open(&bank_pre);
        // some tests don't create the file
        if let Ok(file) = file {
            found = true;
            let file_out = std::fs::File::create(bank_post).unwrap();
            reserialize_bank_fields_with_new_hash(
                &mut BufReader::new(file),
                &mut BufWriter::new(file_out),
                accounts_hash,
            )
            .unwrap();
        }
    }
    if found {
        std::fs::remove_file(bank_pre).unwrap();
    }
    found
}

struct SerializableBankAndStorage<'a, C> {
    bank: &'a Bank,
    snapshot_storages: &'a [SnapshotStorage],
    phantom: std::marker::PhantomData<C>,
}

impl<'a, C: TypeContext<'a>> Serialize for SerializableBankAndStorage<'a, C> {
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::ser::Serializer,
    {
        C::serialize_bank_and_storage(serializer, self)
    }
}

#[cfg(test)]
struct SerializableBankAndStorageNoExtra<'a, C> {
    bank: &'a Bank,
    snapshot_storages: &'a [SnapshotStorage],
    phantom: std::marker::PhantomData<C>,
}

#[cfg(test)]
impl<'a, C: TypeContext<'a>> Serialize for SerializableBankAndStorageNoExtra<'a, C> {
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::ser::Serializer,
    {
        C::serialize_bank_and_storage_without_extra_fields(serializer, self)
    }
}

#[cfg(test)]
impl<'a, C> From<SerializableBankAndStorageNoExtra<'a, C>> for SerializableBankAndStorage<'a, C> {
    fn from(s: SerializableBankAndStorageNoExtra<'a, C>) -> SerializableBankAndStorage<'a, C> {
        let SerializableBankAndStorageNoExtra {
            bank,
            snapshot_storages,
            phantom,
        } = s;
        SerializableBankAndStorage {
            bank,
            snapshot_storages,
            phantom,
        }
    }
}

struct SerializableAccountsDb<'a, C> {
    accounts_db: &'a AccountsDb,
    slot: Slot,
    account_storage_entries: &'a [SnapshotStorage],
    phantom: std::marker::PhantomData<C>,
}

impl<'a, C: TypeContext<'a>> Serialize for SerializableAccountsDb<'a, C> {
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::ser::Serializer,
    {
        C::serialize_accounts_db_fields(serializer, self)
    }
}

#[cfg(RUSTC_WITH_SPECIALIZATION)]
impl<'a, C> solana_frozen_abi::abi_example::IgnoreAsHelper for SerializableAccountsDb<'a, C> {}

#[allow(clippy::too_many_arguments)]
fn reconstruct_bank_from_fields<E>(
    bank_fields: BankFieldsToDeserialize,
    snapshot_accounts_db_fields: SnapshotAccountsDbFields<E>,
    genesis_config: &GenesisConfig,
    account_paths: &[PathBuf],
    storage: DashMap<Slot, SlotStores>,
    uncleaned_pubkeys: DashMap<Slot, HashSet<Pubkey>>,
    accounts_data_len: u64,
    next_append_vec_id: AtomicU32,
    debug_keys: Option<Arc<HashSet<Pubkey>>>,
    additional_builtins: Option<&Builtins>,
    accounts_index: AccountInfoAccountsIndex,
    account_secondary_indexes: AccountSecondaryIndexes,
    caching_enabled: bool,
    limit_load_slot_count_from_snapshot: Option<usize>,
    shrink_ratio: AccountShrinkThreshold,
    verify_index: bool,
    accounts_db_config: Option<AccountsDbConfig>,
    accounts_update_notifier: Option<AccountsUpdateNotifier>,
    accounts_db_skip_shrink: bool,
) -> Result<Bank, Error>
where
    E: SerializableStorage + std::marker::Sync,
{
    let (accounts_db, reconstructed_accounts_db_info) = reconstruct_accountsdb_from_fields(
        snapshot_accounts_db_fields,
        account_paths,
        storage,
        uncleaned_pubkeys,
        accounts_data_len,
        next_append_vec_id,
        genesis_config,
        accounts_index,
        account_secondary_indexes,
        caching_enabled,
        shrink_ratio,
        verify_index,
        accounts_db_config,
        accounts_update_notifier,
        accounts_db_skip_shrink,
    )?;

    let bank_rc = BankRc::new(Accounts::new_empty(accounts_db), bank_fields.slot);

    // if limit_load_slot_count_from_snapshot is set, then we need to side-step some correctness checks beneath this call
    let debug_do_not_add_builtins = limit_load_slot_count_from_snapshot.is_some();
    let bank = Bank::new_from_fields(
        bank_rc,
        genesis_config,
        bank_fields,
        debug_keys,
        additional_builtins,
        debug_do_not_add_builtins,
        reconstructed_accounts_db_info.accounts_data_len,
    );

    info!("rent_collector: {:?}", bank.rent_collector());

    Ok(bank)
}

/// This struct contains side-info while reconstructing the accounts DB from fields.
#[derive(Debug, Default, Copy, Clone)]
struct ReconstructedAccountsDbInfo {
    accounts_data_len: u64,
}

#[allow(clippy::too_many_arguments)]
fn reconstruct_accountsdb_from_fields<E>(
    snapshot_accounts_db_fields: SnapshotAccountsDbFields<E>,
    account_paths: &[PathBuf],
    storage: DashMap<Slot, SlotStores>,
    uncleaned_pubkeys: DashMap<Slot, HashSet<Pubkey>>,
    accounts_data_len: u64,
    next_append_vec_id: AtomicU32,
    genesis_config: &GenesisConfig,
    accounts_index: AccountInfoAccountsIndex,
    account_secondary_indexes: AccountSecondaryIndexes,
    caching_enabled: bool,
    shrink_ratio: AccountShrinkThreshold,
    verify_index: bool,
    accounts_db_config: Option<AccountsDbConfig>,
    accounts_update_notifier: Option<AccountsUpdateNotifier>,
    accounts_db_skip_shrink: bool,
) -> Result<(AccountsDb, ReconstructedAccountsDbInfo), Error>
where
    E: SerializableStorage + std::marker::Sync,
{
    let mut accounts_db = AccountsDb::new_with_config_preindexed(
        account_paths.to_vec(),
        &genesis_config.cluster_type,
        storage,
        accounts_index,
        account_secondary_indexes,
        caching_enabled,
        shrink_ratio,
        accounts_db_config,
        accounts_update_notifier,
    );

    let AccountsDbFields(
        ..,
        snapshot_version,
        snapshot_slot,
        snapshot_bank_hash_info,
        snapshot_historical_roots,
        snapshot_historical_roots_with_hash,
    ) = snapshot_accounts_db_fields.collapse_into()?;

    reconstruct_historical_roots(
        &accounts_db,
        snapshot_historical_roots,
        snapshot_historical_roots_with_hash,
    );

    // Process deserialized data, set necessary fields in self
    accounts_db
        .bank_hashes
        .write()
        .unwrap()
        .insert(snapshot_slot, snapshot_bank_hash_info);

    accounts_db.next_id = next_append_vec_id;
    accounts_db
        .write_version
        .fetch_add(snapshot_version, Ordering::Release);

    let mut measure_notify = Measure::start("accounts_notify");

    let accounts_db = Arc::new(accounts_db);
    let accounts_db_clone = accounts_db.clone();
    let handle = Builder::new()
        .name("notify_account_restore_from_snapshot".to_string())
        .spawn(move || {
            accounts_db_clone.notify_account_restore_from_snapshot();
        })
        .unwrap();

    let accounts_data_len = accounts_db.finalize_index(
        accounts_db_skip_shrink,
        uncleaned_pubkeys,
        accounts_data_len,
    );

    accounts_db.maybe_add_filler_accounts(
        &genesis_config.epoch_schedule,
        &genesis_config.rent,
        snapshot_slot,
    );

    handle.join().unwrap();
    measure_notify.stop();

    Ok((
        Arc::try_unwrap(accounts_db).unwrap(),
        ReconstructedAccountsDbInfo { accounts_data_len },
    ))
}

/// populate 'historical_roots' from 'snapshot_historical_roots' and 'snapshot_historical_roots_with_hash'
fn reconstruct_historical_roots(
    accounts_db: &AccountsDb,
    mut snapshot_historical_roots: Vec<Slot>,
    snapshot_historical_roots_with_hash: Vec<(Slot, Hash)>,
) {
    // inflate 'historical_roots'
    // inserting into 'historical_roots' needs to be in order
    // combine the slots into 1 vec, then sort
    // dups are ok
    snapshot_historical_roots.extend(
        snapshot_historical_roots_with_hash
            .into_iter()
            .map(|(root, _)| root),
    );
    snapshot_historical_roots.sort_unstable();
    let mut roots_tracker = accounts_db.accounts_index.roots_tracker.write().unwrap();
    snapshot_historical_roots.into_iter().for_each(|root| {
        roots_tracker.historical_roots.insert(root);
    });
}
