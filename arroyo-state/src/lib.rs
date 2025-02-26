use crate::tables::DataTuple;
use anyhow::Result;
use arroyo_rpc::grpc::{
    CheckpointMetadata, OperatorCheckpointMetadata, TableDeleteBehavior, TableDescriptor,
    TableType, TableWriteBehavior,
};
use arroyo_rpc::{CompactionResult, ControlResp};
use arroyo_types::{CheckpointBarrier, Data, Key, TaskInfo};
use async_trait::async_trait;
use bincode::config::Configuration;
use bincode::{Decode, Encode};
use std::any::Any;
use std::collections::hash_map::DefaultHasher;
use std::collections::HashMap;
use std::hash::{Hash, Hasher};
use std::ops::RangeInclusive;
use std::time::{Duration, SystemTime};
use tables::global_keyed_map::{GlobalKeyedState, GlobalKeyedStateCache};
use tables::key_time_multi_map::{KeyTimeMultiMap, KeyTimeMultiMapCache};
use tables::keyed_map::{KeyedState, KeyedStateCache};
use tables::time_key_map::{TimeKeyMap, TimeKeyMapCache};
use tables::{global_keyed_map, key_time_multi_map, keyed_map, time_key_map};
use tokio::sync::mpsc::Sender;

mod metrics;
pub mod parquet;
pub mod tables;

pub const BINCODE_CONFIG: Configuration = bincode::config::standard();
pub const FULL_KEY_RANGE: RangeInclusive<u64> = 0..=u64::MAX;

pub type StateBackend = parquet::ParquetBackend;

pub fn global_table(name: impl Into<String>, description: impl Into<String>) -> TableDescriptor {
    TableDescriptor {
        name: name.into(),
        description: description.into(),
        table_type: TableType::Global as i32,
        delete_behavior: TableDeleteBehavior::None as i32,
        write_behavior: TableWriteBehavior::DefaultWrites as i32,
        retention_micros: 0,
    }
}

pub fn timestamp_table(
    name: impl Into<String>,
    description: impl Into<String>,
    delete_behavior: TableDeleteBehavior,
    write_behavior: TableWriteBehavior,
    retention: Duration,
) -> TableDescriptor {
    TableDescriptor {
        name: name.into(),
        description: description.into(),
        table_type: TableType::TimeKeyMap as i32,
        delete_behavior: delete_behavior as i32,
        write_behavior: write_behavior as i32,
        retention_micros: retention.as_micros() as u64,
    }
}

#[derive(Debug, Encode, Decode)]
#[repr(u8)]
pub enum DataOperation {
    Insert = 0,
    DeleteKey = 1, // delete single key/value pair of Global/TimeKeyMap
                   // DeleteValue,  // delete single value of a KeyTimeMultiMap
                   // DeleteBefore, // delete all values for key before timestamp (only for KeyTimeMultiMap)
}

impl From<u8> for DataOperation {
    fn from(op: u8) -> Self {
        match op {
            0 => DataOperation::Insert,
            1 => DataOperation::DeleteKey,
            _ => panic!("Unknown DataOperation {}", op),
        }
    }
}

#[async_trait]
pub trait BackingStore {
    // prepares a checkpoint to be loaded, e.g., by deleting future data
    async fn prepare_checkpoint_load(metadata: &CheckpointMetadata) -> Result<()>;

    async fn load_latest_checkpoint_metadata(job_id: &str) -> Option<CheckpointMetadata>;

    async fn load_checkpoint_metadata(job_id: &str, epoch: u32) -> Option<CheckpointMetadata>;

    async fn load_operator_metadata(
        job_id: &str,
        operator_id: &str,
        epoch: u32,
    ) -> Option<OperatorCheckpointMetadata>;

    async fn new(
        task_info: &TaskInfo,
        tables: Vec<TableDescriptor>,
        control_tx: Sender<ControlResp>,
    ) -> Self;
    async fn from_checkpoint(
        task_info: &TaskInfo,
        metadata: CheckpointMetadata,
        tables: Vec<TableDescriptor>,
        control_tx: Sender<ControlResp>,
    ) -> Self;

    fn name() -> &'static str;

    fn task_info(&self) -> &TaskInfo;

    // prepares a checkpoint to be written
    #[allow(unused_variables)]
    async fn initialize_checkpoint(job_id: &str, epoch: u32, operators: &[&str]) -> Result<()> {
        Ok(())
    }

    async fn write_operator_checkpoint_metadata(metadata: OperatorCheckpointMetadata);

    async fn complete_checkpoint(metadata: CheckpointMetadata);

    async fn cleanup_checkpoint(
        metadata: CheckpointMetadata,
        old_min_epoch: u32,
        new_min_epoch: u32,
    ) -> Result<()>;

    async fn checkpoint(
        &mut self,
        barrier: CheckpointBarrier,
        watermark: Option<SystemTime>,
    ) -> u32;

    async fn get_data_tuples<K: Key, V: Data>(&self, table: char) -> Vec<DataTuple<K, V>>;

    async fn write_data_tuple<K: Key, V: Data>(
        &mut self,
        table: char,
        table_type: TableType,
        timestamp: SystemTime,
        key: &mut K,
        value: &mut V,
    );

    async fn delete_data_tuple<K: Key>(
        &mut self,
        table: char,
        table_type: TableType,
        timestamp: SystemTime,
        key: &mut K,
    );

    async fn write_key_value<K: Key, V: Data>(&mut self, table: char, key: &mut K, value: &mut V);
    async fn delete_key_value<K: Key>(&mut self, table: char, key: &mut K);

    async fn get_global_key_values<K: Key, V: Data>(&self, table: char) -> Vec<(K, V)>;
    async fn get_key_values<K: Key, V: Data>(&self, table: char) -> Vec<(K, V)>;

    async fn load_compacted(&mut self, compaction: CompactionResult);
}

pub struct StateStore<S: BackingStore> {
    backend: S,
    restore_from: Option<CheckpointMetadata>,
    task_info: TaskInfo,
    table_descriptors: HashMap<char, TableDescriptor>,
    caches: HashMap<char, Box<dyn Any + Send>>,
}

pub fn hash_key<K: Hash>(key: &K) -> u64 {
    let mut hasher = DefaultHasher::new();
    key.hash(&mut hasher);
    hasher.finish()
}

impl<S: BackingStore> StateStore<S> {
    pub async fn new(
        task_info: &TaskInfo,
        tables: Vec<TableDescriptor>,
        control_tx: Sender<ControlResp>,
    ) -> Self {
        let backend = S::new(task_info, tables.clone(), control_tx).await;

        StateStore {
            backend,
            task_info: task_info.clone(),
            table_descriptors: tables
                .iter()
                .map(|table| (table.name.chars().next().unwrap(), table.clone()))
                .collect(),
            restore_from: None,
            caches: HashMap::new(),
        }
    }

    pub async fn from_checkpoint(
        task_info: &TaskInfo,
        checkpoint_metadata: CheckpointMetadata,
        tables: Vec<TableDescriptor>,
        tx: Sender<ControlResp>,
    ) -> Self {
        let backend =
            S::from_checkpoint(task_info, checkpoint_metadata.clone(), tables.clone(), tx).await;

        StateStore {
            backend,
            task_info: task_info.clone(),
            table_descriptors: tables
                .iter()
                .map(|table| (table.name.chars().next().unwrap(), table.clone()))
                .collect(),
            restore_from: Some(checkpoint_metadata),
            caches: HashMap::new(),
        }
    }

    // We now handle this in the individual tables. Don't love it, but they have different behaviors.
    pub fn handle_watermark(&mut self, _watermark: SystemTime) {}

    pub async fn get_time_key_map<K: Key, V: Data>(
        &mut self,
        table: char,
        watermark: Option<SystemTime>,
    ) -> TimeKeyMap<K, V, S> {
        // this is done because populating it is async, so can't use or_insert().
        if let std::collections::hash_map::Entry::Vacant(e) = self.caches.entry(table) {
            let cache: Box<dyn Any + Send> = match &self.restore_from {
                Some(_restore_from) => {
                    let cache = TimeKeyMapCache::<K, V>::from_checkpoint(
                        &self.backend,
                        &self.task_info,
                        table,
                        self.table_descriptors.get(&table).unwrap(),
                        watermark,
                    )
                    .await;
                    Box::new(cache)
                }
                None => Box::<time_key_map::TimeKeyMapCache<K, V>>::default(),
            };
            e.insert(cache);
        }

        let cache = self.caches.get_mut(&table).unwrap();
        let cache: &mut TimeKeyMapCache<K, V> = cache.downcast_mut().unwrap_or_else(|| {
            panic!(
                "Failed to get table {} with key {} and value {}",
                table,
                std::any::type_name::<K>(),
                std::any::type_name::<V>()
            )
        });
        TimeKeyMap::new(table, &mut self.backend, cache)
    }

    pub async fn get_key_time_multi_map<K: Key, V: Data>(
        &mut self,
        table: char,
    ) -> KeyTimeMultiMap<K, V, S> {
        // this is done because populating it is async, so can't use or_insert().
        if let std::collections::hash_map::Entry::Vacant(e) = self.caches.entry(table) {
            let cache: Box<dyn Any + Send> = match &self.restore_from {
                Some(restore_from) => {
                    let cache = KeyTimeMultiMapCache::<K, V>::from_checkpoint(
                        &self.backend,
                        &self.task_info,
                        table,
                        self.table_descriptors.get(&table).unwrap(),
                        restore_from,
                    )
                    .await;
                    Box::new(cache)
                }
                None => Box::<key_time_multi_map::KeyTimeMultiMapCache<K, V>>::default(),
            };
            e.insert(cache);
        }

        let cache = self.caches.get_mut(&table).unwrap();
        let cache: &mut KeyTimeMultiMapCache<K, V> = cache.downcast_mut().unwrap_or_else(|| {
            panic!(
                "Failed to get table {} with key {} and value {}",
                table,
                std::any::type_name::<K>(),
                std::any::type_name::<V>()
            )
        });
        KeyTimeMultiMap::new(table, &mut self.backend, cache)
    }

    pub async fn get_global_keyed_state<K: Key, V: Data>(
        &mut self,
        table: char,
    ) -> GlobalKeyedState<K, V, S> {
        // this is done because populating it is async, so can't use or_insert().
        if let std::collections::hash_map::Entry::Vacant(e) = self.caches.entry(table) {
            let cache: Box<dyn Any + Send> = match &self.restore_from {
                Some(_restore_from) => {
                    let cache =
                        GlobalKeyedStateCache::<K, V>::from_checkpoint(&self.backend, table).await;
                    Box::new(cache)
                }
                None => Box::<global_keyed_map::GlobalKeyedStateCache<K, V>>::default(),
            };
            e.insert(cache);
        }

        let cache = self.caches.get_mut(&table).unwrap();
        let cache: &mut GlobalKeyedStateCache<K, V> = cache.downcast_mut().unwrap_or_else(|| {
            panic!(
                "Failed to get table {} with key {} and value {}",
                table,
                std::any::type_name::<K>(),
                std::any::type_name::<V>()
            )
        });
        GlobalKeyedState::new(table, &mut self.backend, cache)
    }

    pub async fn get_key_state<K: Key, V: Data>(&mut self, table: char) -> KeyedState<K, V, S> {
        if let std::collections::hash_map::Entry::Vacant(e) = self.caches.entry(table) {
            let cache: Box<dyn Any + Send> = match &self.restore_from {
                Some(_restore_from) => {
                    let cache =
                        KeyedStateCache::<K, V>::from_checkpoint(&self.backend, table).await;
                    Box::new(cache)
                }
                None => Box::<keyed_map::KeyedStateCache<K, V>>::default(),
            };
            e.insert(cache);
        }

        let cache = self.caches.get_mut(&table).unwrap();
        let cache: &mut KeyedStateCache<K, V> = cache.downcast_mut().unwrap_or_else(|| {
            panic!(
                "Failed to get table {} with key {} and value {}",
                table,
                std::any::type_name::<K>(),
                std::any::type_name::<V>()
            )
        });
        KeyedState::new(table, &mut self.backend, cache)
    }

    pub async fn checkpoint(&mut self, barrier: CheckpointBarrier, watermark: Option<SystemTime>) {
        self.backend.checkpoint(barrier, watermark).await;
    }

    pub async fn load_compacted(&mut self, compaction: CompactionResult) {
        self.backend.load_compacted(compaction).await;
    }
}

#[cfg(test)]
mod test {
    use arroyo_rpc::grpc::{
        CheckpointMetadata, OperatorCheckpointMetadata, TableDeleteBehavior, TableDescriptor,
        TableWriteBehavior,
    };
    use std::env;
    use test_case::test_case;
    use tokio::sync::mpsc::Receiver;

    use arroyo_rpc::{CompactionResult, ControlResp};
    use rand::RngCore;
    use std::time::{Duration, SystemTime};
    use tokio::sync::mpsc::channel;

    use crate::parquet::ParquetBackend;
    use crate::tables::key_time_multi_map::KeyTimeMultiMap;
    use crate::tables::keyed_map::KeyedState;
    use crate::tables::time_key_map::TimeKeyMap;
    use crate::{global_table, timestamp_table, BackingStore, StateStore};
    use arroyo_types::{to_micros, CheckpointBarrier, TaskInfo};

    fn default_tables() -> Vec<TableDescriptor> {
        vec![
            global_table("g", "test"),
            timestamp_table(
                "t",
                "time",
                TableDeleteBehavior::NoReadsBeforeWatermark,
                TableWriteBehavior::NoWritesBeforeWatermark,
                Duration::ZERO,
            ),
        ]
    }

    async fn parquet_for_test() -> (StateStore<ParquetBackend>, Receiver<ControlResp>) {
        let job_id = rand::thread_rng().next_u64();
        let operator_id = rand::thread_rng().next_u64();
        let (tx, rx) = channel(10);
        (
            StateStore::<ParquetBackend>::new(
                &TaskInfo::for_test(
                    &format!("test_job_{}", job_id),
                    &format!("test_op_{}", operator_id),
                ),
                default_tables(),
                tx,
            )
            .await,
            rx,
        )
    }

    async fn parquet_for_test_from_checkpoint(
        job_id: &str,
        operator_id: &str,
        checkpoint_metadata: &CheckpointMetadata,
    ) -> (StateStore<ParquetBackend>, Receiver<ControlResp>) {
        let (tx, rx) = channel(10);
        let task_info = TaskInfo::for_test(&job_id, &operator_id);

        (
            StateStore::<ParquetBackend>::from_checkpoint(
                &task_info,
                checkpoint_metadata.clone(),
                default_tables(),
                tx,
            )
            .await,
            rx,
        )
    }

    async fn do_compaction(job_id: &str, operator_id: &str, epoch: u32) -> CompactionResult {
        env::set_var("MIN_FILES_TO_COMPACT", "2");
        let result = match ParquetBackend::compact_operator(
            1,
            job_id.to_string(),
            operator_id.to_string(),
            epoch,
        )
        .await
        {
            Ok(Some(result)) => result,
            Ok(None) => {
                panic!("no compaction result")
            }
            Err(e) => {
                panic!("compaction failed: {:?}", e)
            }
        };
        result
    }

    async fn do_checkpoint(
        ss: &mut StateStore<impl BackingStore>,
        job_id: &str,
        operator_id: &str,
        epoch: u32,
        rx: &mut Receiver<ControlResp>,
    ) -> CheckpointMetadata {
        ss.backend
            .checkpoint(
                CheckpointBarrier {
                    epoch,
                    min_epoch: 0,
                    timestamp: SystemTime::now(),
                    then_stop: false,
                },
                Some(SystemTime::UNIX_EPOCH),
            )
            .await;
        // wait until we get confirmation on the queue

        let message = match rx.recv().await {
            Some(ControlResp::CheckpointCompleted(c)) => c,
            _ => panic!("Received unexpected message on command queue"),
        };

        ParquetBackend::write_operator_checkpoint_metadata(OperatorCheckpointMetadata {
            job_id: job_id.to_string(),
            operator_id: operator_id.to_string(),
            epoch,
            start_time: to_micros(SystemTime::now()),
            finish_time: to_micros(SystemTime::now()),
            min_watermark: None,
            max_watermark: None,
            has_state: true,
            tables: default_tables(),
            backend_data: message.subtask_metadata.backend_data,
            bytes: 5,
        })
        .await;

        let checkpoint_metadata: CheckpointMetadata = CheckpointMetadata {
            job_id: job_id.to_string(),
            epoch,
            min_epoch: 1,
            start_time: 0,
            finish_time: 0,
            operator_ids: vec![operator_id.to_string()],
        };

        ParquetBackend::complete_checkpoint(checkpoint_metadata.clone()).await;

        checkpoint_metadata
    }

    #[test_case(parquet_for_test().await; "parquet store")]
    #[tokio::test]
    async fn test_global(p: (StateStore<impl BackingStore>, Receiver<ControlResp>)) {
        let (mut ss, _rx) = p;

        let mut gs = ss.get_global_keyed_state::<String, i64>('g').await;

        gs.insert("k1".into(), 1).await;

        assert_eq!(*gs.get(&"k1".into()).unwrap(), 1);

        let mut gs = ss.get_global_keyed_state::<String, i64>('g').await;
        assert_eq!(*gs.get(&"k1".into()).unwrap(), 1);

        gs.insert("k2".into(), 2).await;

        let mut entries = gs.get_all();
        entries.sort();

        assert_eq!(entries, vec![&1i64, &2]);
    }

    #[test_case(parquet_for_test().await; "parquet store")]
    #[tokio::test]
    async fn test_key_time_multi_map(p: (StateStore<impl BackingStore>, Receiver<ControlResp>)) {
        let (mut ss, mut rx) = p;
        let job_id = ss.task_info.job_id.clone();
        let operator_id = ss.task_info.operator_id.clone();
        let mut ks: KeyTimeMultiMap<String, i32, _> = ss.get_key_time_multi_map('t').await;

        let k1 = "k1";
        let t1 = SystemTime::now();
        let t2 = t1 + Duration::from_secs(1);
        let t3 = t1 + Duration::from_secs(2);
        let t4 = t1 + Duration::from_secs(3);
        let _t5 = t1 + Duration::from_secs(4);

        ks.insert(t1, k1.into(), 1).await;
        ks.insert(t1, k1.into(), 2).await;
        ks.insert(t2, k1.into(), 3).await;
        ks.insert(t3, k1.into(), 4).await;
        ks.insert(t4, k1.into(), 5).await;

        assert_eq!(
            ks.get_time_range(&mut k1.into(), t1, t1 + Duration::from_nanos(1))
                .await,
            vec![&1, &2]
        );
        assert_eq!(
            ks.get_time_range(&mut k1.into(), t1, t4).await,
            vec![&1, &2, &3, &4]
        );

        do_checkpoint(&mut ss, &job_id, &operator_id, 1, &mut rx).await;

        let mut ks = ss.get_key_time_multi_map::<String, i32>('t').await;

        assert_eq!(
            ks.get_time_range(&mut k1.into(), t1, t1 + Duration::from_nanos(1))
                .await,
            vec![&1, &2]
        );
        assert_eq!(
            ks.get_time_range(&mut k1.into(), t1, t4).await,
            vec![&1, &2, &3, &4]
        );
    }

    #[test_case(parquet_for_test().await; "parquet store")]
    #[tokio::test]
    async fn test_time_key_map(p: (StateStore<impl BackingStore>, Receiver<ControlResp>)) {
        let (mut ss, mut rx) = p;
        let job_id = ss.task_info.job_id.clone();
        let operator_id = ss.task_info.operator_id.clone();

        let mut ks: TimeKeyMap<usize, i32, _> = ss.get_time_key_map('t', None).await;

        let t1 = SystemTime::now();
        let t2 = t1 + Duration::from_secs(1);
        let t3 = t1 + Duration::from_secs(2);
        let t4 = t1 + Duration::from_secs(3);
        let _t5 = t1 + Duration::from_secs(4);

        ks.insert(t1, 1, 1);
        ks.insert(t1, 1, 2);
        ks.insert(t2, 1, 3);
        ks.insert(t3, 1, 4);
        ks.insert(t4, 1, 5);

        assert_eq!(ks.get_all_for_time(t1), vec![(&1, &2)]);
        assert_eq!(
            ks.get_all().await,
            vec![(t1, &1, &2), (t2, &1, &3), (t3, &1, &4), (t4, &1, &5)]
        );

        do_checkpoint(&mut ss, &job_id, &operator_id, 1, &mut rx).await;

        let mut ks = ss.get_time_key_map::<usize, i32>('t', None).await;

        assert_eq!(ks.get_all_for_time(t1), vec![(&1, &2)]);
        assert_eq!(
            ks.get_all().await,
            vec![(t1, &1, &2), (t2, &1, &3), (t3, &1, &4), (t4, &1, &5)]
        );
    }

    #[test_case(parquet_for_test().await; "parquet store")]
    #[tokio::test]
    async fn test_key_state_compaction(p: (StateStore<impl BackingStore>, Receiver<ControlResp>)) {
        let (mut ss, mut rx) = p;
        let job_id = ss.task_info.job_id.clone();
        let operator_id = ss.task_info.operator_id.clone();

        // insert a key/value

        let mut ks: KeyedState<usize, i32, _> = ss.get_key_state('t').await;
        let t1 = SystemTime::now();
        ks.insert(t1, 1, 1).await;
        assert_eq!(Some(&1), ks.get(&mut 1));

        // checkpoint 1

        do_checkpoint(&mut ss, &job_id, &operator_id, 1, &mut rx).await;

        // update key

        let mut ks: KeyedState<usize, i32, _> = ss.get_key_state('t').await;
        ks.insert(t1, 1, 2).await;

        // checkpoint 2

        do_checkpoint(&mut ss, &job_id, &operator_id, 2, &mut rx).await;

        // compact epoch 1 and 2 and load compacted data

        let result = do_compaction(&job_id, &operator_id, 2).await;
        assert_eq!(2, result.backend_data_to_drop.len());
        assert_eq!(1, result.backend_data_to_load.len());
        ss.load_compacted(result).await;

        // update key again

        let mut ks: KeyedState<usize, i32, _> = ss.get_key_state('t').await;
        ks.insert(t1, 1, 3).await;

        // checkpoint 3

        do_checkpoint(&mut ss, &job_id, &operator_id, 3, &mut rx).await;

        // delete key

        let mut ks: KeyedState<usize, i32, _> = ss.get_key_state('t').await;
        ks.remove(&mut 1).await;

        // checkpoint 4

        do_checkpoint(&mut ss, &job_id, &operator_id, 4, &mut rx).await;

        // compact epoch 3 and 4

        let result = do_compaction(&job_id, &operator_id, 4).await;

        assert_eq!(2, result.backend_data_to_drop.len());
        assert_eq!(1, result.backend_data_to_load.len());
        ss.load_compacted(result).await;

        // checkpoint 5 (contains both previous compactions)

        let checkpoint5 = do_checkpoint(&mut ss, &job_id, &operator_id, 5, &mut rx).await;

        // restore from epoch 5

        let (mut restored, _) =
            parquet_for_test_from_checkpoint(&job_id, &operator_id, &checkpoint5).await;

        // check that the key is gone

        let ks: KeyedState<usize, i32, _> = restored.get_key_state('t').await;
        assert_eq!(None, ks.get(&mut 1));
    }
}
