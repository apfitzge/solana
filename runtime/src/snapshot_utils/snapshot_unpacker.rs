//! Provides interfaces for unpacking snapshots

use {
    super::{untar_snapshot_create_shared_buffer, ArchiveFormat},
    crate::{
        hardened_unpack::{streaming_unpack_snapshot, ParallelSelector},
        shared_buffer_reader::SharedBufferReader,
    },
    crossbeam_channel::{Receiver, Sender},
    rayon::{ThreadPool, ThreadPoolBuilder},
    std::{path::PathBuf, sync::Arc},
    tar::Archive,
};

/// Stores state for unpacking snapshot
pub struct SnapshotUnpacker {
    /// Path to the snapshot archive
    snapshot_archive_path: PathBuf,
    /// Format of the snapshot archive
    archive_format: ArchiveFormat,
    /// Paths to unpack snapshot storage files into
    account_paths: Vec<PathBuf>,
    /// ledger_dir
    ledger_dir: PathBuf,
    /// Number of threads to use during unpack
    num_threads: usize,
    /// Sender for sending unpacked files
    file_sender: Sender<PathBuf>,
}

impl SnapshotUnpacker {
    /// Asynchronously spawns threads to unpack snapshot in parallel.
    /// Returns a receiver that will receive unpacked files
    pub fn spawn_unpack_snapshot(
        account_paths: Vec<PathBuf>,
        ledger_dir: PathBuf,
        snapshot_archive_path: PathBuf,
        archive_format: ArchiveFormat,
        num_threads: usize,
    ) -> Receiver<PathBuf> {
        let (file_sender, file_receiver) = crossbeam_channel::unbounded();

        let unpacker = Self {
            snapshot_archive_path,
            archive_format,
            account_paths,
            ledger_dir,
            num_threads,
            file_sender,
        };
        unpacker.spawn_readers();

        file_receiver
    }

    /// Spawn the reader threads to unpack the snapshot
    /// Consumes the SnapshotUnpacker
    fn spawn_readers(self) {
        let readers = self.build_shared_readers();
        let thread_pool = self.build_thread_pool();

        let unpacker = Arc::new(self);
        for worker in readers.into_iter().enumerate().map(|(index, reader)| {
            let parallel_selector = Some(ParallelSelector {
                index,
                divisions: unpacker.num_threads,
            });
            SnapshotUnpackerWorker::new(unpacker.clone(), reader, parallel_selector)
        }) {
            thread_pool.spawn(move || {
                worker.unpack();
            });
        }
    }

    /// Builds reader for each thread
    fn build_shared_readers(&self) -> Vec<SharedBufferReader> {
        let shared_buffer =
            untar_snapshot_create_shared_buffer(&self.snapshot_archive_path, self.archive_format)
                .unwrap();
        (0..self.num_threads)
            .into_iter()
            .map(|_| SharedBufferReader::new(&shared_buffer))
            .collect()
    }

    /// Builds thread pool to unpack with
    fn build_thread_pool(&self) -> ThreadPool {
        ThreadPoolBuilder::default()
            .num_threads(self.num_threads)
            .build()
            .unwrap()
    }
}

/// Stores state for a thread unpacking snapshot
struct SnapshotUnpackerWorker {
    /// Wrapper for the global SnapshotUnpacker
    unpacker: Arc<SnapshotUnpacker>,
    /// Thread-local archive reader
    archive: Archive<SharedBufferReader>,
    /// Selector for selecting work
    selector: Option<ParallelSelector>,
}

impl SnapshotUnpackerWorker {
    /// Builds a thread-local worker
    fn new(
        unpacker: Arc<SnapshotUnpacker>,
        reader: SharedBufferReader,
        selector: Option<ParallelSelector>,
    ) -> Self {
        let archive = Archive::new(reader);
        Self {
            unpacker,
            archive,
            selector,
        }
    }

    /// Do unpacking work for this worker thread
    fn unpack(mut self) {
        streaming_unpack_snapshot(
            &mut self.archive,
            &self.unpacker.ledger_dir,
            &self.unpacker.account_paths,
            self.selector,
            &self.unpacker.file_sender,
        );
    }
}
