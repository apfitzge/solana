use {
    clap::Parser,
    crossbeam_channel::{select, Receiver, Sender},
    log::info,
    rand::Rng,
    solana_core::transaction_scheduler::{TransactionPriority, TransactionScheduler},
    solana_measure::measure,
    solana_perf::packet::{Packet, PacketBatch},
    solana_runtime::bank::Bank,
    solana_sdk::{
        compute_budget::ComputeBudgetInstruction,
        hash::Hash,
        instruction::{AccountMeta, Instruction},
        signature::Keypair,
        signer::Signer,
        system_program,
        transaction::{Transaction, VersionedTransaction},
    },
    std::{
        sync::{
            atomic::{AtomicBool, AtomicUsize, Ordering},
            Arc,
        },
        thread::{sleep, JoinHandle},
        time::{Duration, Instant},
    },
};

#[derive(Parser, Debug)]
#[clap(author, version, about, long_about = None)]
struct Args {
    /// How many packets per second to send to the scheduler
    #[clap(long, env, default_value_t = 200_000)]
    packet_send_rate: usize,

    /// Number of packets per batch
    #[clap(long, env, default_value_t = 128)]
    packets_per_batch: usize,

    /// Number of batches per message
    #[clap(long, env, default_value_t = 4)]
    batches_per_msg: usize,

    /// Number of consuming threads (number of threads requesting batches from scheduler)
    #[clap(long, env, default_value_t = 20)]
    num_execution_threads: usize,

    /// How long each transaction takes to execution in microseconds
    #[clap(long, env, default_value_t = 15)]
    execution_per_tx_us: u64,

    /// Duration of benchmark
    #[clap(long, env, default_value_t = 20.0)]
    duration: f32,

    /// Number of accounts to choose from when signing transactions
    #[clap(long, env, default_value_t = 100000)]
    num_accounts: usize,

    /// Number of read locks per tx
    #[clap(long, env, default_value_t = 4)]
    num_read_locks_per_tx: usize,

    /// Number of write locks per tx
    #[clap(long, env, default_value_t = 2)]
    num_read_write_locks_per_tx: usize,

    /// Max batch size for scheduler
    #[clap(long, env, default_value_t = 128)]
    max_batch_size: usize,

    /// Do execution threads only send back after the full batch is complete
    #[clap(long, env, default_value_t = true)]
    banking_stage_only_alert_full_batch: bool,
}

#[derive(Debug, Default)]
struct TransactionSchedulerMetrics {
    /// Number of batches sent to the scheduler
    num_batches_sent: AtomicUsize,
    /// Number of transactions sent to the scheduler
    num_transactions_sent: AtomicUsize,
    /// Number of transaction batches scheduled
    num_batches_scheduled: AtomicUsize,
    /// Number of transactions scheduled
    num_transactions_scheduled: AtomicUsize,
    /// Number of transactions completed
    num_transactions_completed: AtomicUsize,
}

impl TransactionSchedulerMetrics {
    fn report(&self) {
        let num_batches_sent = self.num_batches_sent.load(Ordering::Relaxed);
        let num_transactions_sent = self.num_transactions_sent.load(Ordering::Relaxed);
        let num_batches_scheduled = self.num_batches_scheduled.load(Ordering::Relaxed);
        let num_transactions_scheduled = self.num_transactions_scheduled.load(Ordering::Relaxed);
        let num_transactions_completed = self.num_transactions_completed.load(Ordering::Relaxed);

        let num_transactions_pending = num_transactions_sent - num_transactions_scheduled;
        info!("num_transactions_sent: {num_transactions_sent} num_transactions_pending: {num_transactions_pending} num_transactions_scheduled: {num_transactions_scheduled} num_transactions_completed: {num_transactions_completed}");

        // info!("num_batches_sent: {num_batches_sent} num_transactions_sent: {num_transactions_sent} num_batches_scheduled: {num_batches_scheduled} num_transactions_scheduled: {num_transactions_scheduled} num_transactions_completed: {num_transactions_completed}");
    }
}

fn main() {
    solana_logger::setup_with_default("INFO");

    let Args {
        packet_send_rate,
        packets_per_batch,
        batches_per_msg,
        num_execution_threads,
        execution_per_tx_us,
        duration,
        num_accounts,
        num_read_locks_per_tx,
        num_read_write_locks_per_tx,
        max_batch_size,
        banking_stage_only_alert_full_batch,
    } = Args::parse();

    let (packet_batch_sender, packet_batch_receiver) = crossbeam_channel::unbounded();
    let (transaction_batch_senders, transaction_batch_receivers) =
        build_channels(num_execution_threads);
    let (completed_transaction_sender, completed_transaction_receiver) =
        crossbeam_channel::unbounded();
    let bank = Arc::new(Bank::default_for_tests());
    let exit = Arc::new(AtomicBool::new(false));

    // Spawns and runs the scheduler thread
    let scheduler_handle = TransactionScheduler::spawn_scheduler(
        packet_batch_receiver,
        transaction_batch_senders,
        completed_transaction_receiver,
        bank,
        max_batch_size,
        exit.clone(),
    );

    let metrics = Arc::new(TransactionSchedulerMetrics::default());

    // Spawn the execution threads (sleep on transactions and then send completed batches back)
    let execution_handles = start_execution_threads(
        metrics.clone(),
        transaction_batch_receivers,
        completed_transaction_sender,
        execution_per_tx_us,
        banking_stage_only_alert_full_batch,
        exit.clone(),
    );

    // Spawn thread to create and send packet batches
    info!("building accounts...");
    let accounts = Arc::new(build_accounts(num_accounts));
    info!("built accounts...");
    let packet_sender_handles = spawn_packet_senders(
        metrics.clone(),
        accounts,
        packet_batch_sender,
        packets_per_batch,
        batches_per_msg,
        packet_send_rate,
        num_read_locks_per_tx,
        num_read_write_locks_per_tx,
        exit.clone(),
    );

    // Spawn thread for reporting metrics
    std::thread::spawn({
        let exit = exit.clone();
        move || loop {
            if exit.load(Ordering::Relaxed) {
                break;
            }
            metrics.report();
            std::thread::sleep(Duration::from_millis(100));
        }
    });

    std::thread::sleep(Duration::from_secs_f32(duration));
    exit.store(true, Ordering::Relaxed);

    scheduler_handle.join().unwrap();
    execution_handles
        .into_iter()
        .for_each(|jh| jh.join().unwrap());
    packet_sender_handles
        .into_iter()
        .for_each(|jh| jh.join().unwrap());
}

fn start_execution_threads(
    metrics: Arc<TransactionSchedulerMetrics>,
    transaction_batch_receivers: Vec<Receiver<Vec<Arc<TransactionPriority>>>>,
    completed_transaction_sender: Sender<Arc<TransactionPriority>>,
    execution_per_tx_us: u64,
    banking_stage_only_alert_full_batch: bool,
    exit: Arc<AtomicBool>,
) -> Vec<JoinHandle<()>> {
    transaction_batch_receivers
        .into_iter()
        .map(|transaction_batch_receiver| {
            start_execution_thread(
                metrics.clone(),
                transaction_batch_receiver,
                completed_transaction_sender.clone(),
                execution_per_tx_us,
                banking_stage_only_alert_full_batch,
                exit.clone(),
            )
        })
        .collect()
}

fn start_execution_thread(
    metrics: Arc<TransactionSchedulerMetrics>,
    transaction_batch_receiver: Receiver<Vec<Arc<TransactionPriority>>>,
    completed_transaction_sender: Sender<Arc<TransactionPriority>>,
    execution_per_tx_us: u64,
    banking_stage_only_alert_full_batch: bool,
    exit: Arc<AtomicBool>,
) -> JoinHandle<()> {
    std::thread::spawn(move || {
        execution_worker(
            metrics,
            transaction_batch_receiver,
            completed_transaction_sender,
            execution_per_tx_us,
            banking_stage_only_alert_full_batch,
            exit,
        )
    })
}

fn execution_worker(
    metrics: Arc<TransactionSchedulerMetrics>,
    transaction_batch_receiver: Receiver<Vec<Arc<TransactionPriority>>>,
    completed_transaction_sender: Sender<Arc<TransactionPriority>>,
    execution_per_tx_us: u64,
    banking_stage_only_alert_full_batch: bool,
    exit: Arc<AtomicBool>,
) {
    loop {
        if exit.load(Ordering::Relaxed) {
            break;
        }

        select! {
            recv(transaction_batch_receiver) -> maybe_tx_batch => {
                if let Ok(tx_batch) = maybe_tx_batch {
                    handle_transaction_batch(&metrics, &completed_transaction_sender, tx_batch, execution_per_tx_us, banking_stage_only_alert_full_batch);
                }
            }
            default(Duration::from_millis(100)) => {}
        }
    }
}

fn handle_transaction_batch(
    metrics: &TransactionSchedulerMetrics,
    completed_transaction_sender: &Sender<Arc<TransactionPriority>>,
    transaction_batch: Vec<Arc<TransactionPriority>>,
    execution_per_tx_us: u64,
    banking_stage_only_alert_full_batch: bool,
) {
    let num_transactions = transaction_batch.len() as u64;
    metrics
        .num_batches_scheduled
        .fetch_add(1, Ordering::Relaxed);
    metrics
        .num_transactions_scheduled
        .fetch_add(num_transactions as usize, Ordering::Relaxed);

    if banking_stage_only_alert_full_batch {
        // Sleep through executing the batch
        sleep(Duration::from_micros(
            num_transactions * execution_per_tx_us,
        ));
    }

    // Send transaction complete messages
    for tx in transaction_batch {
        if !banking_stage_only_alert_full_batch {
            sleep(Duration::from_micros(execution_per_tx_us));
        }

        let _ = completed_transaction_sender.send(tx);
        metrics
            .num_transactions_completed
            .fetch_add(1, Ordering::Relaxed);
    }
}

const NUM_SENDERS: usize = 1;

fn spawn_packet_senders(
    metrics: Arc<TransactionSchedulerMetrics>,
    accounts: Arc<Vec<Keypair>>,
    packet_batch_sender: Sender<Vec<PacketBatch>>,
    packets_per_batch: usize,
    batches_per_msg: usize,
    packet_send_rate: usize,
    num_read_locks_per_tx: usize,
    num_write_locks_per_tx: usize,
    exit: Arc<AtomicBool>,
) -> Vec<JoinHandle<()>> {
    (0..NUM_SENDERS)
        .map(|_| {
            spawn_packet_sender(
                metrics.clone(),
                accounts.clone(),
                packet_batch_sender.clone(),
                packets_per_batch,
                batches_per_msg,
                packet_send_rate,
                num_read_locks_per_tx,
                num_write_locks_per_tx,
                exit.clone(),
            )
        })
        .collect()
}

fn spawn_packet_sender(
    metrics: Arc<TransactionSchedulerMetrics>,
    accounts: Arc<Vec<Keypair>>,
    packet_batch_sender: Sender<Vec<PacketBatch>>,
    packets_per_batch: usize,
    batches_per_msg: usize,
    packet_send_rate: usize,
    num_read_locks_per_tx: usize,
    num_write_locks_per_tx: usize,
    exit: Arc<AtomicBool>,
) -> JoinHandle<()> {
    std::thread::spawn(move || {
        send_packets(
            metrics,
            accounts,
            packet_batch_sender,
            packets_per_batch,
            batches_per_msg,
            packet_send_rate,
            num_read_locks_per_tx,
            num_write_locks_per_tx,
            exit,
        );
    })
}

fn send_packets(
    metrics: Arc<TransactionSchedulerMetrics>,
    accounts: Arc<Vec<Keypair>>,
    packet_batch_sender: Sender<Vec<PacketBatch>>,
    packets_per_batch: usize,
    batches_per_msg: usize,
    packet_send_rate: usize,
    num_read_locks_per_tx: usize,
    num_write_locks_per_tx: usize,
    exit: Arc<AtomicBool>,
) {
    let packets_per_msg = packets_per_batch * batches_per_msg;
    let loop_frequency = packet_send_rate as f64 * packets_per_msg as f64 / NUM_SENDERS as f64;
    let loop_duration = Duration::from_secs_f64(1.0 / loop_frequency);

    info!("sending packets: packets_per_msg: {packets_per_msg} loop_frequency: {loop_frequency} loop_duration: {loop_duration:?}");

    let blockhash = Hash::default();

    loop {
        if exit.load(Ordering::Relaxed) {
            break;
        }
        let (packet_batches, packet_build_time) = measure!(
            build_packet_batches(
                batches_per_msg,
                packets_per_batch,
                &accounts,
                &blockhash,
                num_read_locks_per_tx,
                num_write_locks_per_tx,
            ),
            "build packets"
        );
        metrics
            .num_batches_sent
            .fetch_add(packet_batches.len(), Ordering::Relaxed);
        metrics.num_transactions_sent.fetch_add(
            packet_batches.iter().map(|pb| pb.len()).sum(),
            Ordering::Relaxed,
        );
        let _ = packet_batch_sender.send(packet_batches);

        std::thread::sleep(loop_duration.saturating_sub(packet_build_time.as_duration()));
    }
}

fn build_packet_batches(
    batches_per_msg: usize,
    packets_per_batch: usize,
    accounts: &Vec<Keypair>,
    blockhash: &Hash,
    num_read_locks_per_tx: usize,
    num_write_locks_per_tx: usize,
) -> Vec<PacketBatch> {
    (0..batches_per_msg)
        .map(|_| {
            build_packet_batch(
                packets_per_batch,
                accounts,
                blockhash,
                num_read_locks_per_tx,
                num_write_locks_per_tx,
            )
        })
        .collect()
}

fn build_packet_batch(
    packets_per_batch: usize,
    accounts: &Vec<Keypair>,
    blockhash: &Hash,
    num_read_locks_per_tx: usize,
    num_write_locks_per_tx: usize,
) -> PacketBatch {
    PacketBatch::new(
        (0..packets_per_batch)
            .map(|_| {
                build_packet(
                    accounts,
                    blockhash,
                    num_read_locks_per_tx,
                    num_write_locks_per_tx,
                )
            })
            .collect(),
    )
}

fn build_packet(
    accounts: &Vec<Keypair>,
    blockhash: &Hash,
    num_read_locks_per_tx: usize,
    num_write_locks_per_tx: usize,
) -> Packet {
    let get_random_account = || &accounts[rand::thread_rng().gen_range(0..accounts.len())];
    let sending_keypair = get_random_account();

    let read_account_metas = (0..num_read_locks_per_tx)
        .map(|_| AccountMeta::new_readonly(get_random_account().pubkey(), false));
    let write_account_metas =
        (0..num_write_locks_per_tx).map(|_| AccountMeta::new(get_random_account().pubkey(), false));
    let ixs = vec![
        ComputeBudgetInstruction::set_compute_unit_price(rand::thread_rng().gen_range(50..500)),
        Instruction::new_with_bytes(
            system_program::id(),
            &[0],
            read_account_metas.chain(write_account_metas).collect(),
        ),
    ];
    let versioned_transaction = VersionedTransaction::from(Transaction::new_signed_with_payer(
        &ixs,
        Some(&sending_keypair.pubkey()),
        &[sending_keypair],
        blockhash.clone(),
    ));
    Packet::from_data(None, &versioned_transaction).unwrap()
}

fn build_accounts(num_accounts: usize) -> Vec<Keypair> {
    (0..num_accounts).map(|_| Keypair::new()).collect()
}

fn build_channels<T>(num_execution_threads: usize) -> (Vec<Sender<T>>, Vec<Receiver<T>>) {
    let mut senders = Vec::with_capacity(num_execution_threads);
    let mut receivers = Vec::with_capacity(num_execution_threads);
    for _ in 0..num_execution_threads {
        let (sender, receiver) = crossbeam_channel::unbounded();
        senders.push(sender);
        receivers.push(receiver);
    }
    (senders, receivers)
}
