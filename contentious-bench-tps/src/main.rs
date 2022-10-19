use {
    crate::{accounts::Accounts, client::Client},
    clap::{crate_name, Parser},
    cli::Config,
    log::*,
    rand::Rng,
    solana_measure::measure,
    solana_sdk::{
        compute_budget::ComputeBudgetInstruction, hash::Hash, message::Message, pubkey::Pubkey,
        signature::Keypair, signer::Signer, system_instruction, transaction::Transaction,
        transport::TransportError,
    },
    std::{
        ops::RangeInclusive,
        sync::{
            atomic::{AtomicBool, AtomicUsize, Ordering},
            Arc,
        },
        time::{Duration, Instant},
    },
};

mod accounts;
mod cli;
mod client;

// TODO: Make these configurable
const JSON_RPC_URL: &str = "http://localhost:8899";
const WEBSOCKET_URL: &str = "";

fn main() {
    solana_logger::setup_with_default("solana=info");
    solana_metrics::set_panic_hook("contentious-bench-tps", /*version:*/ None);

    let config = Config::parse();
    info!("running {} with configuration: {:?}", crate_name!(), config);
    let config = Arc::new(config);

    info!("generating accounts...");
    let accounts = Arc::new(Accounts::new(
        config.num_contentious_transfer_accounts,
        config.num_regular_transfer_accounts,
    ));

    info!("creating client...");
    let client = Arc::new(Client::new(JSON_RPC_URL.to_string(), WEBSOCKET_URL));

    info!("funding accounts...");
    client.fund_accounts(&accounts);
    check_funds(&client, &accounts);

    info!("beginning benchmark...");
    benchmark(&config, client, accounts);
}

fn check_funds(client: &Client, accounts: &Accounts) {
    let mut num_failure = 0;
    let mut num_success = 0;
    for account in accounts
        .contentious_accounts
        .iter()
        .chain(accounts.regular_accounts.iter())
    {
        match client.get_balance(&account.pubkey()) {
            Ok(balance) => {
                if balance < 1_000_000_000 {
                    error!(
                        "account {} has insufficient balance: {}",
                        account.pubkey(),
                        balance
                    );
                    num_failure += 1;
                } else {
                    num_success += 1;
                }
            }
            Err(err) => {
                error!(
                    "failed to get balance for account {}: {}",
                    account.pubkey(),
                    err
                );
                num_failure += 1;
            }
        }
    }
    error!("num_success={num_success}");
    assert!(num_failure == 0);

    // let contentious_funds = client.get_funds(&contentious_accounts);
    // let regular_funds = client.get_funds(&regular_accounts);

    // assert_eq!(
    //     contentious_funds.len(),
    //     contentious_accounts.len(),
    //     "not all contentious accounts have funds"
    // );
    // assert_eq!(
    //     regular_funds.len(),
    //     regular_accounts.len(),
    //     "not all regular accounts have funds"
    // );
}

fn benchmark(config: &Arc<Config>, client: Arc<Client>, accounts: Arc<Accounts>) {
    let num_contentious_transactions_sent = Arc::new(AtomicUsize::new(0));
    let num_regular_transactions_sent = Arc::new(AtomicUsize::new(0));
    let exit = Arc::new(AtomicBool::new(false));
    let ready_count = Arc::new(AtomicUsize::new(0));
    let thread_count =
        config.num_contentious_transfer_threads + config.num_regular_transfer_threads;

    let contentious_threads = (0..config.num_contentious_transfer_threads)
        .map(|idx| {
            let config = config.clone();
            let client = client.clone();
            let accounts = accounts.clone();
            let num_transactions_sent = num_contentious_transactions_sent.clone();
            let exit = exit.clone();
            let ready_count = ready_count.clone();
            std::thread::Builder::new()
                .name(format!("conSnd-{idx}"))
                .spawn(move || {
                    let _ = sender_loop(
                        idx,
                        client,
                        &accounts.contentious_accounts,
                        &accounts.regular_accounts, // transfer to regular accounts so we don't have many repeated transactions
                        &config.contentious_transfer_fee_range,
                        &num_transactions_sent,
                        &exit,
                        &ready_count,
                        thread_count,
                        16384,
                    );
                    exit.store(true, Ordering::Relaxed);
                })
                .unwrap()
        })
        .collect::<Vec<_>>();

    let regular_threads = (0..config.num_regular_transfer_threads)
        .map(|idx| {
            let config = config.clone();
            let client = client.clone();
            let accounts = accounts.clone();
            let num_transactions_sent = num_regular_transactions_sent.clone();
            let exit = exit.clone();
            let ready_count = ready_count.clone();
            std::thread::Builder::new()
                .name(format!("regSnd-{idx}"))
                .spawn(move || {
                    let partition = accounts.regular_accounts.len() / 2;
                    let _ = sender_loop(
                        idx + config.num_contentious_transfer_threads, // offset the thread index so we can distinguish contentious and regular threads in the logs
                        client,
                        &accounts.regular_accounts[..partition],
                        &accounts.regular_accounts[partition..],
                        &config.regular_transfer_fee_range,
                        &num_transactions_sent,
                        &exit,
                        &ready_count,
                        thread_count,
                        1024, // chunk is 1k
                    );
                    exit.store(true, Ordering::Relaxed);
                })
                .unwrap()
        })
        .collect::<Vec<_>>();

    // wait a bit to see how many transactions were confirmed for the funding
    let start = Instant::now();
    while start.elapsed() < config.duration {
        let num_contentious_transactions_sent =
            num_contentious_transactions_sent.load(Ordering::Relaxed);
        let num_regular_transactions_sent = num_regular_transactions_sent.load(Ordering::Relaxed);
        let num_transactions_confirmed = client.get_num_transactions();
        info!(
            "num_contentious_transactions_sent={num_contentious_transactions_sent} regular_transactions_sent={num_regular_transactions_sent} transactions_confirmed={num_transactions_confirmed}"
        );
        std::thread::sleep(Duration::from_millis(100));
    }

    let start = Instant::now();
    while start.elapsed() < config.duration {
        let num_contentious_transactions_sent =
            num_contentious_transactions_sent.load(Ordering::Relaxed);
        let num_regular_transactions_sent = num_regular_transactions_sent.load(Ordering::Relaxed);
        let num_transactions_confirmed = client.get_num_transactions();
        info!(
            "num_contentious_transactions_sent={num_contentious_transactions_sent} regular_transactions_sent={num_regular_transactions_sent} transactions_confirmed={num_transactions_confirmed}"
        );
        std::thread::sleep(Duration::from_millis(100));
    }
    exit.store(true, Ordering::Relaxed);

    contentious_threads
        .into_iter()
        .chain(regular_threads.into_iter())
        .for_each(|thread| thread.join().unwrap());
}

fn sender_loop(
    thread_idx: usize,
    client: Arc<Client>,
    from_accounts: &[Keypair],
    to_accounts: &[Keypair],
    fee_range: &Option<RangeInclusive<u64>>,
    num_transactions_sent: &AtomicUsize,
    exit: &AtomicBool,
    ready_count: &AtomicUsize,
    thread_count: usize,
    chunk_size: usize,
) -> Result<(), TransportError> {
    let mut rng = rand::thread_rng();

    while !exit.load(Ordering::Relaxed) {
        let (unsigned_txs, signers) =
            generate_transactions(from_accounts, to_accounts, fee_range, chunk_size, &mut rng);

        let (recent_blockhash, get_blockhash_time) = measure!(client.get_recent_blockhash());
        let (txs, sign_time) =
            measure!(sign_transactions(&unsigned_txs, &signers, recent_blockhash));
        ready_count.fetch_add(1, Ordering::Relaxed);
        while ready_count.load(Ordering::Relaxed) % thread_count == 0 {}

        let (_, send_time) = measure!(client.send_transactions(&txs).unwrap());

        num_transactions_sent.fetch_add(txs.len(), Ordering::Relaxed);
    }

    Ok(())
}

fn generate_transactions(
    from_accounts: &[Keypair],
    to_accounts: &[Keypair],
    fee_range: &Option<RangeInclusive<u64>>,
    num_txs: usize,
    rng: &mut impl Rng,
) -> (Vec<Message>, Vec<Keypair>) {
    let mut messages = Vec::with_capacity(num_txs);
    let mut signers = Vec::with_capacity(num_txs);
    (0..num_txs)
        .map(|_| generate_transaction(from_accounts, to_accounts, fee_range, rng))
        .for_each(|(message, signer)| {
            messages.push(message);
            signers.push(signer.insecure_clone());
        });
    (messages, signers)
}

fn sign_transactions<'a>(
    messages: &[Message],
    signers: &[Keypair],
    recent_blockhash: Hash,
) -> Vec<Transaction> {
    messages
        .iter()
        .zip(signers.iter())
        .map(|(message, signer)| Transaction::new(&[signer], message.clone(), recent_blockhash))
        .collect()
}

/// Generate a transfer transaction with an additional compute-unit fee for prioritization
fn generate_transaction(
    from_accounts: &[Keypair],
    to_accounts: &[Keypair],
    fee_range: &Option<RangeInclusive<u64>>,
    rng: &mut impl Rng,
) -> (Message, Keypair) {
    let (from_keypair, to_pubkey) = generate_transfer_accounts(from_accounts, to_accounts, rng);
    let mut instructions = vec![system_instruction::transfer(
        &from_keypair.pubkey(),
        &to_pubkey,
        1,
    )];
    if let Some(fee_range) = fee_range {
        let compute_unit_price = generate_compute_unit_price(fee_range, rng);
        instructions.extend_from_slice(&[
            ComputeBudgetInstruction::set_compute_unit_limit(200000),
            ComputeBudgetInstruction::set_compute_unit_price(compute_unit_price),
        ]);
    }

    let message = Message::new(&instructions, Some(&from_keypair.pubkey()));
    (message, from_keypair.insecure_clone())
}

/// Generate a random keypair to transfer from and pubkey to
fn generate_transfer_accounts<'a>(
    from_accounts: &'a [Keypair],
    to_accounts: &'a [Keypair],
    rng: &mut impl Rng,
) -> (&'a Keypair, Pubkey) {
    let from = rng.gen_range(0..from_accounts.len());
    let to = rng.gen_range(0..to_accounts.len());

    let from_keypair = &from_accounts[from];
    let to_pubkey = (&to_accounts[to]).pubkey();

    (from_keypair, to_pubkey)
}

/// Generate a random fee in the given range
fn generate_compute_unit_price(fee_range: &RangeInclusive<u64>, rng: &mut impl Rng) -> u64 {
    rng.gen_range(*fee_range.start()..=*fee_range.end())
}
