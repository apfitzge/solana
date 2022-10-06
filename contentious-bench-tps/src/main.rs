use {
    crate::{accounts::Accounts, client::Client},
    clap::{crate_name, Parser},
    cli::Config,
    log::*,
    rand::Rng,
    solana_sdk::{
        hash::Hash, signature::Keypair, signer::Signer, system_transaction,
        transaction::Transaction, transport::TransportError,
    },
    std::{
        sync::{
            atomic::{AtomicBool, Ordering},
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

    let Config {
        entrypoint,
        num_contentious_transfer_accounts,
        num_regular_transfer_accounts,
    } = config;

    info!("generating accounts...");
    let accounts = Arc::new(Accounts::new(
        num_contentious_transfer_accounts,
        num_regular_transfer_accounts,
    ));

    info!("creating client...");
    let client = Arc::new(Client::new(JSON_RPC_URL.to_string(), WEBSOCKET_URL));

    info!("funding accounts...");
    client.fund_accounts(&accounts);

    info!("beginning benchmark...");
    benchmark(client, accounts);
}

fn benchmark(client: Arc<Client>, accounts: Arc<Accounts>) {
    const NUM_CONTENTIOUS_THREADS: usize = 1;
    const NUM_REGULAR_THREADS: usize = 4;
    const BENCHMARK_DURATION: Duration = Duration::from_secs(10);

    let exit = Arc::new(AtomicBool::new(false));

    let contentious_threads = (0..NUM_CONTENTIOUS_THREADS)
        .map(|idx| {
            let client = client.clone();
            let accounts = accounts.clone();
            let exit = exit.clone();
            std::thread::Builder::new()
                .name(format!("conSnd-{idx}"))
                .spawn(move || {
                    let _ = sender_loop(client, &accounts.contentious_accounts, &exit);
                    exit.store(true, Ordering::Relaxed);
                })
                .unwrap()
        })
        .collect::<Vec<_>>();

    let regular_threads = (0..NUM_REGULAR_THREADS)
        .map(|idx| {
            let client = client.clone();
            let accounts = accounts.clone();
            let exit = exit.clone();
            std::thread::Builder::new()
                .name(format!("regSnd-{idx}"))
                .spawn(move || {
                    let _ = sender_loop(client, &accounts.regular_accounts, &exit);
                    exit.store(true, Ordering::Relaxed);
                })
                .unwrap()
        })
        .collect::<Vec<_>>();
    let start = Instant::now();
    while start.elapsed() < BENCHMARK_DURATION {
        let num_transactions = client.get_num_transactions();
        info!("transactions: {num_transactions}");
        std::thread::sleep(Duration::from_millis(100));
    }
    exit.store(true, Ordering::Relaxed);

    contentious_threads
        .into_iter()
        .chain(regular_threads.into_iter())
        .for_each(|thread| thread.join().unwrap());
}

fn sender_loop(
    client: Arc<Client>,
    accounts: &[Keypair],
    exit: &AtomicBool,
) -> Result<(), TransportError> {
    const TRANSACTION_CHUNK_SIZE: usize = 4;

    let mut rng = rand::thread_rng();

    while !exit.load(Ordering::Relaxed) {
        let recent_blockhash = client.get_recent_blockhash();
        let txs = generate_transactions(
            accounts,
            TRANSACTION_CHUNK_SIZE,
            &mut rng,
            &recent_blockhash,
        );
        client.send_transactions(&txs)?;
    }

    Ok(())
}

fn generate_transactions(
    accounts: &[Keypair],
    num_txs: usize,
    rng: &mut impl Rng,
    recent_blockhash: &Hash,
) -> Vec<Transaction> {
    (0..num_txs)
        .map(|_| {
            let from = rng.gen_range(0..accounts.len());
            let to = loop {
                let x = rng.gen_range(0..accounts.len());
                if x != from {
                    break x;
                }
            };

            let from_keypair = &accounts[from];
            let to_pubkey = &accounts[to].pubkey();

            system_transaction::transfer(from_keypair, to_pubkey, 1, *recent_blockhash)
        })
        .collect()
}
