//! The `fetch_stage` batches input from a UDP socket and sends it to a channel.

use {
    crossbeam_channel::unbounded,
    solana_perf::{packet::PacketBatchRecycler, recycler::Recycler},
    solana_streamer::streamer::{
        self, PacketBatchReceiver, PacketBatchSender, StreamerReceiveStats,
    },
    solana_tpu_client::tpu_client::DEFAULT_TPU_ENABLE_UDP,
    std::{
        net::UdpSocket,
        sync::{
            atomic::{AtomicBool, Ordering},
            Arc,
        },
        thread::{self, sleep, Builder, JoinHandle},
        time::Duration,
    },
};

pub struct FetchStage {
    thread_hdls: Vec<JoinHandle<()>>,
}

impl FetchStage {
    pub fn new(
        sockets: Vec<UdpSocket>,
        tpu_vote_sockets: Vec<UdpSocket>,
        exit: &Arc<AtomicBool>,
        coalesce: Duration,
    ) -> (Self, PacketBatchReceiver, PacketBatchReceiver) {
        let (sender, receiver) = unbounded();
        let (vote_sender, vote_receiver) = unbounded();
        (
            Self::new_with_sender(
                sockets,
                tpu_vote_sockets,
                exit,
                &sender,
                &vote_sender,
                coalesce,
                None,
                DEFAULT_TPU_ENABLE_UDP,
            ),
            receiver,
            vote_receiver,
        )
    }

    #[allow(clippy::too_many_arguments)]
    pub fn new_with_sender(
        sockets: Vec<UdpSocket>,
        tpu_vote_sockets: Vec<UdpSocket>,
        exit: &Arc<AtomicBool>,
        sender: &PacketBatchSender,
        vote_sender: &PacketBatchSender,
        coalesce: Duration,
        in_vote_only_mode: Option<Arc<AtomicBool>>,
        tpu_enable_udp: bool,
    ) -> Self {
        let tx_sockets = sockets.into_iter().map(Arc::new).collect();
        let tpu_vote_sockets = tpu_vote_sockets.into_iter().map(Arc::new).collect();
        Self::new_multi_socket(
            tx_sockets,
            tpu_vote_sockets,
            exit,
            sender,
            vote_sender,
            coalesce,
            in_vote_only_mode,
            tpu_enable_udp,
        )
    }

    #[allow(clippy::too_many_arguments)]
    fn new_multi_socket(
        tpu_sockets: Vec<Arc<UdpSocket>>,
        tpu_vote_sockets: Vec<Arc<UdpSocket>>,
        exit: &Arc<AtomicBool>,
        sender: &PacketBatchSender,
        vote_sender: &PacketBatchSender,
        coalesce: Duration,
        in_vote_only_mode: Option<Arc<AtomicBool>>,
        tpu_enable_udp: bool,
    ) -> Self {
        let recycler: PacketBatchRecycler = Recycler::warmed(1000, 1024);

        let tpu_stats = Arc::new(StreamerReceiveStats::new("tpu_receiver"));

        let tpu_threads: Vec<_> = if tpu_enable_udp {
            tpu_sockets
                .into_iter()
                .map(|socket| {
                    streamer::receiver(
                        socket,
                        exit.clone(),
                        sender.clone(),
                        recycler.clone(),
                        tpu_stats.clone(),
                        coalesce,
                        true,
                        in_vote_only_mode.clone(),
                    )
                })
                .collect()
        } else {
            Vec::default()
        };

        let tpu_vote_stats = Arc::new(StreamerReceiveStats::new("tpu_vote_receiver"));
        let tpu_vote_threads: Vec<_> = tpu_vote_sockets
            .into_iter()
            .map(|socket| {
                streamer::receiver(
                    socket,
                    exit.clone(),
                    vote_sender.clone(),
                    recycler.clone(),
                    tpu_vote_stats.clone(),
                    coalesce,
                    true,
                    None,
                )
            })
            .collect();

        let exit = exit.clone();
        let metrics_thread_hdl = Builder::new()
            .name("solFetchStgMetr".to_string())
            .spawn(move || loop {
                sleep(Duration::from_secs(1));

                tpu_stats.report();
                tpu_vote_stats.report();

                if exit.load(Ordering::Relaxed) {
                    return;
                }
            })
            .unwrap();

        Self {
            thread_hdls: [tpu_threads, tpu_vote_threads, vec![metrics_thread_hdl]]
                .into_iter()
                .flatten()
                .collect(),
        }
    }

    pub fn join(self) -> thread::Result<()> {
        for thread_hdl in self.thread_hdls {
            thread_hdl.join()?;
        }
        Ok(())
    }
}
