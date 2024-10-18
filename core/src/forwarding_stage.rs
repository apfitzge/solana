use {
    crate::banking_trace::BankingPacketReceiver,
    std::thread::{Builder, JoinHandle},
};

pub struct ForwardingStage {
    fwd_thread_hdl: JoinHandle<()>,
}

impl ForwardingStage {
    pub fn new(receiver: BankingPacketReceiver) -> Self {
        Self {
            fwd_thread_hdl: Builder::new()
                .name(format!("solFwdStg"))
                .spawn(move || Self::run_forwarding_loop(receiver))
                .unwrap(),
        }
    }

    fn run_forwarding_loop(receiver: BankingPacketReceiver) {
        while let Ok(_packet_batches) = receiver.recv() {}
    }

    pub fn join(self) -> std::thread::Result<()> {
        self.fwd_thread_hdl.join()
    }
}
