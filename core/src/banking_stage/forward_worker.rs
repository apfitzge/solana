use {
    super::{
        forwarder::Forwarder,
        scheduler_messages::{FinishedForwardWork, ForwardWork},
        ForwardOption,
    },
    crossbeam_channel::{Receiver, RecvError, SendError, Sender},
    thiserror::Error,
};

#[derive(Debug, Error)]
pub enum ForwardWorkerError {
    #[error("Failed to receive work from scheduler: {0}")]
    Recv(#[from] RecvError),
    #[error("Failed to send finalized forward work to scheduler: {0}")]
    Send(#[from] SendError<FinishedForwardWork>),
}

pub(crate) struct ForwardWorker {
    forward_receiver: Receiver<ForwardWork>,
    forward_option: ForwardOption,
    forwarder: Forwarder,
    forwarded_sender: Sender<FinishedForwardWork>,
}

#[allow(dead_code)]
impl ForwardWorker {
    pub fn new(
        forward_receiver: Receiver<ForwardWork>,
        forward_option: ForwardOption,
        forwarder: Forwarder,
        forwarded_sender: Sender<FinishedForwardWork>,
    ) -> Self {
        Self {
            forward_receiver,
            forward_option,
            forwarder,
            forwarded_sender,
        }
    }

    pub fn run(self) -> Result<(), ForwardWorkerError> {
        loop {
            let work = self.forward_receiver.recv()?;
            self.forward_loop(work)?;
        }
    }

    fn forward_loop(&self, work: ForwardWork) -> Result<(), ForwardWorkerError> {
        for work in try_drain_iter(work, &self.forward_receiver) {
            let (res, _num_packets, _forward_us, _leader_pubkey) = self.forwarder.forward_packets(
                &self.forward_option,
                work.packets.iter().map(|p| p.original_packet()),
            );
            match res {
                Ok(()) => self.forwarded_sender.send(FinishedForwardWork {
                    work,
                    successful: true,
                })?,
                Err(_err) => return self.failed_forward_drain(work),
            };
        }
        Ok(())
    }

    fn failed_forward_drain(&self, work: ForwardWork) -> Result<(), ForwardWorkerError> {
        for work in try_drain_iter(work, &self.forward_receiver) {
            self.forwarded_sender.send(FinishedForwardWork {
                work,
                successful: false,
            })?;
        }
        Ok(())
    }
}

/// Helper function to create an non-blocking iterator over work in the receiver,
/// starting with the given work item.
fn try_drain_iter<T>(work: T, receiver: &Receiver<T>) -> impl Iterator<Item = T> + '_ {
    std::iter::once(work).chain(receiver.try_iter())
}
