use thiserror::Error;

/// Reasons a scheduler might fail.
#[derive(Error, Debug)]
pub enum SchedulerError {
    /// Packet receiver was disconnected.
    #[error("Packet receiver was disconnected")]
    PacketReceiverDisconnected,
}
