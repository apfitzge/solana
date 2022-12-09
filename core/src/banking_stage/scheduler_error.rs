use thiserror::Error;

/// Reasons a scheduler might fail.
#[derive(Error, Debug)]
pub enum SchedulerError {
    /// Packet receiver was disconnected.
    #[error("Packet receiver was disconnected")]
    PacketReceiverDisconnected,

    /// Scheduled work receiver was disconnected
    #[error("Scheduled work receiver was disconnected")]
    ScheduledWorkReceiverDisconnected,

    /// Processed transactions sender was disconnected.
    #[error("Processed transactions sender was disconnected")]
    ProcessedTransactionsSenderDisconnected,

    /// Processed transactions receiver was disconnected.
    #[error("Processed transactions receiver was disconnected")]
    ProcessedTransactionsReceiverDisconnected,
}
