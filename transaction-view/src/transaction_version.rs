#[derive(Default)]
#[repr(u8)]
pub enum TransactionVersion {
    #[default]
    Legacy = u8::MAX,
    V0 = 0,
}
