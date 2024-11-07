use {
    super::{ComputeBudgetInstructionDetails, RuntimeTransaction},
    crate::{
        signature_details::get_precompile_signature_details, transaction_meta::TransactionMeta,
    },
    agave_transaction_view::{
        transaction_data::TransactionData, transaction_version::TransactionVersion,
        transaction_view::SanitizedTransactionView,
    },
    solana_sdk::{
        message::{TransactionSignatureDetails, VersionedMessage},
        simple_vote_transaction_checker::is_simple_vote_transaction_impl,
        transaction::{MessageHash, Result},
    },
};

fn is_simple_vote_transaction<D: TransactionData>(
    transaction: &SanitizedTransactionView<D>,
) -> bool {
    let signatures = transaction.signatures();
    let is_legacy_message = matches!(transaction.version(), TransactionVersion::Legacy);
    let instruction_programs = transaction
        .program_instructions_iter()
        .map(|(program_id, _ix)| program_id);

    is_simple_vote_transaction_impl(signatures, is_legacy_message, instruction_programs)
}

impl<D: TransactionData> RuntimeTransaction<SanitizedTransactionView<D>> {
    pub fn try_from(
        transaction: SanitizedTransactionView<D>,
        message_hash: MessageHash,
        is_simple_vote_tx: Option<bool>,
    ) -> Result<Self> {
        let message_hash = match message_hash {
            MessageHash::Precomputed(hash) => hash,
            MessageHash::Compute => VersionedMessage::hash_raw_message(transaction.message_data()),
        };
        let is_simple_vote_tx =
            is_simple_vote_tx.unwrap_or_else(|| is_simple_vote_transaction(&transaction));

        let precompile_signature_details =
            get_precompile_signature_details(transaction.program_instructions_iter());
        let signature_details = TransactionSignatureDetails::new(
            u64::from(transaction.num_required_signatures()),
            precompile_signature_details.num_secp256k1_instruction_signatures,
            precompile_signature_details.num_ed25519_instruction_signatures,
        );
        let compute_budget_instruction_details =
            ComputeBudgetInstructionDetails::try_from(transaction.program_instructions_iter())?;

        Ok(Self {
            transaction,
            meta: TransactionMeta {
                message_hash,
                is_simple_vote_transaction: is_simple_vote_tx,
                signature_details,
                compute_budget_instruction_details,
            },
        })
    }
}
