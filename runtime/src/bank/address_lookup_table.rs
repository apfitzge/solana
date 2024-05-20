use {
    super::Bank,
    solana_sdk::{
        address_lookup_table::error::AddressLookupError,
        message::{v0::LoadedAddresses, AddressLoaderError},
        transaction::AddressLoader,
    },
    solana_signed_message::MessageAddressTableLookup,
};

fn into_address_loader_error(err: AddressLookupError) -> AddressLoaderError {
    match err {
        AddressLookupError::LookupTableAccountNotFound => {
            AddressLoaderError::LookupTableAccountNotFound
        }
        AddressLookupError::InvalidAccountOwner => AddressLoaderError::InvalidAccountOwner,
        AddressLookupError::InvalidAccountData => AddressLoaderError::InvalidAccountData,
        AddressLookupError::InvalidLookupIndex => AddressLoaderError::InvalidLookupIndex,
    }
}

impl AddressLoader for &Bank {
    fn load_addresses(
        self,
        address_table_lookups: &[solana_sdk::message::v0::MessageAddressTableLookup],
    ) -> Result<LoadedAddresses, AddressLoaderError> {
        Bank::load_addresses(
            self,
            address_table_lookups
                .iter()
                .map(|lookup| MessageAddressTableLookup {
                    account_key: &lookup.account_key,
                    writable_indexes: &lookup.writable_indexes,
                    readonly_indexes: &lookup.readonly_indexes,
                }),
        )
    }
}

impl Bank {
    pub fn load_addresses<'a>(
        &self,
        address_table_lookups: impl Iterator<Item = MessageAddressTableLookup<'a>>,
    ) -> Result<LoadedAddresses, AddressLoaderError> {
        let slot_hashes = self
            .transaction_processor
            .sysvar_cache
            .read()
            .unwrap()
            .get_slot_hashes()
            .map_err(|_| AddressLoaderError::SlotHashesSysvarNotFound)?;

        address_table_lookups
            .map(|address_table_lookup| {
                self.rc
                    .accounts
                    .load_lookup_table_addresses(
                        &self.ancestors,
                        address_table_lookup,
                        &slot_hashes,
                    )
                    .map_err(into_address_loader_error)
            })
            .collect::<Result<_, _>>()
    }
}
