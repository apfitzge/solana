#![allow(clippy::integer_arithmetic)]
#![feature(test)]

use {
    solana_core::banking_stage::decision_maker::DecisionMaker,
    solana_ledger::{
        blockstore::Blockstore, genesis_utils::GenesisConfigInfo, get_tmp_ledger_path_auto_delete,
    },
    solana_poh::poh_recorder::{PohRecorder, Slot},
    solana_runtime::{bank::Bank, genesis_utils::create_genesis_config},
    solana_sdk::{poh_config::PohConfig, pubkey::Pubkey},
    std::sync::{Arc, RwLock},
    test::Bencher,
};

extern crate test;

fn bench_decision_maker(bencher: &mut Bencher, next_leader_slot: Option<(Slot, u64)>) {
    let GenesisConfigInfo { genesis_config, .. } = create_genesis_config(10_000);
    let bank = Arc::new(Bank::new_no_wallclock_throttle_for_tests(&genesis_config));

    let ledger_path = get_tmp_ledger_path_auto_delete!();
    let blockstore = Arc::new(
        Blockstore::open(ledger_path.path()).expect("Expected to be able to open database ledger"),
    );

    let (poh_recorder, _bank_recv, _record_recv) = PohRecorder::new(
        0,
        bank.last_blockhash(),
        bank.clone(),
        next_leader_slot,
        64,
        &Pubkey::default(),
        &blockstore,
        &Arc::default(),
        &PohConfig::default(),
        Arc::default(),
    );
    let poh_recorder = Arc::new(RwLock::new(poh_recorder));

    // If next leader slot is current slot (0)
    if let Some((0, 0)) = next_leader_slot {
        poh_recorder.write().unwrap().set_bank(&bank, false);
    }
    let decision_maker = DecisionMaker::new(Pubkey::default(), poh_recorder);

    bencher.iter(|| test::black_box(decision_maker.make_consume_or_forward_decision()));
}

#[bench]
fn bench_decision_maker_bank(bencher: &mut Bencher) {
    bench_decision_maker(bencher, Some((0, 0)));
}

#[bench]
fn bench_decision_maker_leader_shortly(bencher: &mut Bencher) {
    bench_decision_maker(bencher, Some((1, 0)));
}

#[bench]
fn bench_decision_maker_leader_hold(bencher: &mut Bencher) {
    bench_decision_maker(bencher, Some((10, 0)));
}

#[bench]
fn bench_decision_maker_unknown_leader_hold(bencher: &mut Bencher) {
    bench_decision_maker(bencher, Some((25, 0)));
}
