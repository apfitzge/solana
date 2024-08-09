use {
    agave_transaction_view::bytes::optimized_read_compressed_u16,
    core::hint::black_box,
    iai_callgrind::{library_benchmark, library_benchmark_group, main},
};

#[library_benchmark]
#[bench::single_byte(&[0x05])]
fn bench_optimized_read_compressed_u16(bytes: &[u8]) {
    black_box(optimized_read_compressed_u16(black_box(bytes), &mut 0)).unwrap();
}

library_benchmark_group!(
    name = compressed_u16;
    benchmarks = bench_optimized_read_compressed_u16
);

main!(library_benchmark_groups = compressed_u16);
