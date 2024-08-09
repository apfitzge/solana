use {agave_transaction_view::bytes::optimized_read_compressed_u16, iai::black_box};

fn benchmark_optimized_read_compressed_u16_single_byte() {
    let _result = optimized_read_compressed_u16(black_box(&[0b0100_0000]), &mut 0);
}

iai::main!(benchmark_optimized_read_compressed_u16_single_byte);
