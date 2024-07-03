#![feature(test)]

use {
    agave_transaction_view::bytes::unchecked_read_u16_compressed,
    bincode::{serialize_into, DefaultOptions, Options},
    solana_sdk::short_vec::{decode_shortu16_len, ShortU16},
    test::Bencher,
};
extern crate test;

fn setup() -> Vec<(u16, usize, Vec<u8>)> {
    let options = DefaultOptions::new().with_fixint_encoding(); // Ensure fixed-int encoding

    // Create a vector of all u16 values serialized into 16-byte buffers.
    let mut values = Vec::with_capacity(u16::MAX as usize);
    for value in 0..u16::MAX {
        let short_u16 = ShortU16(value);
        let mut buffer = vec![0u8; 16];
        let serialized_len = options
            .serialized_size(&short_u16)
            .expect("Failed to get serialized size");
        serialize_into(&mut buffer[..], &short_u16).expect("Serialization failed");
        values.push((value, serialized_len as usize, buffer));
    }

    values
}

#[bench]
fn bench_decode_shortu16_len(bencher: &mut Bencher) {
    let values_serialized_lengths_and_buffers = setup();

    bencher.iter(|| {
        for (value, serialized_len, buffer) in values_serialized_lengths_and_buffers.iter() {
            // Read the value back using bincode's decode
            let (read_value, bytes_read) = decode_shortu16_len(&buffer).unwrap();

            // Assert that the read value matches the original value
            assert_eq!(read_value, *value as usize, "Value mismatch for: {}", value);

            // Assert that the offset matches the serialized length
            assert_eq!(
                bytes_read, *serialized_len,
                "Offset mismatch for: {}",
                value
            );
        }
    })
}

#[bench]
fn bench_unchecked_read_u16_compressed(bencher: &mut Bencher) {
    let values_serialized_lengths_and_buffers = setup();

    bencher.iter(|| {
        for (value, serialized_len, buffer) in values_serialized_lengths_and_buffers.iter() {
            let mut offset = 0;

            // Read the value back using unchecked_read_u16_compressed
            let read_value = unchecked_read_u16_compressed(&buffer, &mut offset);

            // Assert that the read value matches the original value
            assert_eq!(read_value, *value, "Value mismatch for: {}", value);

            // Assert that the offset matches the serialized length
            assert_eq!(
                offset, *serialized_len as usize,
                "Offset mismatch for: {}",
                value
            );
        }
    })
}
