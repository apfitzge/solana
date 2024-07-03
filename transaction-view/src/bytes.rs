#[inline(always)]
pub fn unchecked_read_byte(bytes: &[u8], offset: &mut usize) -> u8 {
    let value = bytes[*offset];
    *offset += 1;
    value
}

#[inline(always)]
pub fn read_byte(bytes: &[u8], offset: &mut usize) -> Option<u8> {
    let value = bytes.get(*offset).copied();
    *offset += 1;
    value
}

/// Read a compressed u16, without checking for buffer overflows.
/// This should only be used if the caller has already verified that
/// there are at least 3 bytes remaining.
/// The offset is updated to point to the byte after the u16.
#[inline(always)]
pub fn unchecked_read_compressed_u16(bytes: &[u8], offset: &mut usize) -> u16 {
    let mut result = 0u16;
    let mut shift = 0;

    for i in 0..3 {
        let byte = bytes[*offset + i];
        result |= ((byte & 0x7F) as u16) << shift;
        shift += 7;
        if byte & 0x80 == 0 {
            *offset += i + 1;
            return result;
        }
    }

    // if we reach here, it means that all 3 bytes were used
    *offset += 3;
    result
}

/// Read a compressed u16, checking for buffer overflows.
/// The offset is updated to point to the byte after the u16.
/// Returns None if the buffer is too short to read the u16.
#[inline(always)]
pub fn read_compressed_u16(bytes: &[u8], offset: &mut usize) -> Option<u16> {
    let mut result = 0u16;
    let mut shift = 0;

    for i in 0..3 {
        let byte = bytes.get(*offset + i)?;
        result |= ((byte & 0x7F) as u16) << shift;
        shift += 7;
        if byte & 0x80 == 0 {
            *offset += i + 1;
            return Some(result);
        }
    }

    // if we reach here, it means that all 3 bytes were used
    *offset += 3;
    Some(result)
}

/// Given the buffer, the current offset, and a length. Update the offset to
/// point to the byte after the array of length `len` of type `T`.
/// The size of `T` is assumed to be small enough such that a usize will not
/// overflow when multiplied by u16::MAX.
#[inline(always)]
pub fn offset_array_len<T: Sized>(bytes: &[u8], offset: &mut usize, len: u16) -> Option<()> {
    *offset = offset.checked_add((len as usize) * core::mem::size_of::<T>())?;
    if *offset > bytes.len() {
        return None;
    }
    Some(())
}

/// Given the buffer, the current offset, and a length. Update the offset to
/// point to the byte after the `T`.
/// The size of `T` is assumed to be small enough such that a usize will not
/// overflow, given then offset is currently less than u16::MAX.
#[inline(always)]
pub fn offset_type<T: Sized>(bytes: &[u8], offset: &mut usize) -> Option<()> {
    *offset += core::mem::size_of::<T>();
    if *offset > bytes.len() {
        None
    } else {
        Some(())
    }
}

#[cfg(test)]
mod tests {
    use {
        super::*,
        bincode::{serialize_into, DefaultOptions, Options},
        solana_sdk::short_vec::ShortU16,
    };

    #[test]
    fn test_unchecked_read_u16_compressed() {
        let mut buffer = [0u8; 1024];
        let options = DefaultOptions::new().with_fixint_encoding(); // Ensure fixed-int encoding

        for value in 0..=u16::MAX {
            let mut offset;
            let short_u16 = ShortU16(value);

            // Serialize the value into the buffer
            serialize_into(&mut buffer[..], &short_u16).expect("Serialization failed");

            // Use bincode's size calculation to determine the length of the serialized data
            let serialized_len = options
                .serialized_size(&short_u16)
                .expect("Failed to get serialized size");

            // Reset offset
            offset = 0;

            // Read the value back using unchecked_read_u16_compressed
            let read_value = unchecked_read_compressed_u16(&buffer, &mut offset);

            // Assert that the read value matches the original value
            assert_eq!(read_value, value, "Value mismatch for: {}", value);

            // Assert that the offset matches the serialized length
            assert_eq!(
                offset, serialized_len as usize,
                "Offset mismatch for: {}",
                value
            );
        }
    }
}
