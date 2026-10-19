//! LEB128, the variable-width integer that all three of this crate's encodings use.
//!
//! There are three of them. They are the record format of the store, a pack index,
//! and the snapshot encoding. They share this module and almost nothing else.
//! Their field orders, framing, trust postures, and error models are unrelated.
//! Their failure modes differ by design. A record decoder treats a bad number as
//! the end of a torn tail and stops. A pack decoder must name what went wrong to a
//! person. A snapshot decoder must bound an allocation before it makes it. Merging
//! the *formats* would blur exactly the distinction worth keeping.
//!
//! The *number* is not one of those distinctions. Three copies of it meant three
//! copies of the one piece of logic here that is security-relevant. A tenth byte
//! may contribute only the last bit of a `u64`. An encoding that says otherwise is
//! either an overflow or a way to spend ten bytes saying zero.

use std::fmt;

/// Longest LEB128 encoding of a `u64`.
///
/// Ten groups of seven bits is sixty-four, and the tenth has one bit to spare.
pub const MAX: usize = 10;

/// Why a varint could not be read.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Error {
    /// The bytes ran out before the number did.
    Truncated,
    /// More than [`MAX`] groups, or a tenth group contributing more than the one
    /// bit a `u64` has left.
    ///
    /// **This is the only way a number can fail to fit.** The text did not always
    /// say so. An `Error::Overflow` sat beside this one for "the groups did not
    /// fit a `u64`". `read` cannot return that error. Ten groups of seven bits are
    /// sixty-four. The tenth group is the one place where a bit can fall off the
    /// end. The tenth byte contributes more than that single bit when the number
    /// does not fit. A `u64` cannot be overfilled by any other route. The two
    /// answers were one answer with two names. A caller that matched on them had a
    /// case that never came. Coverage said so. The coverage of the enum's own
    /// `Display` was the tell.
    Overlong,
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let what = match self {
            Self::Truncated => "it ends in the middle of a number",
            Self::Overlong => "it is longer than a number can be",
        };
        f.write_str(what)
    }
}

impl std::error::Error for Error {}

/// Append `value` to `out` as LEB128.
pub fn write(out: &mut Vec<u8>, value: u64) {
    let mut value = value;
    loop {
        // `value & 0x7f` is in 0..=127, so this cannot fail. It used to be a
        // `u8::try_from(..).unwrap_or(0)`. That fallback was unreachable and was the
        // worst possible fallback. A wider mask would have written a zero-length
        // number into a record header rather than refusing.
        let byte = (value & 0x7f) as u8;
        value >>= 7;

        if value == 0 {
            out.push(byte);
            return;
        }

        out.push(byte | 0x80);
    }
}

/// Append `value` to `out` as LEB128, padded to exactly [`MAX`] bytes.
///
/// The padded form is still a valid LEB128 number. The first nine bytes hold
/// seven bits each and ask for another. The tenth holds the last bit of the
/// `u64` and ends the number. [`read`] stops at the tenth byte like it does at
/// the last byte of any other encoding, so a reader needs no change.
///
/// The padding costs bytes a short number does not need. A caller pays that to
/// reserve the width *before* it knows the value. The caller can then write the
/// value into the reserved bytes afterwards without moving a byte that follows
/// it.
pub fn write_padded(out: &mut Vec<u8>, value: u64) {
    // Nine groups of seven bits are sixty-three. The tenth byte therefore holds
    // the last bit of the `u64` and nothing else.
    for shift in 0..MAX - 1 {
        out.push((((value >> (shift * 7)) & 0x7f) as u8) | 0x80);
    }
    out.push(((value >> 63) & 1) as u8);
}

/// The LEB128 number at `at`, and how many bytes it occupied.
///
/// # Errors
///
/// Returns [`Error::Truncated`] if the bytes run out first. It returns
/// [`Error::Overlong`] if the encoding is longer than a `u64` can be. That error
/// is the one failure a number has here. Ten groups of seven bits are sixty-four.
/// A tenth group that contributes more than the single bit left over makes the
/// number not fit.
pub fn read(bytes: &[u8], at: usize) -> Result<(u64, usize), Error> {
    let mut out: u64 = 0;
    let mut shift: u32 = 0;

    for index in 0..MAX {
        let byte = *bytes.get(at + index).ok_or(Error::Truncated)?;

        // The tenth group can contribute only the last bit of a u64. More than that
        // is an overlong encoding of something shorter. Accepting it would let a
        // caller believe a number it was never sent. This check also makes the
        // shift below infallible. The shift never passes 63.
        if index == MAX - 1 && byte > 1 {
            return Err(Error::Overlong);
        }

        out |= u64::from(byte & 0x7f) << shift;

        if byte & 0x80 == 0 {
            return Ok((out, index + 1));
        }

        shift += 7;
    }

    // Ten groups and the last one still had its continuation bit set.
    //
    // **Unreachable, and kept as an error rather than removed.** A tenth byte with
    // its continuation bit set is more than the one bit a `u64` has left. The check
    // inside the loop has already answered `Overlong` for it. The function still
    // must produce something here. An error is the only thing it can produce. A
    // number that does not fit is not a number. Nothing else in this signature says
    // so. If a future encoding makes this reachable, the coverage report starts
    // listing this line. The answer is already right.
    Err(Error::Overlong)
}

/// Bytes `value` occupies as LEB128.
///
/// The same count [`write()`] produces. This function is checked against that count.
/// A disagreement between the two would mean a header sized one way and written
/// another.
#[must_use]
pub fn len(value: u64) -> usize {
    let mut value = value;
    let mut len = 1;
    while value >= 0x80 {
        value >>= 7;
        len += 1;
    }
    len
}

#[cfg(test)]
mod tests {
    use super::{Error, MAX, len, read, write, write_padded};

    #[test]
    fn a_number_survives_a_round_trip_at_every_width() {
        // One value per width, plus the boundaries, because a bug in the tenth
        // group is invisible at every smaller one.
        for shift in 0..64 {
            let value = 1u64 << shift;
            let mut out = Vec::new();
            write(&mut out, value);
            assert_eq!(out.len(), len(value), "width disagrees for {value}");
            assert_eq!(read(&out, 0), Ok((value, out.len())));
        }
    }

    #[test]
    fn zero_and_the_boundaries_encode_to_what_they_should() {
        for value in [0, 1, 127, 128, u64::from(u32::MAX), u64::MAX] {
            let mut out = Vec::new();
            write(&mut out, value);
            assert_eq!(out.len(), len(value));
            assert_eq!(read(&out, 0), Ok((value, out.len())));
        }
    }

    #[test]
    fn the_widest_number_is_ten_bytes_and_the_next_one_is_not_a_number() {
        let mut out = Vec::new();
        write(&mut out, u64::MAX);
        assert_eq!(out.len(), MAX);

        // A tenth group contributing more than the last bit of a u64.
        let mut overlong = out.clone();
        overlong[MAX - 1] |= 0x02;
        assert_eq!(read(&overlong, 0), Err(Error::Overlong));
    }

    #[test]
    fn a_number_that_never_ends_is_refused_rather_than_read() {
        // Eleven continuation bytes: longer than any number, and the shape a
        // reader of a damaged index meets.
        let endless = [0x80u8; MAX + 1];
        assert_eq!(read(&endless, 0), Err(Error::Overlong));

        // Ten, with the last still asking for more.
        let exactly_max = [0x80u8; MAX];
        assert_eq!(read(&exactly_max, 0), Err(Error::Overlong));
    }

    #[test]
    fn a_padded_number_is_ten_bytes_and_reads_back_as_itself() {
        // One short value, the boundaries, and the widest. A padded zero is the
        // case a shortening encoder would get wrong.
        for value in [0, 1, 127, 128, u64::from(u32::MAX), u64::MAX] {
            let mut out = Vec::new();
            write_padded(&mut out, value);
            assert_eq!(out.len(), MAX, "a padded number is {MAX} bytes for {value}");
            assert_eq!(read(&out, 0), Ok((value, MAX)));
        }
    }

    #[test]
    fn running_out_of_bytes_is_distinct_from_running_out_of_number() {
        let mut out = Vec::new();
        write(&mut out, 300);
        assert_eq!(read(&out[..1], 0), Err(Error::Truncated));
        assert_eq!(read(&out, out.len()), Err(Error::Truncated));
    }

    /// What a refusal says, since a caller shows it.
    ///
    /// This is not a formatting test for its own sake. These two strings are the
    /// whole of what a caller of this crate learns about a damaged file from the
    /// error alone. The distinction between them is the distinction between "cut
    /// short" and "not a number".
    #[test]
    fn a_refusal_says_which_half_of_the_number_is_missing() {
        assert_eq!(
            Error::Truncated.to_string(),
            "it ends in the middle of a number"
        );
        assert_eq!(
            Error::Overlong.to_string(),
            "it is longer than a number can be"
        );
    }

    #[test]
    fn a_number_is_read_from_where_it_starts() {
        let mut out = vec![0xaa, 0xbb];
        write(&mut out, 42);
        assert_eq!(read(&out, 2), Ok((42, 1)));
    }
}
