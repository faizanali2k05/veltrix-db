//! RESP2 values and an incremental, bounded wire codec.

use bytes::{Buf, BytesMut};
use std::fmt::Write as _;
use thiserror::Error;

/// A RESP2 value.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Value {
    /// A `+`-prefixed string without CR or LF.
    SimpleString(String),
    /// A `-`-prefixed error without CR or LF.
    Error(String),
    /// A signed decimal integer.
    Integer(i64),
    /// A binary-safe bulk string, or RESP null bulk string.
    BulkString(Option<Vec<u8>>),
    /// An array, or RESP null array.
    Array(Option<Vec<Value>>),
}

/// RESP2 serialization failure.
#[derive(Clone, Copy, Debug, Error, PartialEq, Eq)]
pub enum EncodeError {
    /// Simple strings and errors cannot contain a carriage return or newline.
    #[error("RESP line values cannot contain CR or LF")]
    InvalidLine,
}

impl Value {
    /// Construct a non-null bulk string.
    #[must_use]
    pub fn bulk(bytes: impl Into<Vec<u8>>) -> Self {
        Self::BulkString(Some(bytes.into()))
    }

    /// Encode this value into `output`.
    ///
    /// # Panics
    ///
    /// Panics if a simple string or error contains CR or LF. Use
    /// [`Value::try_encode`] when encoding untrusted values.
    pub fn encode(&self, output: &mut Vec<u8>) {
        self.try_encode(output)
            .expect("RESP simple strings and errors must not contain CR or LF");
    }

    /// Encode this value into `output`, validating RESP line values.
    ///
    /// # Errors
    ///
    /// Returns [`EncodeError::InvalidLine`] if a simple string or error
    /// contains CR or LF.
    pub fn try_encode(&self, output: &mut Vec<u8>) -> Result<(), EncodeError> {
        match self {
            Self::SimpleString(value) => {
                validate_line(value)?;
                output.push(b'+');
                output.extend_from_slice(value.as_bytes());
                output.extend_from_slice(b"\r\n");
            }
            Self::Error(value) => {
                validate_line(value)?;
                output.push(b'-');
                output.extend_from_slice(value.as_bytes());
                output.extend_from_slice(b"\r\n");
            }
            Self::Integer(value) => {
                let mut encoded = String::new();
                write!(&mut encoded, ":{value}\r\n").expect("writing to a String cannot fail");
                output.extend_from_slice(encoded.as_bytes());
            }
            Self::BulkString(None) => output.extend_from_slice(b"$-1\r\n"),
            Self::BulkString(Some(value)) => {
                let mut header = String::new();
                write!(&mut header, "${}\r\n", value.len())
                    .expect("writing to a String cannot fail");
                output.extend_from_slice(header.as_bytes());
                output.extend_from_slice(value);
                output.extend_from_slice(b"\r\n");
            }
            Self::Array(None) => output.extend_from_slice(b"*-1\r\n"),
            Self::Array(Some(values)) => {
                let mut header = String::new();
                write!(&mut header, "*{}\r\n", values.len())
                    .expect("writing to a String cannot fail");
                output.extend_from_slice(header.as_bytes());
                for value in values {
                    value.try_encode(output)?;
                }
            }
        }
        Ok(())
    }

    /// Return the complete wire representation.
    ///
    /// # Panics
    ///
    /// Panics if a simple string or error contains CR or LF. Use
    /// [`Value::try_to_bytes`] when encoding untrusted values.
    #[must_use]
    pub fn to_bytes(&self) -> Vec<u8> {
        let mut output = Vec::new();
        self.encode(&mut output);
        output
    }

    /// Return a validated complete wire representation.
    ///
    /// # Errors
    ///
    /// Returns [`EncodeError::InvalidLine`] for invalid line values.
    pub fn try_to_bytes(&self) -> Result<Vec<u8>, EncodeError> {
        let mut output = Vec::new();
        self.try_encode(&mut output)?;
        Ok(output)
    }
}

fn validate_line(value: &str) -> Result<(), EncodeError> {
    if value.bytes().any(|byte| matches!(byte, b'\r' | b'\n')) {
        Err(EncodeError::InvalidLine)
    } else {
        Ok(())
    }
}

/// Resource limits applied by the RESP decoder.
#[derive(Clone, Copy, Debug)]
pub struct Limits {
    /// Maximum bytes retained for one frame.
    pub max_frame_bytes: usize,
    /// Maximum payload length of one bulk string.
    pub max_bulk_bytes: usize,
    /// Maximum number of values in one array.
    pub max_array_len: usize,
    /// Maximum nested array depth.
    pub max_depth: usize,
}

impl Default for Limits {
    fn default() -> Self {
        Self {
            max_frame_bytes: 16 * 1024 * 1024,
            max_bulk_bytes: 8 * 1024 * 1024,
            max_array_len: 1024,
            max_depth: 32,
        }
    }
}

/// A protocol or resource-limit violation.
#[derive(Debug, Error, PartialEq, Eq)]
pub enum DecodeError {
    /// The frame prefix is not part of RESP2.
    #[error("unknown RESP type byte 0x{0:02x}")]
    UnknownType(u8),
    /// A line did not contain a valid signed decimal integer.
    #[error("invalid integer")]
    InvalidInteger,
    /// A RESP line was not valid UTF-8.
    #[error("text field is not valid UTF-8")]
    InvalidUtf8,
    /// Only `-1` is legal as a negative aggregate length.
    #[error("invalid negative length")]
    InvalidLength,
    /// The declared bulk body was not followed by CRLF.
    #[error("bulk string is missing terminating CRLF")]
    InvalidBulkTerminator,
    /// A configured resource limit was exceeded.
    #[error("RESP frame exceeds configured limits")]
    LimitExceeded,
}

/// Incremental RESP2 decoder.
#[derive(Clone, Debug, Default)]
pub struct Decoder {
    limits: Limits,
}

impl Decoder {
    /// Create a decoder with explicit resource limits.
    #[must_use]
    pub const fn new(limits: Limits) -> Self {
        Self { limits }
    }

    /// Decode one frame, retaining incomplete bytes in `buffer`.
    ///
    /// On success, exactly one frame is removed. A `None` result means more
    /// bytes are required and the buffer is unchanged.
    ///
    /// # Errors
    ///
    /// Returns a [`DecodeError`] when the frame is malformed or exceeds a
    /// configured resource limit.
    pub fn decode(&self, buffer: &mut BytesMut) -> Result<Option<Value>, DecodeError> {
        match parse_value(buffer, 0, 0, self.limits)? {
            Parse::Complete(value, consumed) => {
                if consumed > self.limits.max_frame_bytes {
                    return Err(DecodeError::LimitExceeded);
                }
                buffer.advance(consumed);
                Ok(Some(value))
            }
            Parse::Incomplete if buffer.len() > self.limits.max_frame_bytes => {
                Err(DecodeError::LimitExceeded)
            }
            Parse::Incomplete => Ok(None),
        }
    }
}

enum Parse {
    Complete(Value, usize),
    Incomplete,
}

fn parse_value(
    input: &[u8],
    offset: usize,
    depth: usize,
    limits: Limits,
) -> Result<Parse, DecodeError> {
    if depth > limits.max_depth {
        return Err(DecodeError::LimitExceeded);
    }
    let Some(&prefix) = input.get(offset) else {
        return Ok(Parse::Incomplete);
    };
    match prefix {
        b'+' | b'-' | b':' => parse_line_value(input, offset, prefix),
        b'$' => parse_bulk(input, offset, limits),
        b'*' => parse_array(input, offset, depth, limits),
        other => Err(DecodeError::UnknownType(other)),
    }
}

fn parse_line(input: &[u8], start: usize) -> Option<(&[u8], usize)> {
    input[start..]
        .windows(2)
        .position(|window| window == b"\r\n")
        .map(|relative| (&input[start..start + relative], start + relative + 2))
}

fn parse_line_value(input: &[u8], offset: usize, prefix: u8) -> Result<Parse, DecodeError> {
    let Some((line, consumed)) = parse_line(input, offset + 1) else {
        return Ok(Parse::Incomplete);
    };
    let value = match prefix {
        b'+' => Value::SimpleString(
            std::str::from_utf8(line)
                .map_err(|_| DecodeError::InvalidUtf8)?
                .to_owned(),
        ),
        b'-' => Value::Error(
            std::str::from_utf8(line)
                .map_err(|_| DecodeError::InvalidUtf8)?
                .to_owned(),
        ),
        b':' => Value::Integer(parse_decimal(line)?),
        _ => unreachable!("caller validates line prefixes"),
    };
    Ok(Parse::Complete(value, consumed))
}

fn parse_bulk(input: &[u8], offset: usize, limits: Limits) -> Result<Parse, DecodeError> {
    let Some((line, header_end)) = parse_line(input, offset + 1) else {
        return Ok(Parse::Incomplete);
    };
    let length = parse_decimal(line)?;
    if length == -1 {
        return Ok(Parse::Complete(Value::BulkString(None), header_end));
    }
    let length = non_negative_length(length)?;
    if length > limits.max_bulk_bytes {
        return Err(DecodeError::LimitExceeded);
    }
    let body_end = header_end
        .checked_add(length)
        .ok_or(DecodeError::LimitExceeded)?;
    let frame_end = body_end.checked_add(2).ok_or(DecodeError::LimitExceeded)?;
    if frame_end > limits.max_frame_bytes {
        return Err(DecodeError::LimitExceeded);
    }
    if input.len() < frame_end {
        return Ok(Parse::Incomplete);
    }
    if &input[body_end..frame_end] != b"\r\n" {
        return Err(DecodeError::InvalidBulkTerminator);
    }
    Ok(Parse::Complete(
        Value::BulkString(Some(input[header_end..body_end].to_vec())),
        frame_end,
    ))
}

fn parse_array(
    input: &[u8],
    offset: usize,
    depth: usize,
    limits: Limits,
) -> Result<Parse, DecodeError> {
    let Some((line, mut cursor)) = parse_line(input, offset + 1) else {
        return Ok(Parse::Incomplete);
    };
    let length = parse_decimal(line)?;
    if length == -1 {
        return Ok(Parse::Complete(Value::Array(None), cursor));
    }
    let length = non_negative_length(length)?;
    if length > limits.max_array_len {
        return Err(DecodeError::LimitExceeded);
    }
    let mut values = Vec::with_capacity(length);
    for _ in 0..length {
        match parse_value(input, cursor, depth + 1, limits)? {
            Parse::Complete(value, consumed) => {
                cursor = consumed;
                if cursor > limits.max_frame_bytes {
                    return Err(DecodeError::LimitExceeded);
                }
                values.push(value);
            }
            Parse::Incomplete => return Ok(Parse::Incomplete),
        }
    }
    Ok(Parse::Complete(Value::Array(Some(values)), cursor))
}

fn parse_decimal(bytes: &[u8]) -> Result<i64, DecodeError> {
    std::str::from_utf8(bytes)
        .map_err(|_| DecodeError::InvalidInteger)?
        .parse()
        .map_err(|_| DecodeError::InvalidInteger)
}

fn non_negative_length(value: i64) -> Result<usize, DecodeError> {
    usize::try_from(value).map_err(|_| DecodeError::InvalidLength)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn round_trips_nested_values() {
        let value = Value::Array(Some(vec![
            Value::bulk(b"SET".to_vec()),
            Value::bulk(b"binary\0key".to_vec()),
            Value::Integer(-42),
            Value::BulkString(None),
        ]));
        let mut bytes = BytesMut::from(value.to_bytes().as_slice());
        assert_eq!(Decoder::default().decode(&mut bytes), Ok(Some(value)));
        assert!(bytes.is_empty());
    }

    #[test]
    fn retains_every_fragment_until_complete() {
        let encoded = b"*2\r\n$4\r\nECHO\r\n$5\r\nhello\r\n";
        let decoder = Decoder::default();
        let mut buffer = BytesMut::new();
        for (index, byte) in encoded.iter().enumerate() {
            buffer.extend_from_slice(&[*byte]);
            let result = decoder.decode(&mut buffer).unwrap();
            if index + 1 == encoded.len() {
                assert!(result.is_some());
            } else {
                assert_eq!(result, None);
            }
        }
    }

    #[test]
    fn decodes_pipelined_frames_one_at_a_time() {
        let mut buffer = BytesMut::from(&b"+OK\r\n:3\r\n"[..]);
        let decoder = Decoder::default();
        assert_eq!(
            decoder.decode(&mut buffer),
            Ok(Some(Value::SimpleString("OK".into())))
        );
        assert_eq!(decoder.decode(&mut buffer), Ok(Some(Value::Integer(3))));
    }

    #[test]
    fn frame_limit_allows_multiple_small_pipelined_frames() {
        let decoder = Decoder::new(Limits {
            max_frame_bytes: 5,
            ..Limits::default()
        });
        let mut buffer = BytesMut::from(&b"+OK\r\n+HI\r\n"[..]);
        assert_eq!(
            decoder.decode(&mut buffer),
            Ok(Some(Value::SimpleString("OK".into())))
        );
        assert_eq!(
            decoder.decode(&mut buffer),
            Ok(Some(Value::SimpleString("HI".into())))
        );
    }

    #[test]
    fn rejects_malformed_and_oversized_input() {
        let decoder = Decoder::default();
        let mut malformed = BytesMut::from(&b"$3\r\nabcXX"[..]);
        assert_eq!(
            decoder.decode(&mut malformed),
            Err(DecodeError::InvalidBulkTerminator)
        );

        let decoder = Decoder::new(Limits {
            max_bulk_bytes: 2,
            ..Limits::default()
        });
        let mut oversized = BytesMut::from(&b"$3\r\nabc\r\n"[..]);
        assert_eq!(
            decoder.decode(&mut oversized),
            Err(DecodeError::LimitExceeded)
        );
    }

    #[test]
    fn serializer_rejects_line_injection() {
        assert_eq!(
            Value::Error("ERR bad\r\n+injected".into()).try_to_bytes(),
            Err(EncodeError::InvalidLine)
        );
    }
}
