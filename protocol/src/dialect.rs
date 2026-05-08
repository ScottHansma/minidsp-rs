use crate::{
    commands::{Addr, Value},
    FixedPoint,
};

/// Dialect represents the different encodings between devices
#[derive(Default)]
#[cfg_attr(feature = "debug", derive(Debug))]
pub struct Dialect {
    /// Length of addresses sent (either 3 (default) or 2)
    pub addr_encoding: AddrEncoding,

    /// Encoding for floating point values
    pub float_encoding: FloatEncoding,
}

impl Dialect {
    pub const fn const_default() -> Self {
        Self {
            addr_encoding: AddrEncoding::AddrLen3,
            float_encoding: FloatEncoding::Float32LE,
        }
    }
}

#[repr(u8)]
#[derive(Clone, Copy)]
#[cfg_attr(feature = "debug", derive(Debug))]
#[derive(Default)]
pub enum AddrEncoding {
    AddrLen2 = 2,
    #[default]
    AddrLen3 = 3,
}

#[derive(Clone, Copy)]
#[cfg_attr(feature = "debug", derive(Debug))]
#[derive(Default)]
pub enum FloatEncoding {
    #[default]
    Float32LE,
    FixedPoint,
}

impl Dialect {
    pub fn addr(&self, value: u16) -> Addr {
        Addr::new(value, self.addr_encoding as u8)
    }

    pub fn float(&self, value: f32) -> Value {
        match self.float_encoding {
            FloatEncoding::Float32LE => Value::Float(value),
            FloatEncoding::FixedPoint => Value::FixedPoint(FixedPoint::from_f32(value)),
        }
    }

    pub fn db(&self, value: f32) -> Value {
        match self.float_encoding {
            FloatEncoding::Float32LE => Value::Float(value),
            FloatEncoding::FixedPoint => Value::FixedPoint(FixedPoint::from_db(value)),
        }
    }

    pub fn int(&self, value: u16) -> Value {
        Value::Int(value)
    }

    // FIXME: Don't rely on addr len here
    pub fn delay(&self, num_samples: u32) -> Value {
        match self.addr_encoding {
            AddrEncoding::AddrLen2 => Value::Int32(num_samples),
            AddrEncoding::AddrLen3 => Value::Int(num_samples as _),
        }
    }

    pub fn mute(&self, mute: bool) -> Value {
        match self.addr_encoding {
            AddrEncoding::AddrLen2 => Value::Int32(if mute { 0x0 } else { 0x0080_0000 }),
            AddrEncoding::AddrLen3 => Value::Int(if mute { 0x1 } else { 0x2 }),
        }
    }

    pub fn invert(&self, value: bool) -> Value {
        match self.addr_encoding {
            AddrEncoding::AddrLen2 => Value::Int32(if value { 0xFF80_0000 } else { 0x0080_0000 }),
            AddrEncoding::AddrLen3 => Value::Int(value as _),
        }
    }

    /// Decodes a mute setting from a `Value` returned by a `Read` command.
    /// This is the inverse of [`Dialect::mute`].
    pub fn decode_mute(&self, value: &Value) -> Option<bool> {
        let bytes = value.clone().into_bytes();
        if bytes.len() < 4 {
            return None;
        }
        match self.addr_encoding {
            AddrEncoding::AddrLen3 => {
                // Encoded as Int LE in the first two bytes: 1 = mute, 2 = unmute
                let i = u16::from_le_bytes([bytes[0], bytes[1]]);
                match i {
                    1 => Some(true),
                    2 => Some(false),
                    _ => None,
                }
            }
            AddrEncoding::AddrLen2 => {
                // Encoded as Int32 BE: 0 = mute, 0x0080_0000 = unmute
                let v = u32::from_be_bytes([bytes[0], bytes[1], bytes[2], bytes[3]]);
                match v {
                    0 => Some(true),
                    0x0080_0000 => Some(false),
                    _ => None,
                }
            }
        }
    }

    /// Decodes a dB gain setting from a `Value` returned by a `Read` command.
    /// This is the inverse of [`Dialect::db`].
    pub fn decode_db(&self, value: &Value) -> Option<f32> {
        let bytes = value.clone().into_bytes();
        if bytes.len() < 4 {
            return None;
        }
        match self.float_encoding {
            FloatEncoding::Float32LE => Some(f32::from_le_bytes([
                bytes[0], bytes[1], bytes[2], bytes[3],
            ])),
            FloatEncoding::FixedPoint => {
                let v = u32::from_be_bytes([bytes[0], bytes[1], bytes[2], bytes[3]]);
                Some(FixedPoint::from_u32(v).to_db())
            }
        }
    }
}
