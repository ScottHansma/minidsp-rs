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

    /// Decodes a mute setting from an f32 value read via `read_floats`.
    /// Inverse of [`Dialect::mute`]. The mute register holds an integer-shaped
    /// bit pattern that, when read as a denormal f32, has a recognizable
    /// to_bits() value. We compare those.
    pub fn decode_mute(&self, value: f32) -> Option<bool> {
        // read_floats parses raw bytes as f32 LE, so to_bits() gives back the
        // u32 LE-interpreted bit pattern of the original wire bytes.
        let bits = value.to_bits();
        match self.addr_encoding {
            AddrEncoding::AddrLen3 => {
                // mute encodes as Value::Int(1)/Int(2). Wire bytes:
                //   Int(1) -> [0x01, 0x00, 0x00, 0x00] -> LE bits 0x00000001
                //   Int(2) -> [0x02, 0x00, 0x00, 0x00] -> LE bits 0x00000002
                match bits {
                    1 => Some(true),
                    2 => Some(false),
                    _ => None,
                }
            }
            AddrEncoding::AddrLen2 => {
                // mute encodes as Value::Int32(0) / Int32(0x0080_0000), written
                // BE. So wire bytes:
                //   0          -> [0,0,0,0]       -> LE bits 0x00000000
                //   0x00800000 -> [0,0x80,0,0]    -> LE bits 0x00008000
                match bits {
                    0 => Some(true),
                    0x0000_8000 => Some(false),
                    _ => None,
                }
            }
        }
    }

    /// Decodes a dB gain reading from an f32 returned by `read_floats`.
    /// Inverse of [`Dialect::db`]. For Float32LE devices the f32 is already
    /// the dB value. For FixedPoint devices the bytes were written BE but
    /// read_floats parses them LE, so we byte-swap to recover the original
    /// FixedPoint u32 and convert to dB through it.
    pub fn decode_db(&self, value: f32) -> f32 {
        match self.float_encoding {
            FloatEncoding::Float32LE => value,
            FloatEncoding::FixedPoint => {
                let be_bits = value.to_bits().swap_bytes();
                FixedPoint::from_u32(be_bits).to_db()
            }
        }
    }
}
