use bs58::encode as bs58encode;
use serde_json::Value;

#[derive(Debug)]
struct Cursor<'a> {
    data: &'a [u8],
    pos: usize,
}

impl<'a> Cursor<'a> {
    fn new(data: &'a [u8]) -> Self {
        Self { data, pos: 0 }
    }

    fn read_exact(&mut self, len: usize) -> anyhow::Result<&'a [u8]> {
        if self.pos + len > self.data.len() {
            anyhow::bail!("unexpected eof reading {} bytes", len);
        }
        let out = &self.data[self.pos..self.pos + len];
        self.pos += len;
        Ok(out)
    }

    fn read_u8(&mut self) -> anyhow::Result<u8> {
        Ok(self.read_exact(1)?[0])
    }

    fn read_u16(&mut self) -> anyhow::Result<u16> {
        let bytes = self.read_exact(2)?;
        Ok(u16::from_le_bytes(bytes.try_into().unwrap()))
    }

    fn read_u32(&mut self) -> anyhow::Result<u32> {
        let bytes = self.read_exact(4)?;
        Ok(u32::from_le_bytes(bytes.try_into().unwrap()))
    }

    fn read_u64(&mut self) -> anyhow::Result<u64> {
        let bytes = self.read_exact(8)?;
        Ok(u64::from_le_bytes(bytes.try_into().unwrap()))
    }

    fn read_i64(&mut self) -> anyhow::Result<i64> {
        let bytes = self.read_exact(8)?;
        Ok(i64::from_le_bytes(bytes.try_into().unwrap()))
    }

    fn read_bool(&mut self) -> anyhow::Result<bool> {
        let b = self.read_exact(1)?[0];
        Ok(b != 0)
    }

    fn read_pubkey(&mut self) -> anyhow::Result<[u8; 32]> {
        let bytes = self.read_exact(32)?;
        Ok(bytes.try_into().unwrap())
    }
}

#[derive(Clone, Debug)]
enum FieldType {
    U8,
    U16,
    U32,
    U64,
    I64,
    Bool,
    Pubkey,
    Array(Box<FieldType>, usize),
}

#[derive(Clone, Debug)]
struct FieldSpec {
    name: String,
    ty: FieldType,
}

#[derive(Clone, Debug)]
struct EventSpec {
    name: String,
    discriminator: [u8; 8],
    fields: Vec<FieldSpec>,
}

pub struct IdlEventDecoder {
    events: Vec<EventSpec>,
    // Fast lookup for event by discriminator
    disc_to_index: std::collections::HashMap<[u8; 8], usize>,
}

impl IdlEventDecoder {
    pub fn from_idl_path<P: AsRef<std::path::Path>>(path: P) -> anyhow::Result<Self> {
        let idl = std::fs::read_to_string(path)?;
        let idl_json: Value = serde_json::from_str(&idl)?;
        Self::from_idl_value(&idl_json)
    }

    pub fn from_idl_value(idl: &Value) -> anyhow::Result<Self> {
        let events = idl
            .get("events")
            .and_then(|v| v.as_array())
            .ok_or_else(|| anyhow::anyhow!("events[] not found in idl"))?;
        let types = idl
            .get("types")
            .and_then(|v| v.as_array())
            .ok_or_else(|| anyhow::anyhow!("types[] not found in idl"))?;

        let mut specs: Vec<EventSpec> = Vec::new();
        for ev in events {
            let name = ev
                .get("name")
                .and_then(|v| v.as_str())
                .ok_or_else(|| anyhow::anyhow!("event missing name"))?
                .to_string();
            let disc_arr = ev
                .get("discriminator")
                .and_then(|v| v.as_array())
                .ok_or_else(|| anyhow::anyhow!("event missing discriminator"))?;
            if disc_arr.len() != 8 {
                anyhow::bail!("event discriminator must be 8 bytes");
            }
            let mut d = [0u8; 8];
            for (i, v) in disc_arr.iter().enumerate() {
                d[i] = v.as_u64().unwrap_or(0) as u8;
            }

            // Find struct def in types with same name
            let type_def = types
                .iter()
                .find(|t| t.get("name").and_then(|v| v.as_str()) == Some(&name))
                .ok_or_else(|| anyhow::anyhow!(format!("no type struct for event {}", name)))?;
            let fields = type_def
                .get("type")
                .and_then(|v| v.get("fields"))
                .and_then(|v| v.as_array())
                .ok_or_else(|| anyhow::anyhow!("event struct missing fields"))?;

            let mut field_specs = Vec::with_capacity(fields.len());
            for f in fields {
                let fname = f
                    .get("name")
                    .and_then(|v| v.as_str())
                    .ok_or_else(|| anyhow::anyhow!("field missing name"))?
                    .to_string();
                let ftyv = f
                    .get("type")
                    .ok_or_else(|| anyhow::anyhow!("field missing type"))?;
                let fty = parse_field_type(ftyv)?;
                field_specs.push(FieldSpec {
                    name: fname,
                    ty: fty,
                });
            }

            specs.push(EventSpec {
                name,
                discriminator: d,
                fields: field_specs,
            });
        }

        // Build discriminator -> index map for O(1) lookup during decode
        let mut disc_to_index = std::collections::HashMap::with_capacity(specs.len());
        for (idx, ev) in specs.iter().enumerate() {
            disc_to_index.insert(ev.discriminator, idx);
        }

        Ok(Self {
            events: specs,
            disc_to_index,
        })
    }

    pub fn decode(&self, bytes: &[u8]) -> anyhow::Result<(String, Value)> {
        if bytes.len() < 8 {
            anyhow::bail!("input shorter than 8 bytes");
        }
        let disc = &bytes[..8];
        let Some(&idx) = self.disc_to_index.get(disc) else {
            anyhow::bail!("unknown event discriminator");
        };
        let spec = &self.events[idx];

        let mut cur = Cursor::new(&bytes[8..]);
        let mut obj = serde_json::Map::new();
        for fs in &spec.fields {
            let key = snake_to_camel(&fs.name);
            let val = decode_field(&mut cur, &fs.ty)?;
            obj.insert(key, val);
        }
        Ok((spec.name.clone(), Value::Object(obj)))
    }
}

fn parse_field_type(tyv: &Value) -> anyhow::Result<FieldType> {
    if let Some(s) = tyv.as_str() {
        return Ok(match s {
            "u8" => FieldType::U8,
            "u16" => FieldType::U16,
            "u32" => FieldType::U32,
            "u64" => FieldType::U64,
            "i64" => FieldType::I64,
            "bool" => FieldType::Bool,
            "pubkey" => FieldType::Pubkey,
            other => anyhow::bail!("unsupported primitive type in events: {}", other),
        });
    }
    if let Some(obj) = tyv.as_object()
        && let Some(arr) = obj.get("array").and_then(|v| v.as_array())
    {
        if arr.len() != 2 {
            anyhow::bail!("array type must have [elem, len]");
        }
        let elem = parse_field_type(&arr[0])?;
        let len = arr[1]
            .as_u64()
            .ok_or_else(|| anyhow::anyhow!("array length must be number"))?
            as usize;
        return Ok(FieldType::Array(Box::new(elem), len));
    }
    anyhow::bail!("unsupported field type in events: {}", tyv)
}

fn decode_field(cur: &mut Cursor, ty: &FieldType) -> anyhow::Result<Value> {
    Ok(match ty {
        FieldType::U8 => Value::String(cur.read_u8()?.to_string()),
        FieldType::U16 => Value::String(cur.read_u16()?.to_string()),
        FieldType::U32 => Value::String(cur.read_u32()?.to_string()),
        FieldType::U64 => Value::String(cur.read_u64()?.to_string()),
        FieldType::I64 => Value::String(cur.read_i64()?.to_string()),
        FieldType::Bool => Value::Bool(cur.read_bool()?),
        FieldType::Pubkey => {
            let pk = cur.read_pubkey()?;
            Value::String(bs58encode(pk).into_string())
        }
        FieldType::Array(elem, len) => {
            let mut arr = Vec::with_capacity(*len);
            for _ in 0..*len {
                arr.push(decode_field(cur, elem)?);
            }
            Value::Array(arr)
        }
    })
}

fn snake_to_camel(s: &str) -> String {
    let mut out = String::with_capacity(s.len());
    let mut upper_next = false;
    for (i, ch) in s.chars().enumerate() {
        if ch == '_' {
            if i != 0 {
                upper_next = true;
            }
            continue;
        }
        if upper_next {
            out.extend(ch.to_uppercase());
            upper_next = false;
        } else {
            out.push(ch);
        }
    }
    out
}
