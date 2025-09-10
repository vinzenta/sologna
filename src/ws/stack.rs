use tracing::{debug, warn};

pub fn extract_program_data_for_target<'a>(
    logs: &'a [&'a str],
    target_address: &str,
) -> Vec<&'a str> {
    let mut extracted_data: Vec<&'a str> = Vec::new();
    let mut call_stack: Vec<&'a str> = Vec::new();

    for line in logs.iter() {
        if let Some(rest) = line.strip_prefix("Program ") {
            if let Some(idx) = rest.find(" invoke [") {
                let program_id = &rest[..idx];
                let depth_part = &rest[idx + " invoke [".len()..];
                if let Some(end_bracket) = depth_part.find(']') {
                    let depth_str = &depth_part[..end_bracket];
                    if let Ok(depth_num) = depth_str.parse::<usize>() {
                        let expected = call_stack.len() + 1;
                        if depth_num != expected {
                            warn!(expected_depth = %expected, seen_depth = %depth_num, line = %line, "Invoke depth mismatch while parsing logs");
                        }
                    } else {
                        warn!(line = %line, "Failed to parse invoke depth number");
                    }
                }
                call_stack.push(program_id);
                continue;
            }
            if rest.ends_with(" success") || rest.contains(" failed") {
                let program_id = if let Some(prefix) = rest.strip_suffix(" success") {
                    prefix
                } else if let Some(idx) = rest.find(" failed") {
                    &rest[..idx]
                } else {
                    rest
                };
                match call_stack.pop() {
                    Some(top) => {
                        if top != program_id {
                            warn!(popped = %top, saw = %program_id, line = %line, "Program exit does not match call stack top");
                        }
                    }
                    None => {
                        warn!(line = %line, "Program exit seen with empty call stack");
                    }
                }
                continue;
            }
        }

        if let Some(rest) = line.strip_prefix("Program data: ") {
            match call_stack.last() {
                Some(&current) if current == target_address => {
                    extracted_data.push(rest);
                }
                Some(&current) => {
                    debug!(emitter = %current, target = %target_address, "Ignoring Program data from non-target program");
                }
                None => {
                    warn!("Program data seen with empty call stack; ignoring");
                }
            }
        }
    }

    extracted_data
}
