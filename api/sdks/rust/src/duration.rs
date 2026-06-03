//! Parsing of Go `time.Duration` string representations.
//!
//! The REST and QUIC servers serialize durations using Go's `Duration.String()`
//! (e.g. `"5.2s"`, `"1m30s"`, `"100ms"`, `"1h2m3s"`, `"1.5µs"`). The SDK exposes
//! these as integer milliseconds, so this module converts the Go textual form
//! into milliseconds.

/// Parse a Go `time.Duration` string into milliseconds.
///
/// Returns `0` for an empty string or `"0s"`. Unparseable input yields `0` so
/// that callers never fail solely because of a malformed duration field.
pub fn parse_go_duration_ms(s: &str) -> i64 {
    parse_go_duration_nanos(s)
        .map(|nanos| nanos / 1_000_000)
        .unwrap_or(0)
}

/// Parse a Go `time.Duration` string into nanoseconds, returning `None` on
/// malformed input.
fn parse_go_duration_nanos(s: &str) -> Option<i64> {
    let s = s.trim();
    if s.is_empty() {
        return Some(0);
    }
    if s == "0" {
        return Some(0);
    }

    let mut chars = s.chars().peekable();
    let mut total: f64 = 0.0;
    let mut sign = 1.0;

    if let Some(&c) = chars.peek() {
        if c == '+' || c == '-' {
            if c == '-' {
                sign = -1.0;
            }
            chars.next();
        }
    }

    // Must contain at least one unit-prefixed value.
    let mut saw_unit = false;

    while chars.peek().is_some() {
        // Parse the numeric portion (digits and an optional decimal point).
        let mut num = String::new();
        while let Some(&c) = chars.peek() {
            if c.is_ascii_digit() || c == '.' {
                num.push(c);
                chars.next();
            } else {
                break;
            }
        }
        if num.is_empty() {
            return None;
        }
        let value: f64 = num.parse().ok()?;

        // Parse the unit. The longest valid unit is "µs"/"us"/"ms"/"ns".
        let mut unit = String::new();
        while let Some(&c) = chars.peek() {
            if c.is_ascii_digit() || c == '.' {
                break;
            }
            unit.push(c);
            chars.next();
        }
        if unit.is_empty() {
            return None;
        }

        let unit_nanos: f64 = match unit.as_str() {
            "ns" => 1.0,
            "us" | "µs" | "μs" => 1_000.0,
            "ms" => 1_000_000.0,
            "s" => 1_000_000_000.0,
            "m" => 60.0 * 1_000_000_000.0,
            "h" => 3_600.0 * 1_000_000_000.0,
            _ => return None,
        };

        total += value * unit_nanos;
        saw_unit = true;
    }

    if !saw_unit {
        return None;
    }

    Some((sign * total) as i64)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_seconds() {
        assert_eq!(parse_go_duration_ms("5.2s"), 5200);
        assert_eq!(parse_go_duration_ms("2.5s"), 2500);
    }

    #[test]
    fn test_milliseconds() {
        assert_eq!(parse_go_duration_ms("100ms"), 100);
    }

    #[test]
    fn test_compound() {
        assert_eq!(parse_go_duration_ms("1m30s"), 90_000);
        assert_eq!(parse_go_duration_ms("1h2m3s"), 3_723_000);
    }

    #[test]
    fn test_sub_millisecond() {
        assert_eq!(parse_go_duration_ms("500us"), 0);
        assert_eq!(parse_go_duration_ms("1500us"), 1);
        assert_eq!(parse_go_duration_ms("1.5µs"), 0);
    }

    #[test]
    fn test_zero_and_empty() {
        assert_eq!(parse_go_duration_ms(""), 0);
        assert_eq!(parse_go_duration_ms("0s"), 0);
        assert_eq!(parse_go_duration_ms("0"), 0);
    }

    #[test]
    fn test_malformed() {
        assert_eq!(parse_go_duration_ms("garbage"), 0);
        assert_eq!(parse_go_duration_ms("12"), 0);
    }
}
