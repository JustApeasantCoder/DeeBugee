use std::{borrow::Cow, ops::Range};

use chrono::{DateTime, Duration, Utc};
use dee_bugee_schema::LogEvent;
use serde_json::Value;

#[derive(Debug, Clone, PartialEq)]
pub struct ParsedQuery {
    pub text: String,
    pub predicates: Vec<ParsedPredicate>,
    pub errors: Vec<QueryError>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct ParsedPredicate {
    pub label: String,
    pub range: Range<usize>,
    predicate: StructuredPredicate,
}

impl ParsedPredicate {
    pub(crate) fn matches(&self, event: &LogEvent) -> bool {
        self.predicate.matches(event)
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct QueryError {
    pub message: String,
    pub range: Range<usize>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum QueryOperator {
    Equal,
    NotEqual,
    Greater,
    GreaterOrEqual,
    Less,
    LessOrEqual,
}

impl QueryOperator {
    fn text(self) -> &'static str {
        match self {
            Self::Equal => "=",
            Self::NotEqual => "!=",
            Self::Greater => ">",
            Self::GreaterOrEqual => ">=",
            Self::Less => "<",
            Self::LessOrEqual => "<=",
        }
    }

    fn compare_number(self, left: f64, right: f64) -> bool {
        match self {
            Self::Equal => (left - right).abs() < f64::EPSILON,
            Self::NotEqual => (left - right).abs() >= f64::EPSILON,
            Self::Greater => left > right,
            Self::GreaterOrEqual => left >= right,
            Self::Less => left < right,
            Self::LessOrEqual => left <= right,
        }
    }

    fn compare_text(self, left: &str, right: &str) -> bool {
        match self {
            Self::Equal => left.eq_ignore_ascii_case(right),
            Self::NotEqual => !left.eq_ignore_ascii_case(right),
            Self::Greater | Self::GreaterOrEqual | Self::Less | Self::LessOrEqual => false,
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
enum QueryLiteral {
    Text(String),
    Number(f64),
}

#[derive(Debug, Clone, PartialEq)]
struct StructuredPredicate {
    field: String,
    operator: QueryOperator,
    value: QueryLiteral,
}

impl StructuredPredicate {
    fn matches(&self, event: &LogEvent) -> bool {
        let Some(value) = event_query_value(event, &self.field) else {
            return false;
        };
        match (&self.value, value) {
            (QueryLiteral::Number(right), QueryValue::Number(left)) => {
                self.operator.compare_number(left, *right)
            }
            (QueryLiteral::Text(right), QueryValue::Text(left)) => {
                self.operator.compare_text(&left, right)
            }
            (QueryLiteral::Text(right), QueryValue::Number(left)) => right
                .parse::<f64>()
                .is_ok_and(|right| self.operator.compare_number(left, right)),
            (QueryLiteral::Number(right), QueryValue::Text(left)) => left
                .parse::<f64>()
                .is_ok_and(|left| self.operator.compare_number(left, *right)),
        }
    }
}

enum QueryValue<'a> {
    Text(Cow<'a, str>),
    Number(f64),
}

#[derive(Debug)]
struct Token<'a> {
    text: &'a str,
    range: Range<usize>,
}

pub fn parse_structured_query(query: &str, now: DateTime<Utc>) -> ParsedQuery {
    let tokens = query_tokens(query);
    let mut predicates = Vec::new();
    let mut errors = Vec::new();
    let mut consumed = vec![false; tokens.len()];
    let mut index = 0;

    while index < tokens.len() {
        if index + 1 < tokens.len() && parse_operator(tokens[index + 1].text).is_some() {
            let end_index = (index + 2).min(tokens.len().saturating_sub(1));
            let range = tokens[index].range.start..tokens[end_index].range.end;
            if !is_query_field(tokens[index].text) {
                errors.push(QueryError {
                    message: format!("Unknown query field '{}'", tokens[index].text),
                    range,
                });
                consumed[index] = true;
                consumed[index + 1] = true;
                if index + 2 < tokens.len() {
                    consumed[index + 2] = true;
                    index += 3;
                } else {
                    index += 2;
                }
                continue;
            }
            if index + 2 >= tokens.len() {
                errors.push(QueryError {
                    message: format!("{} requires a value", tokens[index].text),
                    range,
                });
                consumed[index] = true;
                consumed[index + 1] = true;
                index += 2;
                continue;
            }
            let field = tokens[index].text;
            let operator_text = tokens[index + 1].text;
            let value = tokens[index + 2].text;
            let range = tokens[index].range.start..tokens[index + 2].range.end;
            match build_predicate(field, operator_text, value, range.clone(), now) {
                Ok(predicate) => predicates.push(predicate),
                Err(error) => errors.push(error),
            }
            consumed[index..=index + 2].fill(true);
            index += 3;
            continue;
        }

        if let Some((field, operator, value)) = split_inline_expression(tokens[index].text)
            && is_query_field(field)
        {
            let range = tokens[index].range.clone();
            match build_predicate(field, operator, value, range.clone(), now) {
                Ok(predicate) => predicates.push(predicate),
                Err(error) => errors.push(error),
            }
            consumed[index] = true;
        } else if looks_like_broken_expression(tokens[index].text) {
            errors.push(QueryError {
                message: format!("Unknown query field in '{}'", tokens[index].text),
                range: tokens[index].range.clone(),
            });
            consumed[index] = true;
        }
        index += 1;
    }

    let mut text = String::new();
    for (token, consumed) in tokens.iter().zip(consumed) {
        if consumed {
            continue;
        }
        if !text.is_empty() {
            text.push(' ');
        }
        text.push_str(token.text);
    }

    ParsedQuery {
        text,
        predicates,
        errors,
    }
}

fn build_predicate(
    field: &str,
    operator_text: &str,
    raw_value: &str,
    range: Range<usize>,
    now: DateTime<Utc>,
) -> Result<ParsedPredicate, QueryError> {
    let operator = parse_operator(operator_text).ok_or_else(|| QueryError {
        message: format!("Unsupported operator '{operator_text}'"),
        range: range.clone(),
    })?;
    let value = unquote(raw_value).ok_or_else(|| QueryError {
        message: "Query value has an unmatched quote".to_string(),
        range: range.clone(),
    })?;
    if value.is_empty() {
        return Err(QueryError {
            message: format!("{field} requires a value"),
            range,
        });
    }

    let (operator, literal, display_value) = if field == "timestamp" {
        if let Some(duration) = parse_relative_duration(&value) {
            let threshold = now - duration;
            (
                QueryOperator::GreaterOrEqual,
                QueryLiteral::Number(threshold.timestamp_millis() as f64),
                value,
            )
        } else {
            let timestamp = value.parse::<i64>().ok().or_else(|| {
                DateTime::parse_from_rfc3339(&value)
                    .ok()
                    .map(|timestamp| timestamp.timestamp_millis())
            });
            let Some(timestamp) = timestamp else {
                return Err(QueryError {
                    message:
                        "timestamp expects Unix milliseconds, RFC 3339, or last-<number><s|m|h|d>"
                            .to_string(),
                    range,
                });
            };
            (operator, QueryLiteral::Number(timestamp as f64), value)
        }
    } else if is_numeric_field(field)
        || matches!(
            operator,
            QueryOperator::Greater
                | QueryOperator::GreaterOrEqual
                | QueryOperator::Less
                | QueryOperator::LessOrEqual
        )
    {
        let number = value.parse::<f64>().map_err(|_| QueryError {
            message: format!("{field} expects a numeric value"),
            range: range.clone(),
        })?;
        (operator, QueryLiteral::Number(number), value)
    } else {
        (operator, QueryLiteral::Text(value.clone()), value)
    };

    Ok(ParsedPredicate {
        label: format!("{field} {} {display_value}", operator.text()),
        range,
        predicate: StructuredPredicate {
            field: field.to_ascii_lowercase(),
            operator,
            value: literal,
        },
    })
}

fn query_tokens(query: &str) -> Vec<Token<'_>> {
    let mut tokens = Vec::new();
    let mut start = None;
    let mut quote = None;
    let mut escaped = false;
    for (index, character) in query.char_indices() {
        if escaped {
            escaped = false;
            continue;
        }
        if character == '\\' && quote.is_some() {
            escaped = true;
            continue;
        }
        if matches!(character, '\'' | '"') {
            if quote == Some(character) {
                quote = None;
            } else if quote.is_none() {
                quote = Some(character);
            }
        }
        if character.is_whitespace() && quote.is_none() {
            if let Some(token_start) = start.take() {
                tokens.push(Token {
                    text: &query[token_start..index],
                    range: token_start..index,
                });
            }
        } else if start.is_none() {
            start = Some(index);
        }
    }
    if let Some(token_start) = start {
        tokens.push(Token {
            text: &query[token_start..],
            range: token_start..query.len(),
        });
    }
    tokens
}

fn split_inline_expression(token: &str) -> Option<(&str, &str, &str)> {
    for operator in [">=", "<=", "!=", "=", ">", "<", ":"] {
        let Some(index) = token.find(operator) else {
            continue;
        };
        let field = &token[..index];
        let value = &token[index + operator.len()..];
        if !field.is_empty() {
            return Some((field, operator, value));
        }
    }
    None
}

fn parse_operator(value: &str) -> Option<QueryOperator> {
    match value {
        "=" | ":" => Some(QueryOperator::Equal),
        "!=" => Some(QueryOperator::NotEqual),
        ">" => Some(QueryOperator::Greater),
        ">=" => Some(QueryOperator::GreaterOrEqual),
        "<" => Some(QueryOperator::Less),
        "<=" => Some(QueryOperator::LessOrEqual),
        _ => None,
    }
}

fn is_query_field(field: &str) -> bool {
    matches!(
        field.to_ascii_lowercase().as_str(),
        "schema_version"
            | "timestamp"
            | "level"
            | "source"
            | "tag"
            | "subsystem"
            | "target"
            | "event"
            | "message"
            | "app_session_id"
            | "playback_session_id"
            | "request_id"
            | "session_id"
            | "provider"
            | "duration_ms"
            | "error_kind"
            | "status"
            | "correlation"
    ) || field
        .strip_prefix("fields.")
        .is_some_and(|name| !name.is_empty())
}

fn is_numeric_field(field: &str) -> bool {
    matches!(field, "schema_version" | "duration_ms")
}

fn looks_like_broken_expression(token: &str) -> bool {
    split_inline_expression(token).is_some_and(|(field, operator, _)| {
        operator != ":"
            && !field.is_empty()
            && field.chars().all(|character| {
                character.is_ascii_alphanumeric() || matches!(character, '_' | '.')
            })
    })
}

fn unquote(value: &str) -> Option<String> {
    let value = value.trim();
    if value.len() >= 2
        && ((value.starts_with('"') && value.ends_with('"'))
            || (value.starts_with('\'') && value.ends_with('\'')))
    {
        return Some(
            value[1..value.len() - 1]
                .replace("\\\"", "\"")
                .replace("\\'", "'"),
        );
    }
    if value.starts_with(['"', '\'']) || value.ends_with(['"', '\'']) {
        None
    } else {
        Some(value.to_string())
    }
}

fn parse_relative_duration(value: &str) -> Option<Duration> {
    let value = value.to_ascii_lowercase();
    let suffix = value.strip_prefix("last-")?;
    let (amount, unit) = suffix.split_at(suffix.len().checked_sub(1)?);
    let amount = amount.parse::<i64>().ok().filter(|amount| *amount > 0)?;
    match unit {
        "s" => Some(Duration::seconds(amount)),
        "m" => Some(Duration::minutes(amount)),
        "h" => Some(Duration::hours(amount)),
        "d" => Some(Duration::days(amount)),
        _ => None,
    }
}

fn event_query_value<'a>(event: &'a LogEvent, field: &str) -> Option<QueryValue<'a>> {
    match field {
        "schema_version" => Some(QueryValue::Number(f64::from(event.schema_version))),
        "timestamp" => Some(QueryValue::Number(event.timestamp.timestamp_millis() as f64)),
        "level" => Some(QueryValue::Text(Cow::Owned(event.level.to_string()))),
        "source" => Some(QueryValue::Text(Cow::Borrowed(&event.source))),
        "tag" => Some(QueryValue::Text(Cow::Owned(event.tag()))),
        "subsystem" => Some(QueryValue::Text(Cow::Borrowed(&event.subsystem))),
        "target" => event
            .target
            .as_deref()
            .map(|value| QueryValue::Text(Cow::Borrowed(value))),
        "event" => Some(QueryValue::Text(Cow::Borrowed(&event.event))),
        "message" => Some(QueryValue::Text(Cow::Borrowed(&event.message))),
        "app_session_id" => Some(QueryValue::Text(Cow::Borrowed(&event.app_session_id))),
        "playback_session_id" => event
            .playback_session_id
            .as_deref()
            .map(|value| QueryValue::Text(Cow::Borrowed(value))),
        "request_id" => event
            .request_id
            .as_deref()
            .map(|value| QueryValue::Text(Cow::Borrowed(value))),
        "session_id" => event
            .session_id
            .as_deref()
            .map(|value| QueryValue::Text(Cow::Borrowed(value))),
        "provider" => event
            .provider
            .as_deref()
            .map(|value| QueryValue::Text(Cow::Borrowed(value))),
        "duration_ms" => event.duration_ms.map(QueryValue::Number),
        "error_kind" => event
            .error_kind
            .as_deref()
            .map(|value| QueryValue::Text(Cow::Borrowed(value))),
        "status" => event.status.as_ref().and_then(json_query_value),
        "correlation" => Some(QueryValue::Text(Cow::Borrowed(event.correlation_id()))),
        _ => field
            .strip_prefix("fields.")
            .and_then(|name| event.fields.get(name))
            .and_then(json_query_value),
    }
}

fn json_query_value(value: &Value) -> Option<QueryValue<'_>> {
    match value {
        Value::String(value) => Some(QueryValue::Text(Cow::Borrowed(value))),
        Value::Number(value) => value.as_f64().map(QueryValue::Number),
        Value::Bool(value) => Some(QueryValue::Text(Cow::Borrowed(if *value {
            "true"
        } else {
            "false"
        }))),
        Value::Null | Value::Array(_) | Value::Object(_) => None,
    }
}

#[cfg(test)]
mod tests {
    use chrono::TimeZone;

    use super::*;

    fn now() -> DateTime<Utc> {
        Utc.with_ymd_and_hms(2026, 8, 22, 12, 0, 0)
            .single()
            .unwrap()
    }

    #[test]
    fn parses_mixed_fuzzy_exact_numeric_and_relative_terms() {
        let parsed = parse_structured_query(
            "sync provider=remote duration_ms > 1000 timestamp:last-5m status != completed",
            now(),
        );

        assert_eq!(parsed.text, "sync");
        assert!(parsed.errors.is_empty());
        assert_eq!(parsed.predicates.len(), 4);
        assert_eq!(parsed.predicates[0].label, "provider = remote");
        assert_eq!(parsed.predicates[1].label, "duration_ms > 1000");
        assert_eq!(parsed.predicates[2].label, "timestamp >= last-5m");
        assert_eq!(parsed.predicates[3].label, "status != completed");
    }

    #[test]
    fn quoted_values_can_include_spaces() {
        let parsed = parse_structured_query("event = 'sync completed' local", now());
        assert_eq!(parsed.text, "local");
        assert_eq!(parsed.predicates[0].label, "event = sync completed");
    }

    #[test]
    fn malformed_structured_terms_are_reported_and_removed_from_fuzzy_text() {
        let parsed = parse_structured_query(
            "duration_ms > slow duraton_ms>10 unknown_field = 4 ordinary",
            now(),
        );
        assert_eq!(parsed.text, "ordinary");
        assert_eq!(parsed.errors.len(), 3);
        assert!(parsed.errors[0].message.contains("numeric"));
        assert!(parsed.errors[1].message.contains("Unknown"));
        assert!(parsed.errors[2].message.contains("Unknown"));
    }
}
