//! Vendor-agnostic authentication headers applied to every HTTP/gRPC request.
//!
//! [`AuthHeaders`] is the parsing/validation core. It is independent of any transport and any
//! tenant routing: the result is a static list of `(name, value)` pairs that each transport
//! attaches to every outgoing request. Charset validation (valid HTTP header / ASCII gRPC
//! metadata) is deferred to the transports, which know their own constraints.

use crate::error::{GeneratorError, Result};
use base64::Engine;

/// A set of static auth headers, built from raw `key=value` pairs plus optional Bearer/Basic
/// shortcuts. Vendor-agnostic: no header name is special-cased except the conflict check on
/// `Authorization` between the shortcuts and the raw headers.
#[derive(Clone, Debug, Default)]
pub struct AuthHeaders {
    headers: Vec<(String, String)>,
}

impl AuthHeaders {
    /// Build the header set from configuration.
    ///
    /// * `raw` — `key=value,key2=value2` (same shape as the cardinality-limits spec: entries are
    ///   trimmed, empties skipped, split on the first `=`). The value may contain `=`.
    /// * `bearer` — shortcut producing `Authorization: Bearer <token>`.
    /// * `basic` — `user:pass` shortcut producing `Authorization: Basic <base64(user:pass)>`.
    ///
    /// `bearer` and `basic` are mutually exclusive. A shortcut that would set `Authorization`
    /// conflicts (case-insensitively) with an `Authorization` provided via `raw`; this is an
    /// error rather than a silent overwrite.
    #[allow(clippy::result_large_err)]
    pub fn build(raw: &str, bearer: Option<&str>, basic: Option<&str>) -> Result<Self> {
        // Normalize present-but-empty shortcuts to None: clap reads an unset-default env var as
        // `Some("")`, not `None`, so `OTEL_AUTH_BEARER=` (and `--auth-bearer ""`) must not trigger
        // the mutual-exclusion check nor emit an empty `Authorization`. Mirrors how raw pairs skip
        // empties below.
        let bearer = bearer.filter(|s| !s.trim().is_empty());
        let basic = basic.filter(|s| !s.trim().is_empty());

        let mut headers: Vec<(String, String)> = Vec::new();

        // 1. Raw "key=value,key2=value2" pairs.
        let trimmed = raw.trim();
        if !trimmed.is_empty() {
            for pair in trimmed.split(',') {
                let pair = pair.trim();
                if pair.is_empty() {
                    continue;
                }

                let (key, value) = pair.split_once('=').ok_or_else(|| {
                    GeneratorError::InvalidConfiguration(format!(
                        "invalid auth header pair '{pair}', expected key=value"
                    ))
                })?;

                let key = key.trim();
                if key.is_empty() {
                    return Err(GeneratorError::InvalidConfiguration(
                        "auth header key must not be empty".to_string(),
                    ));
                }

                headers.push((key.to_string(), value.trim().to_string()));
            }
        }

        // 2. Bearer and Basic are mutually exclusive.
        if bearer.is_some() && basic.is_some() {
            return Err(GeneratorError::InvalidConfiguration(
                "auth_bearer and auth_basic are mutually exclusive".to_string(),
            ));
        }

        // 3. Resolve the shortcut into an Authorization value.
        let shortcut = if let Some(token) = bearer {
            Some(format!("Bearer {token}"))
        } else {
            basic.map(|creds| {
                let encoded = base64::engine::general_purpose::STANDARD.encode(creds.as_bytes());
                format!("Basic {encoded}")
            })
        };

        // 4. A shortcut Authorization must not collide with a raw Authorization header.
        if let Some(value) = shortcut {
            if headers
                .iter()
                .any(|(key, _)| key.eq_ignore_ascii_case("authorization"))
            {
                return Err(GeneratorError::InvalidConfiguration(
                    "Authorization header set both via raw headers and auth_bearer/auth_basic"
                        .to_string(),
                ));
            }
            headers.push(("Authorization".to_string(), value));
        }

        Ok(Self { headers })
    }

    /// Iterate the `(name, value)` pairs in insertion order.
    pub fn iter(&self) -> impl Iterator<Item = (&str, &str)> {
        self.headers
            .iter()
            .map(|(key, value)| (key.as_str(), value.as_str()))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn collect(auth: &AuthHeaders) -> Vec<(String, String)> {
        auth.iter()
            .map(|(k, v)| (k.to_string(), v.to_string()))
            .collect()
    }

    #[test]
    fn parses_raw_pairs() {
        let auth =
            AuthHeaders::build("x-api-key=secret, x-bt-parent=project:foo", None, None).unwrap();
        let pairs = collect(&auth);
        assert_eq!(
            pairs,
            vec![
                ("x-api-key".to_string(), "secret".to_string()),
                ("x-bt-parent".to_string(), "project:foo".to_string()),
            ]
        );
    }

    #[test]
    fn raw_value_may_contain_equals() {
        let auth = AuthHeaders::build("token=ab==", None, None).unwrap();
        assert_eq!(
            collect(&auth),
            vec![("token".to_string(), "ab==".to_string())]
        );
    }

    #[test]
    fn bearer_builds_authorization() {
        let auth = AuthHeaders::build("", Some("xyz"), None).unwrap();
        assert_eq!(
            collect(&auth),
            vec![("Authorization".to_string(), "Bearer xyz".to_string())]
        );
    }

    #[test]
    fn basic_is_base64_of_user_pass() {
        let auth = AuthHeaders::build("", None, Some("user:pass")).unwrap();
        // base64("user:pass") == "dXNlcjpwYXNz"
        assert_eq!(
            collect(&auth),
            vec![(
                "Authorization".to_string(),
                "Basic dXNlcjpwYXNz".to_string()
            )]
        );
    }

    #[test]
    fn bearer_and_basic_conflict() {
        let err = AuthHeaders::build("", Some("xyz"), Some("user:pass"))
            .expect_err("bearer + basic must fail");
        assert!(matches!(err, GeneratorError::InvalidConfiguration(_)));
    }

    #[test]
    fn shortcut_conflicts_with_raw_authorization() {
        let err = AuthHeaders::build("authorization=Bearer raw", Some("xyz"), None)
            .expect_err("duplicate Authorization must fail");
        assert!(matches!(err, GeneratorError::InvalidConfiguration(_)));
    }

    #[test]
    fn empty_key_is_rejected() {
        let err = AuthHeaders::build("=value", None, None).expect_err("empty key must fail");
        assert!(matches!(err, GeneratorError::InvalidConfiguration(_)));
    }

    #[test]
    fn missing_equals_is_rejected() {
        let err = AuthHeaders::build("novalue", None, None).expect_err("missing '=' must fail");
        assert!(matches!(err, GeneratorError::InvalidConfiguration(_)));
    }

    #[test]
    fn empty_yields_empty() {
        let auth = AuthHeaders::build("  ", None, None).unwrap();
        assert_eq!(auth.iter().count(), 0);
    }

    #[test]
    fn empty_shortcuts_are_not_mutually_exclusive() {
        // clap surfaces `OTEL_AUTH_BEARER=`/`OTEL_AUTH_BASIC=` as `Some("")`. Both empty must be a
        // no-op, never the mutual-exclusion error, and must not emit an empty `Authorization`.
        let auth = AuthHeaders::build("", Some(""), Some("  ")).unwrap();
        assert_eq!(auth.iter().count(), 0);
    }

    #[test]
    fn empty_bearer_emits_no_authorization() {
        let auth = AuthHeaders::build("", Some(""), None).unwrap();
        assert_eq!(auth.iter().count(), 0);
    }
}
