use fake::faker::internet::en::*;
use fake::faker::lorem::en::*;
use fake::Fake;
use rand::seq::SliceRandom;
use rand::{Rng, RngCore};

/// Failure category for a span, selecting a realistic exception type/message family.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ExceptionKind {
    /// LLM-call failure (timeout, rate limit, connection).
    Llm,
    /// Tool-execution failure (bad arguments, runtime error).
    Tool,
    /// Generic workflow/task failure (I/O, timeout).
    Generic,
}

pub struct FakeDataGenerator;

impl FakeDataGenerator {
    // Test data constants
    const PROJECT_IDS: &'static [&'static str] = &["trihub-prod", "trihub-dev", "trihub-staging"];

    const SERVICES: &'static [&'static str] = &[
        "depot-service",
        "query-service",
        "bff-service",
        "collector",
        "console-server",
    ];

    const SEVERITY_LEVELS: &'static [(u32, &'static str)] = &[
        (1, "TRACE"),
        (5, "DEBUG"),
        (9, "INFO"),
        (13, "WARN"),
        (17, "ERROR"),
        (21, "FATAL"),
    ];

    const HTTP_METHODS: &'static [&'static str] =
        &["GET", "POST", "PUT", "DELETE", "PATCH", "HEAD", "OPTIONS"];

    const HTTP_STATUS_CODES: &'static [u32] = &[200, 201, 204, 400, 401, 403, 404, 500, 502, 503];

    const DEPLOYMENT_ENVIRONMENTS: &'static [&'static str] =
        &["production", "staging", "development"];

    const GEN_AI_PROVIDERS: &'static [&'static str] = &[
        "openai",
        "anthropic",
        "aws.bedrock",
        "gcp.vertex_ai",
        "cohere",
        "mistral_ai",
    ];

    const TOOL_NAMES: &'static [&'static str] = &[
        "get_weather",
        "search_web",
        "query_database",
        "send_email",
        "calculator",
    ];

    const FINISH_REASONS: &'static [&'static str] =
        &["stop", "length", "tool_calls", "content_filter"];

    const AGENT_NAMES: &'static [&'static str] = &[
        "research-agent",
        "support-agent",
        "coding-agent",
        "planner-agent",
    ];

    pub fn generate_trace_id() -> [u8; 16] {
        let mut rng = rand::thread_rng();
        loop {
            let id = rng.gen::<[u8; 16]>();
            if id != [0u8; 16] {
                return id;
            }
        }
    }

    pub fn generate_span_id() -> [u8; 8] {
        let mut rng = rand::thread_rng();
        loop {
            let id = rng.gen::<[u8; 8]>();
            if id != [0u8; 8] {
                return id;
            }
        }
    }

    pub fn generate_project_id() -> String {
        let mut rng = rand::thread_rng();
        Self::PROJECT_IDS.choose(&mut rng).unwrap().to_string()
    }

    pub fn generate_service_name() -> String {
        let mut rng = rand::thread_rng();
        Self::SERVICES.choose(&mut rng).unwrap().to_string()
    }

    pub fn generate_severity() -> (u32, String) {
        let mut rng = rand::thread_rng();
        let (num, text) = Self::SEVERITY_LEVELS.choose(&mut rng).unwrap();
        (*num, text.to_string())
    }

    pub fn generate_http_method() -> String {
        let mut rng = rand::thread_rng();
        Self::HTTP_METHODS.choose(&mut rng).unwrap().to_string()
    }

    pub fn generate_http_status_code() -> u32 {
        let mut rng = rand::thread_rng();
        *Self::HTTP_STATUS_CODES.choose(&mut rng).unwrap()
    }

    pub fn generate_deployment_environment() -> String {
        let mut rng = rand::thread_rng();
        Self::DEPLOYMENT_ENVIRONMENTS
            .choose(&mut rng)
            .unwrap()
            .to_string()
    }

    pub fn generate_host_name() -> String {
        format!(
            "{}.local",
            fake::faker::internet::en::Username().fake::<String>()
        )
    }

    pub fn generate_uuid() -> String {
        fake::uuid::UUIDv4.fake::<uuid::Uuid>().to_string()
    }

    pub fn generate_sentence() -> String {
        Sentence(5..15).fake()
    }

    pub fn generate_email() -> String {
        SafeEmail().fake()
    }

    pub fn generate_user_agent() -> String {
        UserAgent().fake()
    }

    pub fn generate_service_version() -> String {
        let mut rng = rand::thread_rng();
        format!(
            "{}.{}.{}",
            rng.gen_range(0..5),
            rng.gen_range(0..20),
            rng.gen_range(0..50)
        )
    }

    pub fn generate_k8s_namespace() -> String {
        let mut rng = rand::thread_rng();
        let namespaces = ["default", "kube-system", "monitoring", "logging", "ingress"];
        namespaces.choose(&mut rng).unwrap().to_string()
    }

    /// Generate a Kubernetes pod name in the conventional
    /// `<service>-<replicaset-hash>-<pod-hash>` form.
    ///
    /// `service` must be the shard's `service.name` so that the resulting pod identity is
    /// consistent with the rest of the shard's resource attributes (a pod always belongs to a
    /// specific service/deployment).
    pub fn generate_k8s_pod_name(service: &str) -> String {
        let mut rng = rand::thread_rng();
        format!(
            "{}-{}-{}",
            service,
            rng.gen_range(100000..999999),
            rng.gen_range(10000..99999)
        )
    }

    pub fn generate_thread_id() -> String {
        let mut rng = rand::thread_rng();
        rng.gen_range(1000..9999).to_string()
    }

    /// Random gen_ai.provider.name.
    pub fn generate_gen_ai_provider() -> String {
        let mut rng = rand::thread_rng();
        Self::GEN_AI_PROVIDERS.choose(&mut rng).unwrap().to_string()
    }

    /// Model consistent with the provider (gen_ai.request.model).
    pub fn generate_gen_ai_model(provider: &str) -> String {
        let mut rng = rand::thread_rng();
        let models: &[&str] = match provider {
            "openai" => &["gpt-4o", "gpt-4o-mini", "gpt-4.1", "o3"],
            "anthropic" => &["claude-3-5-sonnet", "claude-3-5-haiku", "claude-opus-4"],
            "aws.bedrock" => &["amazon.titan-text", "anthropic.claude-3-5-sonnet"],
            "gcp.vertex_ai" => &["gemini-1.5-pro", "gemini-2.0-flash"],
            "cohere" => &["command-r-plus", "command-r"],
            "mistral_ai" => &["mistral-large", "mistral-small"],
            _ => &["unknown-model"],
        };
        models.choose(&mut rng).unwrap().to_string()
    }

    /// Tool name (gen_ai.tool.name).
    pub fn generate_tool_name() -> String {
        let mut rng = rand::thread_rng();
        Self::TOOL_NAMES.choose(&mut rng).unwrap().to_string()
    }

    /// Finish reason (element of gen_ai.response.finish_reasons).
    pub fn generate_finish_reason() -> String {
        let mut rng = rand::thread_rng();
        Self::FINISH_REASONS.choose(&mut rng).unwrap().to_string()
    }

    /// Agent name (gen_ai.agent.name).
    pub fn generate_agent_name() -> String {
        let mut rng = rand::thread_rng();
        Self::AGENT_NAMES.choose(&mut rng).unwrap().to_string()
    }

    /// Model response identifier (gen_ai.response.id).
    pub fn generate_response_id() -> String {
        format!("chatcmpl-{}", &Self::generate_uuid()[..8])
    }

    /// Generate an exception `(type, message)` pair appropriate for the failure category.
    ///
    /// The returned `type` doubles as the `error.type` attribute value and the
    /// `exception.type` event attribute; the `message` is the human-readable failure text.
    ///
    /// # Arguments
    ///
    /// * `kind` - failure category that selects the exception family.
    pub fn generate_exception(kind: ExceptionKind) -> (String, String) {
        let mut rng = rand::thread_rng();
        let variants: &[(&str, &str)] = match kind {
            ExceptionKind::Llm => &[
                ("APITimeoutError", "Request timed out after 60s"),
                ("RateLimitError", "Rate limit exceeded, retry after 20s"),
                (
                    "APIConnectionError",
                    "Connection error while contacting provider",
                ),
            ],
            ExceptionKind::Tool => &[
                ("ValueError", "Invalid argument passed to tool"),
                ("RuntimeError", "Tool execution failed unexpectedly"),
            ],
            ExceptionKind::Generic => &[
                ("IOError", "Failed to read upstream resource"),
                ("TimeoutError", "Operation exceeded its deadline"),
            ],
        };
        let (etype, msg) = variants.choose(&mut rng).unwrap();
        (etype.to_string(), msg.to_string())
    }

    /// Generate a conversation identifier (`gen_ai.conversation.id`) of the form `conv-<8 hex>`.
    ///
    /// Draws from the supplied `rng` so a seeded generator produces a reproducible pool.
    ///
    /// # Arguments
    ///
    /// * `rng` - randomness source for the identifier bytes.
    pub fn generate_conversation_id(rng: &mut dyn RngCore) -> String {
        let bytes: [u8; 4] = rng.gen();
        format!("conv-{}", hex::encode(bytes))
    }

    /// Generate a short fake Python-style stacktrace for an `exception.stacktrace` attribute.
    pub fn generate_stacktrace(exception_type: &str) -> String {
        format!(
            "Traceback (most recent call last):\n  File \"app/agent.py\", line {}, in run\n    {}",
            rand::thread_rng().gen_range(20..400),
            exception_type
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn gen_ai_provider_and_model_consistent() {
        let provider = FakeDataGenerator::generate_gen_ai_provider();
        let model = FakeDataGenerator::generate_gen_ai_model(&provider);
        assert!(!provider.is_empty());
        assert!(!model.is_empty());
    }

    #[test]
    fn finish_reason_from_known_set() {
        let reason = FakeDataGenerator::generate_finish_reason();
        assert!(["stop", "length", "tool_calls", "content_filter"].contains(&reason.as_str()));
    }
}
