use fake::faker::internet::en::*;
use fake::faker::lorem::en::*;
use fake::Fake;
use rand::seq::SliceRandom;
use rand::Rng;

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

    pub fn generate_trace_id() -> [u8; 16] {
        rand::thread_rng().gen::<[u8; 16]>()
    }

    pub fn generate_span_id() -> [u8; 8] {
        rand::thread_rng().gen::<[u8; 8]>()
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
}
