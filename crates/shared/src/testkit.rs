use crate::LaserFactory;
use std::time::Duration;
use testcontainers_modules::testcontainers::core::{ContainerPort, Healthcheck, WaitFor};
use testcontainers_modules::testcontainers::runners::AsyncRunner;
use testcontainers_modules::testcontainers::{ContainerAsync, GenericImage, ImageExt};

const IGGY_IMAGE: &str = "docker.io/laserdatainc/iggy-server";
const IGGY_TAG_ENV: &str = "LASER_TEST_IGGY_TAG";
const DEFAULT_TAG: &str = "latest";
const TCP_PORT: u16 = 8090;

pub struct TestIggy {
    container: ContainerAsync<GenericImage>,
    tcp_port: u16,
}

impl TestIggy {
    pub async fn start() -> Self {
        let tag = std::env::var(IGGY_TAG_ENV).unwrap_or_else(|_| DEFAULT_TAG.to_owned());
        let image = GenericImage::new(IGGY_IMAGE, tag.as_str())
            .with_exposed_port(ContainerPort::Tcp(TCP_PORT))
            .with_wait_for(WaitFor::healthcheck())
            .with_cap_add("SYS_NICE")
            .with_security_opt("seccomp=unconfined")
            .with_ulimit("memlock", -1, Some(-1))
            .with_health_check(
                Healthcheck::cmd(["/usr/local/bin/iggy-healthcheck"])
                    .with_interval(Duration::from_secs(2))
                    .with_timeout(Duration::from_secs(5))
                    .with_retries(30),
            )
            .with_env_var("IGGY_ROOT_USERNAME", "iggy")
            .with_env_var("IGGY_ROOT_PASSWORD", "laser")
            .with_env_var("IGGY_TCP_ENABLED", "true")
            .with_env_var("IGGY_TCP_ADDRESS", "0.0.0.0:8090")
            .with_env_var("IGGY_HTTP_ENABLED", "true")
            .with_env_var("IGGY_HTTP_ADDRESS", "0.0.0.0:3000")
            .with_env_var("IGGY_QUIC_ENABLED", "false")
            .with_env_var("IGGY_WEBSOCKET_ENABLED", "false")
            .with_env_var("IGGY_PLANE_ENABLED", "false")
            .with_env_var("IGGY_SYSTEM_PATH", "/tmp/iggy");
        let container = image
            .start()
            .await
            .expect("failed to start the iggy container");
        let tcp_port = container
            .get_host_port_ipv4(TCP_PORT)
            .await
            .expect("failed to resolve the iggy tcp port");
        Self {
            container,
            tcp_port,
        }
    }

    pub fn connection_string(&self) -> String {
        format!("iggy://iggy:laser@127.0.0.1:{}", self.tcp_port)
    }

    pub fn container_id(&self) -> &str {
        self.container.id()
    }

    pub fn factory(&self) -> LaserFactory {
        LaserFactory::from_connection_string(self.connection_string())
    }
}
