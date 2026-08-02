up:
    docker compose up -d --wait

down:
    docker compose down

down-clean:
    docker compose down -v

demo features="":
    LASER_ADVERSARY=${LASER_ADVERSARY:-random} LASER_ADVERSARY_RATE=${LASER_ADVERSARY_RATE:-4} LASER_LLM_SKEW=${LASER_LLM_SKEW:-100} LASER_GOVERNOR=${LASER_GOVERNOR:-enforce} LASER_CONCURRENCY=${LASER_CONCURRENCY:-1} LASER_SESSION_INTERVAL_MS=${LASER_SESSION_INTERVAL_MS:-900} cargo run -p photon-demo {{ if features == "" { "" } else { "--features " + features } }} -- live

demo-once features="":
    LASER_ADVERSARY=${LASER_ADVERSARY:-scripted} LASER_SESSION_INTERVAL_MS=${LASER_SESSION_INTERVAL_MS:-150} cargo run -p photon-demo {{ if features == "" { "" } else { "--features " + features } }} -- finite

run-calm:
    LASER_ADVERSARY=off LASER_LLM_SKEW=0 LASER_GOVERNOR=observe LASER_CONCURRENCY=${LASER_CONCURRENCY:-1} LASER_SESSION_INTERVAL_MS=${LASER_SESSION_INTERVAL_MS:-900} cargo run -p photon-demo -- live

lint:
    cargo fmt --all -- --check
    cargo sort --workspace --check
    cargo machete --with-metadata
    cargo clippy --workspace --all-targets --all-features -- -D warnings

test:
    cargo test --workspace

doctest:
    cargo test --workspace --all-features --doc

test-it:
    cargo test -p photon-shared --features integration

e2e:
    cargo test -p photon-demo --features e2e

ci: lint test doctest test-it e2e
