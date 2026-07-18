// Criterion benchmarks for SchemaRegistry register/validate paths.
// Run: cargo bench --bench schema
use criterion::{criterion_group, criterion_main, BenchmarkId, Criterion, Throughput};
use pheno_events::schema::SchemaRegistry;
use serde_json::json;

fn bench_register(c: &mut Criterion) {
    let schema = json!({
        "type": "object",
        "required": ["id"],
        "properties": {
            "id": {"type": "integer"},
            "name": {"type": "string"}
        }
    });

    c.bench_function("schema_register", |b| {
        b.iter(|| {
            let mut reg = SchemaRegistry::new();
            reg.register("bench.event".into(), 1, schema.clone())
                .expect("register");
        });
    });
}

fn bench_validate(c: &mut Criterion) {
    let schema = json!({
        "type": "object",
        "required": ["id"],
        "properties": {
            "id": {"type": "integer"}
        }
    });
    let payload = json!({"id": 42});

    let mut group = c.benchmark_group("schema_validate");
    for n in [1u64, 10, 100] {
        group.throughput(Throughput::Elements(n));
        group.bench_with_input(BenchmarkId::from_parameter(n), &n, |b, &n| {
            let mut reg = SchemaRegistry::new();
            reg.register("bench.event".into(), 1, schema.clone())
                .expect("register");
            b.iter(|| {
                for _ in 0..n {
                    reg.validate("bench.event".into(), 1, &payload)
                        .expect("validate");
                }
            });
        });
    }
    group.finish();
}

criterion_group!(benches, bench_register, bench_validate);
criterion_main!(benches);
