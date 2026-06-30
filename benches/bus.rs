// Criterion benchmarks for SqliteBus publish/subscribe throughput.
// Run: cargo bench --bench bus
use criterion::{criterion_group, criterion_main, BenchmarkId, Criterion, Throughput};
use pheno_events::{bus::{Bus, SqliteBus}, core::EventEnvelope};
use serde_json::json;
use sqlx::SqlitePool;

fn rt() -> tokio::runtime::Runtime {
    tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("runtime")
}

fn make_pool(rt: &tokio::runtime::Runtime) -> SqlitePool {
    rt.block_on(async { SqlitePool::connect("sqlite::memory:").await.expect("pool") })
}

fn make_bus(rt: &tokio::runtime::Runtime, pool: SqlitePool) -> SqliteBus {
    rt.block_on(async { SqliteBus::new(pool).await.expect("bus") })
}

fn envelope(i: u64) -> EventEnvelope {
    EventEnvelope::builder("bench.event", "bench", json!({"seq": i}))
        .build()
        .expect("envelope")
}

fn bench_publish(c: &mut Criterion) {
    let mut group = c.benchmark_group("publish");

    for batch in [1u64, 10, 100] {
        group.throughput(Throughput::Elements(batch));
        group.bench_with_input(BenchmarkId::from_parameter(batch), &batch, |b, &batch| {
            let rt = rt();
            let pool = make_pool(&rt);
            let bus = make_bus(&rt, pool);
            b.to_async(&rt).iter(|| async {
                for i in 0..batch {
                    bus.publish(envelope(i)).await.expect("publish");
                }
            });
        });
    }
    group.finish();
}

criterion_group!(benches, bench_publish);
criterion_main!(benches);
