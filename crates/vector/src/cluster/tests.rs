use serde_json::{Value, json};

use super::*;
use common::storage::config::{LocalObjectStoreConfig, ObjectStoreConfig, SlateDbStorageConfig};
use tempfile::TempDir;

async fn cluster() -> VectorCluster {
    VectorCluster::open(ClusterConfig::default()).await.unwrap()
}

fn write_request(body: Value) -> WriteRequest {
    serde_json::from_value(body).unwrap()
}

fn query_request(body: Value) -> QueryRequest {
    serde_json::from_value(body).unwrap()
}

fn ids(response: &QueryResponse) -> Vec<String> {
    response
        .rows
        .as_ref()
        .unwrap()
        .iter()
        .map(|row| row["id"].as_str().unwrap().to_string())
        .collect()
}

async fn query_ids(cluster: &VectorCluster, ns: &str, body: Value) -> Vec<String> {
    ids(&cluster.query(ns, query_request(body)).await.unwrap())
}

/// Rows `r0..r{n}` with 2-d vectors and a few attributes.
fn rows(n: usize) -> Value {
    Value::Array(
        (0..n)
            .map(|i| {
                json!({
                    "id": format!("r{i}"),
                    "vector": [1.0, i as f32],
                    "n": i,
                    "parity": if i % 2 == 0 { "even" } else { "odd" },
                    "tags": [format!("t{}", i % 3), "all"],
                })
            })
            .collect(),
    )
}

#[tokio::test]
async fn should_isolate_namespaces_sharing_a_shard() {
    // given
    let cluster = cluster().await;
    cluster
        .write(
            "a",
            write_request(json!({"upsert_rows": [{"id": "x", "vector": [1, 0], "who": "a"}]})),
        )
        .await
        .unwrap();
    cluster
        .write(
            "b",
            write_request(json!({"upsert_rows": [{"id": "x", "vector": [0, 1], "who": "b"}]})),
        )
        .await
        .unwrap();

    // when
    let a = cluster.get_row("a", "x").await.unwrap().unwrap();
    let b = cluster.get_row("b", "x").await.unwrap().unwrap();

    // then
    assert_eq!(a["who"], "a");
    assert_eq!(b["who"], "b");
    assert_eq!(a["vector"], json!([1.0, 0.0]));
    let names: Vec<_> = cluster
        .list_namespaces()
        .await
        .unwrap()
        .into_iter()
        .map(|n| n.name)
        .collect();
    assert_eq!(names, vec!["a", "b"]);
    assert_eq!(cluster.namespace("a").await.unwrap().shard, 0);
    assert!(matches!(
        cluster.query("missing", QueryRequest::default()).await,
        Err(Error::NotFound(_))
    ));
}

#[tokio::test]
async fn should_infer_schema_and_evaluate_bool_filters() {
    // given
    let cluster = cluster().await;
    cluster
        .write("ns", write_request(json!({"upsert_rows": rows(10)})))
        .await
        .unwrap();
    let meta = cluster.namespace("ns").await.unwrap();
    assert_eq!(meta.dimensions, 2);
    assert_eq!(meta.schema["n"].attribute_type, AttributeType::Int);
    assert_eq!(
        meta.schema["tags"].attribute_type,
        AttributeType::StringArray
    );

    // then
    let cases = [
        (
            json!({"op": "Lt", "attribute": "n", "value": 3}),
            vec!["r0", "r1", "r2"],
        ),
        (
            json!({"op": "And", "filters": [
                {"op": "Gte", "attribute": "n", "value": 2},
                {"op": "Lte", "attribute": "n", "value": 4}
            ]}),
            vec!["r2", "r3", "r4"],
        ),
        (
            json!({"op": "And", "filters": [
                {"op": "Eq", "attribute": "parity", "value": "odd"},
                {"op": "Not", "filter": {"op": "Gt", "attribute": "n", "value": 5}}
            ]}),
            vec!["r1", "r3", "r5"],
        ),
        (
            json!({"op": "Or", "filters": [
                {"op": "Eq", "attribute": "n", "value": 0},
                {"op": "In", "attribute": "n", "values": [8, 9]}
            ]}),
            vec!["r0", "r8", "r9"],
        ),
        (
            json!({"op": "Contains", "attribute": "tags", "value": "t1"}),
            vec!["r1", "r4", "r7"],
        ),
        (
            json!({"op": "And", "filters": [
                {"op": "ContainsAny", "attribute": "tags", "values": ["t0", "t1"]},
                {"op": "NotIn", "attribute": "n", "values": [0, 1, 3, 4, 6]}
            ]}),
            vec!["r7", "r9"],
        ),
        (
            json!({"op": "Glob", "attribute": "parity", "pattern": "ev*"}),
            vec!["r0", "r2", "r4", "r6", "r8"],
        ),
        (
            json!({"op": "IGlob", "attribute": "parity", "pattern": "ODD"}),
            vec!["r1", "r3", "r5", "r7", "r9"],
        ),
        (json!({"op": "Exists", "attribute": "missing"}), vec![]),
        (json!({"op": "NotExists", "attribute": "tags"}), vec![]),
    ];
    for (filter, expected) in cases {
        let got = query_ids(&cluster, "ns", json!({"filters": filter, "limit": 100})).await;
        assert_eq!(got, expected, "filter {filter}");
    }
}

#[tokio::test]
async fn should_rank_by_vector_attribute_and_aggregate() {
    // given
    let cluster = cluster().await;
    cluster
        .write(
            "ns",
            write_request(json!({"upsert_rows": rows(6), "distance_metric": "euclidean_squared"})),
        )
        .await
        .unwrap();

    // when
    let ann = cluster
        .query(
            "ns",
            query_request(json!({"rank_by": {"op": "ANN", "vector": [1.0, 4.2]}, "limit": 2})),
        )
        .await
        .unwrap();
    let knn = query_ids(
        &cluster,
        "ns",
        json!({
            "rank_by": {"op": "kNN", "vector": [1.0, 0.0]},
            "filters": {"op": "Eq", "attribute": "parity", "value": "odd"},
            "limit": 2
        }),
    )
    .await;
    let desc = query_ids(
        &cluster,
        "ns",
        json!({"rank_by": {"op": "Order", "attribute": "n", "direction": "desc"}, "limit": 3}),
    )
    .await;
    let counted = cluster
        .query(
            "ns",
            query_request(json!({
                "aggregate_by": {"odd": {"op": "Count"}},
                "filters": {"op": "Eq", "attribute": "parity", "value": "odd"}
            })),
        )
        .await
        .unwrap();

    // then
    assert_eq!(ids(&ann), vec!["r4", "r5"]);
    let first = &ann.rows.as_ref().unwrap()[0];
    assert!((first["$dist"].as_f64().unwrap() - 0.04).abs() < 1e-4);
    assert!(first.get("vector").is_none());
    assert_eq!(first["parity"], "even");
    assert_eq!(knn, vec!["r1", "r3"]);
    assert_eq!(desc, vec!["r5", "r4", "r3"]);
    assert!(counted.rows.is_none());
    assert_eq!(counted.aggregations.unwrap()["odd"], 3);
}

#[tokio::test]
async fn should_score_knn_like_ann_on_unnormalized_cosine_vectors() {
    // given
    let cluster = cluster().await;
    cluster
        .write(
            "ns",
            write_request(json!({"upsert_rows": rows(6), "distance_metric": "cosine_distance"})),
        )
        .await
        .unwrap();

    // when
    let query = |rank| {
        cluster.query(
            "ns",
            query_request(json!({"rank_by": {"op": rank, "vector": [2.0, 6.0]}, "limit": 3})),
        )
    };
    let knn = query("kNN").await.unwrap();
    let ann = query("ANN").await.unwrap();

    // then
    assert_eq!(ids(&knn), vec!["r3", "r4", "r5"]);
    assert_eq!(ids(&knn), ids(&ann));
    let dists = |response: &QueryResponse| -> Vec<f64> {
        response
            .rows
            .as_ref()
            .unwrap()
            .iter()
            .map(|row| row["$dist"].as_f64().unwrap())
            .collect()
    };
    assert!(dists(&knn)[0].abs() < 1e-5);
    for (k, a) in dists(&knn).iter().zip(dists(&ann)) {
        assert!((k - a).abs() < 1e-5, "kNN {k} vs ANN {a}");
    }
}

#[tokio::test]
async fn should_project_attributes() {
    let cluster = cluster().await;
    cluster
        .write("ns", write_request(json!({"upsert_rows": rows(1)})))
        .await
        .unwrap();

    let only = cluster
        .query(
            "ns",
            query_request(json!({"include_attributes": ["n", "vector"]})),
        )
        .await
        .unwrap();
    let none = cluster
        .query("ns", query_request(json!({"include_attributes": false})))
        .await
        .unwrap();
    let excluded = cluster
        .query("ns", query_request(json!({"exclude_attributes": ["tags"]})))
        .await
        .unwrap();

    let only = &only.rows.unwrap()[0];
    assert_eq!(only.len(), 3);
    assert_eq!(only["vector"], json!([1.0, 0.0]));
    assert_eq!(none.rows.unwrap()[0].len(), 1);
    let excluded = &excluded.rows.unwrap()[0];
    assert!(excluded.get("tags").is_none());
    assert!(excluded.get("parity").is_some());
}

#[tokio::test]
async fn should_patch_and_delete_rows() {
    // given
    let cluster = cluster().await;
    cluster
        .write("ns", write_request(json!({"upsert_rows": rows(6)})))
        .await
        .unwrap();

    // when
    let response = cluster
        .write(
            "ns",
            write_request(json!({
                "delete_by_filter": {"op": "Gte", "attribute": "n", "value": 4},
                "patch_rows": [
                    {"id": "r0", "parity": null, "color": "red"},
                    {"id": "nope", "color": "blue"},
                ],
                "deletes": ["r1"],
            })),
        )
        .await
        .unwrap();

    // then
    assert_eq!(response.rows_deleted, 3);
    assert_eq!(response.rows_patched, 1);
    assert_eq!(
        query_ids(&cluster, "ns", json!({})).await,
        vec!["r0", "r2", "r3"]
    );
    let r0 = cluster.get_row("ns", "r0").await.unwrap().unwrap();
    assert_eq!(r0["color"], "red");
    assert!(r0.get("parity").is_none());
    assert_eq!(r0["n"], 0);
    assert_eq!(
        query_ids(
            &cluster,
            "ns",
            json!({"filters": {"op": "Eq", "attribute": "color", "value": "red"}})
        )
        .await,
        vec!["r0"]
    );
}

#[tokio::test]
async fn should_store_documents_as_passages_and_collapse_them() {
    // given
    let cluster = cluster().await;
    let docs = json!({
        "upsert_documents": [
            {"id": "d1", "attributes": {"kind": "guide"}, "passages": [
                {"vector": [1.0, 0.0], "text": "a"},
                {"vector": [1.0, 0.1], "text": "b"},
                {"vector": [1.0, 0.2], "text": "c"},
            ]},
            {"id": "d2", "attributes": {"kind": "faq"}, "passages": [
                {"vector": [1.0, 0.3]},
                {"vector": [1.0, 0.4]},
            ]},
        ]
    });
    let response = cluster.write("ns", write_request(docs)).await.unwrap();
    assert_eq!(response.documents_upserted, 2);
    assert_eq!(response.rows_upserted, 5);

    // when
    let collapsed = cluster
        .query(
            "ns",
            query_request(json!({
                "rank_by": {"op": "kNN", "vector": [1.0, 0.0]},
                "limit": {"total": 10, "per": {"attributes": ["$doc_id"], "limit": 1}},
            })),
        )
        .await
        .unwrap();

    // then
    assert_eq!(ids(&collapsed), vec!["d1#0", "d2#0"]);
    let first = &collapsed.rows.as_ref().unwrap()[0];
    assert_eq!(first["$doc_id"], "d1");
    assert_eq!(first["$passage"], 0);
    assert_eq!(first["kind"], "guide");

    // re-upserting with fewer passages drops the stale ones
    cluster
        .write(
            "ns",
            write_request(
                json!({"upsert_documents": [{"id": "d1", "passages": [{"vector": [0.0, 1.0]}]}]}),
            ),
        )
        .await
        .unwrap();
    assert_eq!(
        query_ids(
            &cluster,
            "ns",
            json!({"filters": {"op": "Eq", "attribute": "$doc_id", "value": "d1"}})
        )
        .await,
        vec!["d1#0"]
    );

    // deleting a document removes all of its passages
    let deleted = cluster
        .write("ns", write_request(json!({"delete_documents": ["d2"]})))
        .await
        .unwrap();
    assert_eq!(deleted.rows_deleted, 2);
    assert_eq!(query_ids(&cluster, "ns", json!({})).await, vec!["d1#0"]);
}

#[tokio::test]
async fn should_search_full_text_and_fuse_with_rrf() {
    // given
    let cluster = cluster().await;
    cluster
        .create_namespace(
            "ns",
            CreateNamespaceRequest {
                dimensions: Some(2),
                distance_metric: Metric::Cosine,
                schema: BTreeMap::from([(
                    "body".to_string(),
                    AttributeSchema {
                        attribute_type: AttributeType::String,
                        full_text_search: true,
                        embed: false,
                    },
                )]),
            },
        )
        .await
        .unwrap();
    cluster
        .write(
            "ns",
            write_request(json!({"upsert_rows": [
                {"id": "a", "vector": [1, 0], "body": "the quick brown fox"},
                {"id": "b", "vector": [0, 1], "body": "a lazy dog sleeps"},
                {"id": "c", "vector": [1, 1], "body": "quick quick fox jumps"},
            ]})),
        )
        .await
        .unwrap();

    // when
    let text = query_ids(
        &cluster,
        "ns",
        json!({"rank_by": {"op": "BM25", "attribute": "body", "query": "quick fox"}, "limit": 10}),
    )
    .await;
    let fused: MultiQueryRequest = serde_json::from_value(json!({
        "queries": [
            {"rank_by": {"op": "BM25", "attribute": "body", "query": "dog"}, "limit": 3},
            {"rank_by": {"op": "kNN", "vector": [0, 1]}, "limit": 3},
        ],
        "rrf": {"limit": 2},
    }))
    .unwrap();
    let fused = cluster.multi_query("ns", fused).await.unwrap();

    // then
    assert_eq!(text.len(), 2);
    assert!(!text.contains(&"b".to_string()));
    let MultiQueryResponse::Fused(fused) = fused else {
        panic!("expected fused results");
    };
    assert_eq!(ids(&fused)[0], "b");
    assert!(fused.rows.unwrap()[0]["$score"].as_f64().unwrap() > 0.0);
}

#[tokio::test]
async fn should_reject_invalid_writes_without_side_effects() {
    let cluster = cluster().await;
    cluster
        .write("ns", write_request(json!({"upsert_rows": rows(2)})))
        .await
        .unwrap();

    let bad_type = cluster
        .write(
            "ns",
            write_request(json!({"deletes": ["r0"], "upsert_rows": [{"id": "z", "vector": [1, 2], "n": "x"}]})),
        )
        .await;
    let no_vector = cluster
        .write("other", write_request(json!({"deletes": ["r0"]})))
        .await;

    assert!(matches!(bad_type, Err(Error::InvalidInput(_))));
    assert!(matches!(no_vector, Err(Error::NotFound(_))));
    assert_eq!(query_ids(&cluster, "ns", json!({})).await, vec!["r0", "r1"]);
    assert!(validate_namespace_name("bad/name").is_err());
}

#[tokio::test]
async fn should_delete_namespace_data() {
    let cluster = cluster().await;
    cluster
        .write("ns", write_request(json!({"upsert_rows": rows(3)})))
        .await
        .unwrap();

    cluster.delete_namespace("ns").await.unwrap();

    assert!(cluster.list_namespaces().await.unwrap().is_empty());
    cluster
        .write(
            "ns",
            write_request(json!({"upsert_rows": [{"id": "new", "vector": [0, 1, 2]}]})),
        )
        .await
        .unwrap();
    assert_eq!(cluster.namespace("ns").await.unwrap().dimensions, 3);
    assert_eq!(query_ids(&cluster, "ns", json!({})).await, vec!["new"]);
}

fn as_vector(value: &Value) -> Vec<f32> {
    serde_json::from_value(value.clone()).unwrap()
}

#[tokio::test]
async fn should_embed_rows_and_query_texts_with_the_namespace_model() {
    // given
    let model = embedding::tests::FakeModel::new(4);
    let embedder = Arc::new(Embedder::with_model(model.clone(), 1 << 20, 16));
    let cluster = cluster().await.with_embedder(embedder);

    // when: the first write creates the namespace from the model's dimensions
    cluster
        .write(
            "docs",
            write_request(json!({
                "schema": {"body": {"type": "string", "embed": true}},
                "upsert_rows": [
                    {"id": "a", "body": "alpha"},
                    {"id": "b", "body": "beta"},
                    {"id": "c", "body": "gamma", "vector": [1, 1, 1, 1]},
                ],
            })),
        )
        .await
        .unwrap();

    // then
    let meta = cluster.namespace("docs").await.unwrap();
    assert_eq!(meta.dimensions, 4);
    assert_eq!(meta.embedding_model.as_deref(), Some("fake:4"));
    let a = cluster.get_row("docs", "a").await.unwrap().unwrap();
    assert_eq!(as_vector(&a["vector"]), model.vector("alpha"));
    let c = cluster.get_row("docs", "c").await.unwrap().unwrap();
    assert_eq!(
        as_vector(&c["vector"]),
        vec![1.0; 4],
        "an explicit vector wins"
    );
    let nearest = query_ids(
        &cluster,
        "docs",
        json!({"rank_by": {"op": "kNN", "text": "beta"}, "top_k": 1}),
    )
    .await;
    assert_eq!(nearest, vec!["b"]);
    let by_vector = query_ids(
        &cluster,
        "docs",
        json!({"rank_by": {"op": "kNN", "vector": [1, 1, 1, 1]}, "top_k": 1}),
    )
    .await;
    assert_eq!(by_vector, vec!["c"], "query vectors still work");

    // when: a patch changes the embedded text
    cluster
        .write(
            "docs",
            write_request(json!({"patch_rows": [{"id": "a", "body": "beta"}]})),
        )
        .await
        .unwrap();

    // then: the row is re-embedded, from the cache
    let a = cluster.get_row("docs", "a").await.unwrap().unwrap();
    assert_eq!(as_vector(&a["vector"]), model.vector("beta"));
    assert_eq!(
        model.texts(),
        vec!["dimensions", "alpha", "beta", "beta"],
        "the probe, each document text once, then the query text"
    );
}

#[tokio::test]
async fn should_reject_embedding_without_a_model_or_text() {
    let unconfigured = cluster().await;
    let create = CreateNamespaceRequest {
        dimensions: Some(4),
        distance_metric: Metric::Cosine,
        schema: serde_json::from_value(json!({"body": {"type": "string", "embed": true}})).unwrap(),
    };
    let err = unconfigured
        .create_namespace("docs", create.clone())
        .await
        .unwrap_err();
    assert!(err.to_string().contains("no embedding model"), "{err}");

    let cluster = cluster().await.with_embedder(Arc::new(Embedder::with_model(
        embedding::tests::FakeModel::new(4),
        0,
        16,
    )));
    let wrong_size = CreateNamespaceRequest {
        dimensions: Some(3),
        ..create.clone()
    };
    let err = cluster
        .create_namespace("docs", wrong_size)
        .await
        .unwrap_err();
    assert!(err.to_string().contains("4-dimensional"), "{err}");
    cluster.create_namespace("docs", create).await.unwrap();

    let err = cluster
        .write(
            "docs",
            write_request(json!({"upsert_rows": [{"id": "a", "title": "x"}]})),
        )
        .await
        .unwrap_err();
    assert!(
        err.to_string().contains("needs a 'vector' or a 'body'"),
        "{err}"
    );
    let second =
        serde_json::from_value(json!({"title": {"type": "string", "embed": true}})).unwrap();
    let err = cluster.update_schema("docs", &second).await.unwrap_err();
    assert!(err.to_string().contains("only one attribute"), "{err}");

    // A namespace pinned to a model the server no longer has still takes
    // explicit vectors.
    let changed = cluster.with_embedder(Arc::new(Embedder::with_model(
        embedding::tests::FakeModel::new(5),
        0,
        16,
    )));
    changed
        .write(
            "docs",
            write_request(
                json!({"upsert_rows": [{"id": "a", "body": "x", "vector": [1, 0, 0, 0]}]}),
            ),
        )
        .await
        .unwrap();
    let nearest = query_ids(
        &changed,
        "docs",
        json!({"rank_by": {"op": "kNN", "vector": [1, 0, 0, 0]}}),
    )
    .await;
    assert_eq!(nearest, vec!["a"]);
    let err = changed
        .query(
            "docs",
            query_request(json!({"rank_by": {"op": "kNN", "text": "x"}})),
        )
        .await
        .unwrap_err();
    assert!(
        err.to_string().contains("configured model is fake:5"),
        "{err}"
    );
    let cluster = changed;

    cluster
        .write("plain", write_request(json!({"upsert_rows": rows(2)})))
        .await
        .unwrap();
    let err = cluster
        .query(
            "plain",
            query_request(json!({"rank_by": {"op": "ANN", "text": "x"}})),
        )
        .await
        .unwrap_err();
    assert!(err.to_string().contains("doesn't embed"), "{err}");
}

fn declared(schemas: Value) -> DeclaredSchemas {
    serde_json::from_value(schemas).unwrap()
}

#[tokio::test]
async fn should_create_namespaces_with_their_declared_schema() {
    // given
    let cluster = cluster().await;
    cluster.declare_schemas(declared(json!({
        "docs": {"n": {"type": "float"}, "parity": {"type": "string", "full_text_search": true}},
    })));

    // when
    for ns in ["docs", "plain"] {
        cluster
            .write(ns, write_request(json!({"upsert_rows": rows(3)})))
            .await
            .unwrap();
    }

    // then
    let docs = cluster.namespace("docs").await.unwrap();
    assert_eq!(docs.schema["n"].attribute_type, AttributeType::Float);
    assert!(docs.schema["parity"].full_text_search);
    let plain = cluster.namespace("plain").await.unwrap();
    assert_eq!(plain.schema["n"].attribute_type, AttributeType::Int);
    assert!(!plain.schema["parity"].full_text_search);
}

#[tokio::test]
async fn should_merge_declared_schemas_into_existing_namespaces() {
    // given
    let cluster = cluster().await;
    cluster
        .write("docs", write_request(json!({"upsert_rows": rows(3)})))
        .await
        .unwrap();
    cluster.declare_schemas(declared(json!({
        "docs": {"parity": {"type": "string", "full_text_search": true}, "year": {"type": "int"}},
        "missing": {"year": {"type": "int"}},
    })));

    // when
    let failed = cluster.apply_declared_schemas().await;

    // then
    assert!(failed.is_empty(), "{failed:?}");
    let docs = cluster.namespace("docs").await.unwrap();
    assert!(docs.schema["parity"].full_text_search);
    assert_eq!(docs.schema["year"].attribute_type, AttributeType::Int);
    assert_eq!(docs.schema["n"].attribute_type, AttributeType::Int);
    assert!(matches!(
        cluster.namespace("missing").await,
        Err(Error::NotFound(_))
    ));
    let ids = query_ids(
        &cluster,
        "docs",
        json!({"rank_by": {"op": "BM25", "attribute": "parity", "query": "even"}}),
    )
    .await;
    assert!(
        ids.is_empty(),
        "rows written before full-text search aren't indexed"
    );
}

#[tokio::test]
async fn should_apply_newly_declared_schemas_in_the_background() {
    // given: an applier that would otherwise wait an hour between passes
    let cluster = Arc::new(
        VectorCluster::open(ClusterConfig {
            reconcile_interval: Duration::from_secs(3600),
            ..ClusterConfig::default()
        })
        .await
        .unwrap(),
    );
    cluster
        .write("docs", write_request(json!({"upsert_rows": rows(3)})))
        .await
        .unwrap();
    let applier = cluster.spawn_schema_applier();

    // when
    cluster.declare_schemas(declared(json!({"docs": {"year": {"type": "int"}}})));

    // then
    tokio::time::timeout(Duration::from_secs(5), async {
        while !cluster
            .namespace("docs")
            .await
            .unwrap()
            .schema
            .contains_key("year")
        {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("the declared schema was not applied");
    applier.abort();
}

#[tokio::test]
async fn should_report_declared_schemas_a_namespace_cannot_take() {
    // given
    let cluster = cluster().await;
    cluster
        .write("docs", write_request(json!({"upsert_rows": rows(3)})))
        .await
        .unwrap();
    let before = cluster.namespace("docs").await.unwrap();
    cluster.declare_schemas(declared(json!({"docs": {"n": {"type": "string"}}})));

    // when
    let failed = cluster.apply_declared_schemas().await;

    // then
    assert_eq!(failed.len(), 1);
    assert_eq!(failed[0].0, "docs");
    assert!(
        failed[0].1.to_string().contains("cannot change"),
        "{}",
        failed[0].1
    );
    assert_eq!(cluster.namespace("docs").await.unwrap(), before);
}

#[tokio::test]
async fn should_create_a_namespace_from_text_when_its_declared_schema_embeds() {
    // given
    let cluster = cluster().await.with_embedder(Arc::new(Embedder::with_model(
        embedding::tests::FakeModel::new(4),
        0,
        16,
    )));
    cluster.declare_schemas(declared(
        json!({"docs": {"body": {"type": "string", "embed": true}}}),
    ));

    // when
    cluster
        .write(
            "docs",
            write_request(json!({"upsert_rows": [{"id": "a", "body": "hello"}]})),
        )
        .await
        .unwrap();

    // then
    let docs = cluster.namespace("docs").await.unwrap();
    assert_eq!(docs.dimensions, 4);
    assert!(docs.schema["body"].embed);
}

async fn populate(cluster: &VectorCluster, count: usize) -> Vec<String> {
    let names: Vec<String> = (0..count).map(|i| format!("tenant-{i}")).collect();
    for name in &names {
        cluster
            .write(
                name,
                write_request(
                    json!({"upsert_rows": rows(5), "schema": {"label": {"type": "string"}}}),
                ),
            )
            .await
            .unwrap();
        cluster
            .write(
                name,
                write_request(json!({"patch_rows": [{"id": "r0", "label": name}]})),
            )
            .await
            .unwrap();
    }
    names
}

async fn assert_namespaces_intact(cluster: &VectorCluster, names: &[String]) {
    for name in names {
        assert_eq!(
            query_ids(
                cluster,
                name,
                json!({"filters": {"op": "Lt", "attribute": "n", "value": 2}})
            )
            .await,
            vec!["r0", "r1"],
            "namespace {name}"
        );
        let hit = query_ids(
            cluster,
            name,
            json!({"rank_by": {"op": "ANN", "vector": [1.0, 3.0]}, "limit": 1}),
        )
        .await;
        assert_eq!(hit, vec!["r3"], "namespace {name}");
        assert_eq!(
            cluster.get_row(name, "r0").await.unwrap().unwrap()["label"],
            Value::String(name.clone())
        );
    }
}

#[tokio::test]
async fn should_keep_a_standalone_cluster_on_one_shard() {
    // given
    let dir = TempDir::new().unwrap();
    let cluster = VectorCluster::open(local_config(&dir)).await.unwrap();
    let names = populate(&cluster, 4).await;

    // when
    cluster.close().await.unwrap();
    let cluster = VectorCluster::open(local_config(&dir)).await.unwrap();

    // then
    let shards = cluster.list_shards().await.unwrap();
    assert_eq!(shards.len(), 1);
    assert_eq!(shards[0].meta.owner.as_deref(), Some(STANDALONE_IDENTITY));
    let mut listed = shards[0].namespaces.clone();
    listed.sort();
    assert_eq!(listed, names);
    assert_namespaces_intact(&cluster, &names).await;
    cluster.close().await.unwrap();
}

fn local_config(dir: &TempDir) -> ClusterConfig {
    ClusterConfig {
        storage: StorageConfig::SlateDb(SlateDbStorageConfig {
            path: "vectors".to_string(),
            object_store: ObjectStoreConfig::Local(LocalObjectStoreConfig {
                path: dir.path().to_string_lossy().to_string(),
            }),
            settings_path: None,
            block_cache: None,
            meta_cache: None,
            disk: Default::default(),
        }),
        ..ClusterConfig::default()
    }
}

struct Replicas {
    dir: TempDir,
    store: Arc<MemoryTopologyStore>,
    leases: Arc<MemoryLeases>,
}

impl Replicas {
    fn new() -> Self {
        Self {
            dir: TempDir::new().unwrap(),
            store: MemoryTopologyStore::new(),
            leases: MemoryLeases::new(),
        }
    }

    async fn open(&self, identity: &str, role: Role) -> VectorCluster {
        let config = ClusterConfig {
            role,
            ..local_config(&self.dir)
        };
        let runtime = ClusterRuntime {
            identity: identity.into(),
            topology: self.store.clone(),
            leases: self.leases.clone(),
        };
        VectorCluster::open_with(config, runtime).await.unwrap()
    }

    /// Plays the coordinator: plans splits for the writers of `ready`.
    async fn plan(&self, ready: &[&str]) {
        let current = self.store.load().await.unwrap().unwrap();
        let ready: Vec<String> = ready.iter().map(|m| m.to_string()).collect();
        if let Some(next) = current.plan(&ready) {
            assert!(
                self.store
                    .publish(Some(current.generation), &next)
                    .await
                    .unwrap()
            );
        }
    }
}

/// The replica of `replicas` that `need`s place `name` locally.
async fn serving<'a>(replicas: &[&'a VectorCluster], name: &str, need: Need) -> &'a VectorCluster {
    for replica in replicas {
        if replica.place(name, need).await.unwrap() == Placement::Local {
            return replica;
        }
    }
    panic!("no replica serves '{name}' for {need:?}");
}

/// Every namespace's rows, ranked by vector, from the writer that owns it.
async fn snapshot(writers: &[&VectorCluster], names: &[String]) -> Vec<Vec<String>> {
    let mut out = Vec::new();
    for name in names {
        let writer = serving(writers, name, Need::Write).await;
        out.push(
            query_ids(
                writer,
                name,
                json!({"rank_by": {"op": "kNN", "vector": [1.0, 2.0]}}),
            )
            .await,
        );
    }
    out
}

/// Waits until `reader` answers like the writers did in `expected`.
async fn await_reader(reader: &VectorCluster, names: &[String], expected: &[Vec<String>]) {
    let mut last = Vec::new();
    for _ in 0..60 {
        reader.reconcile().await.unwrap();
        last.clear();
        for name in names {
            let request = query_request(json!({
                "rank_by": {"op": "kNN", "vector": [1.0, 2.0]},
                "consistency": {"level": "eventual"},
            }));
            last.push(match reader.query(name, request).await {
                Ok(response) => ids(&response),
                Err(_) => Vec::new(),
            });
        }
        if last == expected {
            return;
        }
        tokio::time::sleep(Duration::from_millis(250)).await;
    }
    panic!("reader answered {last:?}, writers {expected:?}");
}

#[tokio::test]
async fn should_split_a_shard_onto_a_joining_writer_and_follow_on_readers() {
    // given - one writer owning the only shard, and a reader of every shard
    let replicas = Replicas::new();
    replicas
        .store
        .publish(None, &Topology::initial(&["w0".into()], 0))
        .await
        .unwrap();
    let w0 = replicas.open("w0", Role::Writer).await;
    let names = populate(&w0, 8).await;
    w0.flush().await.unwrap();
    let expected = snapshot(&[&w0], &names).await;
    let reader = replicas.open("r", Role::Reader).await;

    // then - the reader serves eventual reads and defers row reads, and no
    // replica of a sharded cluster serves strong queries
    await_reader(&reader, &names, &expected).await;
    assert!(matches!(
        reader.place(&names[0], Need::StrongRead).await.unwrap(),
        Placement::Remote { owner: Some(owner), .. } if owner == "w0"
    ));
    let strong = json!({"consistency": {"level": "strong"}});
    for replica in [&w0, &reader] {
        assert!(matches!(
            replica
                .query(&names[0], query_request(strong.clone()))
                .await,
            Err(Error::InvalidInput(_))
        ));
    }

    // when - a second writer joins, the coordinator plans a split for it,
    // and the owner splits its shard
    let w1 = replicas.open("w1", Role::Writer).await;
    replicas.plan(&["w0", "w1"]).await;
    w0.reconcile().await.unwrap();
    w1.reconcile().await.unwrap();

    // then - the shard split in two, one child per writer, each holding
    // half the namespaces, and nothing changed
    let topology = replicas.store.load().await.unwrap().unwrap();
    let parent = topology.shard(0).unwrap();
    assert_eq!(parent.state, ShardState::Retired);
    assert_eq!(parent.children, vec![1, 2]);
    assert_eq!(topology.owned_by("w0").unwrap().id, 1);
    assert_eq!(topology.owned_by("w1").unwrap().id, 2);
    assert!(topology.plan(&["w0".into(), "w1".into()]).is_none());
    let mut moved = 0;
    for name in &names {
        let w0_writes = w0.place(name, Need::Write).await.unwrap() == Placement::Local;
        let w1_writes = w1.place(name, Need::Write).await.unwrap() == Placement::Local;
        assert!(w0_writes ^ w1_writes, "namespace {name}");
        moved += usize::from(w1_writes);
    }
    assert_eq!(moved, names.len() / 2);
    assert_eq!(snapshot(&[&w0, &w1], &names).await, expected);
    await_reader(&reader, &names, &expected).await;

    // and - writes after the split land on the children
    let writer = serving(&[&w0, &w1], &names[0], Need::Write).await;
    writer
        .write(&names[0], write_request(json!({"deletes": ["r4"]})))
        .await
        .unwrap();
    assert_eq!(
        writer.get_row(&names[0], "r4").await.unwrap(),
        None,
        "row reads see writes at once"
    );
    writer.flush().await.unwrap();
    assert_eq!(
        query_ids(writer, &names[0], json!({})).await,
        vec!["r0", "r1", "r2", "r3"]
    );
    let expected = snapshot(&[&w0, &w1], &names).await;

    // and - the children reopen from their clones
    for replica in [&reader, &w0, &w1] {
        replica.close().await.unwrap();
    }
    let w0 = replicas.open("w0", Role::Writer).await;
    let w1 = replicas.open("w1", Role::Writer).await;
    assert_eq!(snapshot(&[&w0, &w1], &names).await, expected);
    for replica in [&w0, &w1] {
        replica.close().await.unwrap();
    }
}
