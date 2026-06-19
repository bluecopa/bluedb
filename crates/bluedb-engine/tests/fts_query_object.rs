use std::sync::Arc;

use bluedb_engine::FtsIndex;
use bluedb_fts::mapping::{Analyzer, IndexMapping};
use bluedb_fts::policy::CompactionPolicy;
use bluedb_fts::IdField;
use bluedb_storage::SlateDbBlobStore;
use slatedb::object_store::memory::InMemory;
use slatedb::Db;
use tantivy::query::{BooleanQuery, Occur, Query, TermQuery};
use tantivy::schema::{IndexRecordOption, Term};
use tantivy::TantivyDocument;

async fn in_mem_blob() -> Arc<SlateDbBlobStore> {
    let db = Db::open("fts-query-object", Arc::new(InMemory::new()))
        .await
        .expect("open slatedb");
    Arc::new(SlateDbBlobStore::new(Arc::new(db)))
}

#[tokio::test]
async fn search_query_ids_roundtrip() {
    let mapping = IndexMapping::new()
        .keyword("_id")
        .text("body", Analyzer::Default);
    let schema = mapping.build_schema();
    let id_f = schema.get_field("_id").unwrap();
    let body_f = schema.get_field("body").unwrap();

    let blob = in_mem_blob().await;
    let idx = FtsIndex::new(
        "search/_/docs",
        blob,
        schema,
        IdField(id_f),
        CompactionPolicy::default(),
    );

    let mut d = TantivyDocument::default();
    d.add_text(id_f, "x1");
    d.add_text(body_f, "hello search world");
    idx.append([d]).await.unwrap();

    let q: Box<dyn Query> = Box::new(BooleanQuery::new(vec![(
        Occur::Should,
        Box::new(TermQuery::new(
            Term::from_field_text(body_f, "search"),
            IndexRecordOption::WithFreqs,
        )) as Box<dyn Query>,
    )]));
    let ids = idx.search_query_ids(q.as_ref(), 10).await.unwrap();
    assert_eq!(ids.len(), 1);
    assert_eq!(ids[0].0, "x1");
    let n = idx.count_query(q.as_ref()).await.unwrap();
    assert_eq!(n, 1);
}
