use bluedb_fts::mapping::{Analyzer, IndexMapping};
use bluedb_fts::search::{
    multi_split_count_query_filtered, multi_split_search_query_filtered_ids, SplitHandle,
};
use bluedb_fts::tombstones::Tombstones;
use bluedb_fts::IdField;
use tantivy::query::{BooleanQuery, Occur, Query, TermQuery};
use tantivy::schema::IndexRecordOption;
use tantivy::schema::Term;
use tantivy::{Index, TantivyDocument};

#[test]
fn query_object_search_filtered_ids() {
    let mapping = IndexMapping::new().keyword("_id").text("body", Analyzer::Default);
    let schema = mapping.build_schema();
    let id_f = schema.get_field("_id").unwrap();
    let body_f = schema.get_field("body").unwrap();

    let index = Index::create_in_ram(schema.clone());
    mapping.register_tokenizers(&index);
    let mut writer = index.writer(15_000_000).unwrap();
    let mut d1 = TantivyDocument::default();
    d1.add_text(id_f, "a");
    d1.add_text(body_f, "good dogs");
    writer.add_document(d1).unwrap();
    let mut d2 = TantivyDocument::default();
    d2.add_text(id_f, "b");
    d2.add_text(body_f, "lazy cats");
    writer.add_document(d2).unwrap();
    writer.commit().unwrap();

    let handles = vec![SplitHandle::with_generation("s1", 0, &index)];
    let tombstones = Tombstones::new("idx");

    let query: Box<dyn Query> = Box::new(BooleanQuery::new(vec![(
        Occur::Should,
        Box::new(TermQuery::new(
            Term::from_field_text(body_f, "dogs"),
            IndexRecordOption::WithFreqs,
        )) as Box<dyn Query>,
    )]));

    let ids = multi_split_search_query_filtered_ids(
        &handles, query.as_ref(), 10, IdField(id_f), &tombstones,
    )
    .unwrap();
    assert_eq!(ids.len(), 1);
    assert_eq!(ids[0].0, "a");

    let count =
        multi_split_count_query_filtered(&handles, query.as_ref(), IdField(id_f), &tombstones).unwrap();
    assert_eq!(count, 1);
}
