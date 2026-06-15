use bluedb_evidence::{EntryInput, Evidence};
mod harness;

fn e(t: &str) -> EntryInput {
    EntryInput { etype: t.into(), payload: t.as_bytes().to_vec(), at: String::new(), edges: vec![] }
}

#[tokio::test]
async fn read_range_roundtrips_in_numeric_order_across_digit_boundary() {
    let db = harness::memory_db().await;
    let ev = Evidence::new(&db, "_");
    for i in 1..=1000 {
        ev.append("c", vec![e(&i.to_string())], None).await.unwrap();
    }
    let all = ev.read_range("c", 1, ev.head("c").await.unwrap()).await.unwrap();
    assert_eq!(all.len(), 1000);
    assert_eq!(all[8].1.payload, b"9");
    assert_eq!(all[9].1.payload, b"10");
    assert_eq!(all[998].1.payload, b"999");
    assert_eq!(all[999].1.payload, b"1000");
    assert_eq!(all.first().unwrap().0, 1);
    assert_eq!(all.last().unwrap().0, 1000);
}

#[tokio::test]
async fn read_range_hi_lt_lo_is_empty() {
    let db = harness::memory_db().await;
    let ev = Evidence::new(&db, "_");
    assert!(ev.read_range("c", 1, 0).await.unwrap().is_empty());
}

#[tokio::test]
async fn read_from_paging_reproduces_full_range() {
    let db = harness::memory_db().await;
    let ev = Evidence::new(&db, "_");
    for _ in 0..250 {
        ev.append("c", vec![e("x")], None).await.unwrap();
    }
    let head = ev.head("c").await.unwrap();
    let mut paged = Vec::new();
    let mut after = 0;
    loop {
        let page = ev.read_from("c", after, Some(100)).await.unwrap();
        if page.is_empty() {
            break;
        }
        after = page.last().unwrap().0;
        paged.extend(page);
    }
    let full = ev.read_range("c", 1, head).await.unwrap();
    assert_eq!(
        paged.iter().map(|(s, _)| *s).collect::<Vec<_>>(),
        full.iter().map(|(s, _)| *s).collect::<Vec<_>>()
    );
    assert_eq!(paged.len(), 250);
}
