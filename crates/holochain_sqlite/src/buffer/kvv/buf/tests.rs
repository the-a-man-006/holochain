use crate::buffer::kvv::KvvBufUsed;
use crate::buffer::kvv::KvvOp;
use crate::buffer::kvv::ValuesDelta;
use crate::buffer::BufferedStore;
use crate::db::ReadManager;
use crate::db::WriteManager;
use crate::error::DatabaseError;
use crate::error::DatabaseResult;
use crate::test_utils::test_cell_env;
use crate::test_utils::DbString;
use crate::transaction::Readable;
use serde_derive::Deserialize;
use serde_derive::Serialize;
use std::collections::BTreeMap;

#[derive(Clone, Debug, Hash, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
struct V(pub u32);

type Store = KvvBufUsed<DbString, V>;

fn test_buf(
    a: &BTreeMap<DbString, ValuesDelta<V>>,
    b: impl Iterator<Item = (DbString, Vec<(V, KvvOp)>)>,
) {
    for (k, v) in b {
        let val = a.get(&k).expect("Missing key");
        test_get(&val.deltas, v.into_iter());
    }
}

fn test_persisted<R: Readable>(r: &mut R, a: &Store, b: impl Iterator<Item = (DbString, Vec<V>)>) {
    for (k, v) in b {
        assert_eq!(collect_sorted(a.get_persisted(r, &k)), Ok(v));
    }
}

fn test_get(a: &BTreeMap<V, KvvOp>, b: impl Iterator<Item = (V, KvvOp)>) {
    for (k, v) in b {
        let val = a.get(&k).expect("Missing key");
        assert_eq!(*val, v);
    }
}

fn collect_sorted<T: Ord, E, I: IntoIterator<Item = Result<T, E>>>(
    iter: Result<I, E>,
) -> Result<Vec<T>, E> {
    let mut vec = iter?.into_iter().collect::<Result<Vec<_>, _>>()?;
    vec.sort_unstable();
    Ok(vec)
}

#[tokio::test(flavor = "multi_thread")]
async fn kvvbuf_basics() {
    let test_env = test_cell_env();
    let arc = test_env.env();
    let mut env = arc.guard();

    let multi_store = env.open_multi("kvv").unwrap();

    arc.guard()
        .with_reader::<DatabaseError, _, _>(|mut reader| {
            let mut store: Store = Store::new(multi_store.clone());
            assert_eq!(
                store
                    .get(&mut reader, DbString::from("key"))
                    .unwrap()
                    .collect::<Vec<_>>(),
                []
            );

            store.delete("key".into(), V(0));
            assert_eq!(
                store
                    .get(&mut reader, DbString::from("key"))
                    .unwrap()
                    .collect::<Vec<_>>(),
                []
            );

            store.insert("key".into(), V(0));
            assert_eq!(
                store
                    .get(&mut reader, DbString::from("key"))
                    .unwrap()
                    .collect::<Vec<_>>(),
                [Ok(V(0))]
            );

            arc.guard()
                .with_commit(|mut writer| store.flush_to_txn(&mut writer))
                .unwrap();

            Ok(())
        })
        .unwrap();

    let multi_store = env.open_multi("kvv").unwrap();

    arc.guard()
        .with_reader::<DatabaseError, _, _>(|mut reader| {
            let mut store: Store = Store::new(multi_store.clone());
            assert_eq!(
                store
                    .get(&mut reader, DbString::from("key"))
                    .unwrap()
                    .collect::<Vec<_>>(),
                [Ok(V(0))]
            );

            store.insert("key".into(), V(0));
            assert_eq!(
                store
                    .get(&mut reader, DbString::from("key"))
                    .unwrap()
                    .collect::<Vec<_>>(),
                [Ok(V(0))]
            );

            store.delete("key".into(), V(0));
            assert_eq!(
                store
                    .get(&mut reader, DbString::from("key"))
                    .unwrap()
                    .collect::<Vec<_>>(),
                []
            );

            arc.guard()
                .with_commit(|mut writer| store.flush_to_txn(&mut writer))
                .unwrap();

            Ok(())
        })
        .unwrap();

    let multi_store = env.open_multi("kvv").unwrap();

    arc.guard()
        .with_reader::<DatabaseError, _, _>(|mut reader| {
            let store: Store = Store::new(multi_store.clone());
            assert_eq!(
                store
                    .get(&mut reader, DbString::from("key"))
                    .unwrap()
                    .collect::<Vec<_>>(),
                []
            );
            Ok(())
        })
        .unwrap();
}

#[tokio::test(flavor = "multi_thread")]
async fn delete_all() {
    observability::test_run().ok();
    let test_env = test_cell_env();
    let arc = test_env.env();
    let mut env = arc.guard();

    let multi_store = env.open_multi("kvv").unwrap();

    arc.guard()
        .with_reader::<DatabaseError, _, _>(|mut reader| {
            let mut store: Store = Store::new(multi_store.clone());
            assert_eq!(
                store
                    .get(&mut reader, DbString::from("key"))
                    .unwrap()
                    .collect::<Vec<_>>(),
                []
            );

            store.insert("key".into(), V(0));
            assert_eq!(
                store
                    .get(&mut reader, DbString::from("key"))
                    .unwrap()
                    .collect::<Vec<_>>(),
                [Ok(V(0))]
            );

            store.insert("key".into(), V(1));
            assert_eq!(
                collect_sorted(store.get(&mut reader, DbString::from("key"))),
                Ok(vec![V(0), V(1)])
            );

            arc.guard()
                .with_commit(|mut writer| store.flush_to_txn(&mut writer))
                .unwrap();

            Ok(())
        })
        .unwrap();

    let multi_store = env.open_multi("kvv").unwrap();

    arc.guard()
        .with_reader::<DatabaseError, _, _>(|mut reader| {
            let mut store: Store = Store::new(multi_store.clone());
            assert_eq!(
                collect_sorted(store.get(&mut reader, DbString::from("key"))),
                Ok(vec![V(0), V(1)])
            );

            store.insert("key".into(), V(2));
            assert_eq!(
                collect_sorted(store.get(&mut reader, DbString::from("key"))),
                Ok(vec![V(0), V(1), V(2)])
            );

            store.delete_all("key".into());
            assert_eq!(
                store
                    .get(&mut reader, DbString::from("key"))
                    .unwrap()
                    .collect::<Vec<_>>(),
                []
            );

            store.insert("key".into(), V(3));
            assert_eq!(
                store
                    .get(&mut reader, DbString::from("key"))
                    .unwrap()
                    .collect::<Vec<_>>(),
                [Ok(V(3))]
            );

            arc.guard()
                .with_commit(|mut writer| store.flush_to_txn(&mut writer))
                .unwrap();

            Ok(())
        })
        .unwrap();

    let multi_store = env.open_multi("kvv").unwrap();

    arc.guard()
        .with_reader::<DatabaseError, _, _>(|mut reader| {
            let store: Store = Store::new(multi_store.clone());
            assert_eq!(
                store
                    .get(&mut reader, DbString::from("key"))
                    .unwrap()
                    .collect::<Vec<_>>(),
                [Ok(V(3))]
            );
            Ok(())
        })
        .unwrap();
}

/// make sure that even if there are unsorted items both
/// before and after our idempotent operation
/// both in the actual persistence and in our scratch
/// that duplicates are not returned on get
#[tokio::test(flavor = "multi_thread")]
async fn idempotent_inserts() {
    let test_env = test_cell_env();
    let arc = test_env.env();
    let mut env = arc.guard();

    let multi_store = env.open_multi("kvv").unwrap();

    arc.guard()
        .with_reader::<DatabaseError, _, _>(|mut reader| {
            let mut store: Store = Store::new(multi_store.clone());
            assert_eq!(
                collect_sorted(store.get(&mut reader, DbString::from("key"))),
                Ok(vec![])
            );

            store.insert("key".into(), V(2));
            assert_eq!(
                collect_sorted(store.get(&mut reader, DbString::from("key"))),
                Ok(vec![V(2)])
            );

            store.insert("key".into(), V(1));
            assert_eq!(
                collect_sorted(store.get(&mut reader, DbString::from("key"))),
                Ok(vec![V(1), V(2)])
            );

            store.insert("key".into(), V(1));
            assert_eq!(
                collect_sorted(store.get(&mut reader, DbString::from("key"))),
                Ok(vec![V(1), V(2)])
            );

            store.insert("key".into(), V(0));
            assert_eq!(
                collect_sorted(store.get(&mut reader, DbString::from("key"))),
                Ok(vec![V(0), V(1), V(2)])
            );

            arc.guard()
                .with_commit(|mut writer| store.flush_to_txn(&mut writer))
                .unwrap();

            Ok(())
        })
        .unwrap();

    arc.guard()
        .with_reader::<DatabaseError, _, _>(|mut reader| {
            let mut store: Store = Store::new(multi_store.clone());
            assert_eq!(
                collect_sorted(store.get(&mut reader, DbString::from("key"))),
                Ok(vec![V(0), V(1), V(2)])
            );

            store.insert("key".into(), V(1));
            assert_eq!(
                collect_sorted(store.get(&mut reader, DbString::from("key"))),
                Ok(vec![V(0), V(1), V(2)])
            );

            Ok(())
        })
        .unwrap();
}

#[tokio::test(flavor = "multi_thread")]
async fn kvv_indicate_value_appends() -> DatabaseResult<()> {
    observability::test_run().ok();
    let test_env = test_cell_env();
    let arc = test_env.env();
    let mut env = arc.guard();
    let db = env.open_multi("kvv")?;
    arc.guard().with_reader(|mut reader| {
        let mut buf = Store::new(db.clone());

        buf.insert("a".into(), V(1));
        assert_eq!(
            buf.get(&mut reader, DbString::from("a"))?.next().unwrap()?,
            V(1)
        );
        buf.insert("a".into(), V(2));
        assert_eq!(
            collect_sorted(buf.get(&mut reader, DbString::from("a"))),
            Ok(vec![V(1), V(2)])
        );
        Ok(())
    })
}

#[tokio::test(flavor = "multi_thread")]
async fn kvv_indicate_value_overwritten() -> DatabaseResult<()> {
    observability::test_run().ok();
    let test_env = test_cell_env();
    let arc = test_env.env();
    let mut env = arc.guard();
    let db = env.open_multi("kvv")?;
    arc.guard().with_reader(|mut reader| {
        let mut buf = Store::new(db.clone());

        buf.insert("a".into(), V(1));
        assert_eq!(
            buf.get(&mut reader, DbString::from("a"))?.next().unwrap()?,
            V(1)
        );
        buf.delete_all("a".into());
        buf.insert("a".into(), V(2));
        assert_eq!(
            buf.get(&mut reader, DbString::from("a"))?.next().unwrap()?,
            V(2)
        );
        buf.delete("a".into(), V(2));
        buf.insert("a".into(), V(3));
        assert_eq!(
            buf.get(&mut reader, DbString::from("a"))?.next().unwrap()?,
            V(3)
        );
        Ok(())
    })
}

#[tokio::test(flavor = "multi_thread")]
async fn kvv_deleted_persisted() -> DatabaseResult<()> {
    observability::test_run().ok();
    let test_env = test_cell_env();
    let arc = test_env.env();
    let mut env = arc.guard();
    let db = env.open_multi("kv")?;

    {
        let mut buf = Store::new(db.clone());

        buf.insert("a".into(), V(1));
        buf.insert("b".into(), V(2));
        buf.insert("c".into(), V(3));

        arc.guard()
            .with_commit(|mut writer| buf.flush_to_txn(&mut writer))?;
    }
    {
        let mut buf: KvvBufUsed<_, V> = Store::new(db.clone());

        buf.delete("b".into(), V(2));

        arc.guard()
            .with_commit(|mut writer| buf.flush_to_txn(&mut writer))?;
    }
    arc.guard().with_reader(|mut reader| {
        let buf: KvvBufUsed<DbString, _> = Store::new(db.clone());
        test_persisted(
            &mut reader,
            &buf,
            [("a".into(), vec![V(1)]), ("c".into(), vec![V(3)])]
                .iter()
                .cloned(),
        );
        Ok(())
    })
}

#[tokio::test(flavor = "multi_thread")]
async fn kvv_deleted_buffer() -> DatabaseResult<()> {
    use KvvOp::*;
    observability::test_run().ok();
    let test_env = test_cell_env();
    let arc = test_env.env();
    let mut env = arc.guard();
    let db = env.open_multi("kv")?;

    {
        let mut buf = Store::new(db.clone());

        buf.insert("a".into(), V(5));
        buf.insert("b".into(), V(4));
        buf.insert("c".into(), V(9));
        test_buf(
            &buf.scratch,
            [
                ("a".into(), vec![(V(5), Insert)]),
                ("b".into(), vec![(V(4), Insert)]),
                ("c".into(), vec![(V(9), Insert)]),
            ]
            .iter()
            .cloned(),
        );
        buf.delete("b".into(), V(4));
        test_buf(
            &buf.scratch,
            [
                ("a".into(), vec![(V(5), Insert)]),
                ("c".into(), vec![(V(9), Insert)]),
                ("b".into(), vec![(V(4), Delete)]),
            ]
            .iter()
            .cloned(),
        );

        arc.guard()
            .with_commit(|mut writer| buf.flush_to_txn(&mut writer))?;
    }
    arc.guard().with_reader(|mut reader| {
        let buf: KvvBufUsed<DbString, _> = Store::new(db.clone());
        test_persisted(
            &mut reader,
            &buf,
            [("a".into(), vec![V(5)]), ("c".into(), vec![V(9)])]
                .iter()
                .cloned(),
        );
        Ok(())
    })
}

#[tokio::test(flavor = "multi_thread")]
async fn kvv_get_buffer() -> DatabaseResult<()> {
    observability::test_run().ok();
    let test_env = test_cell_env();
    let arc = test_env.env();
    let mut env = arc.guard();
    let db = env.open_multi("kv")?;

    arc.guard().with_reader(|mut reader| {
        let mut buf = Store::new(db.clone());

        buf.insert("a".into(), V(5));
        buf.insert("b".into(), V(4));
        buf.insert("c".into(), V(9));
        let mut n = buf.get(&mut reader, DbString::from("b"))?;
        assert_eq!(n.next(), Some(Ok(V(4))));

        Ok(())
    })
}

#[tokio::test(flavor = "multi_thread")]
async fn kvv_get_persisted() -> DatabaseResult<()> {
    observability::test_run().ok();
    let test_env = test_cell_env();
    let arc = test_env.env();
    let mut env = arc.guard();
    let db = env.open_multi("kv")?;

    {
        let mut buf = Store::new(db.clone());

        buf.insert("a".into(), V(1));
        buf.insert("b".into(), V(2));
        buf.insert("c".into(), V(3));

        arc.guard()
            .with_commit(|mut writer| buf.flush_to_txn(&mut writer))?;
    }

    arc.guard().with_reader(|mut reader| {
        let buf = Store::new(db.clone());

        let mut n = buf.get(&mut reader, DbString::from("b"))?;
        assert_eq!(n.next(), Some(Ok(V(2))));
        Ok(())
    })
}

#[tokio::test(flavor = "multi_thread")]
async fn kvv_get_del_buffer() -> DatabaseResult<()> {
    observability::test_run().ok();
    let test_env = test_cell_env();
    let arc = test_env.env();
    let mut env = arc.guard();
    let db = env.open_multi("kv")?;

    arc.guard().with_reader(|mut reader| {
        let mut buf = Store::new(db.clone());

        buf.insert("a".into(), V(5));
        buf.insert("b".into(), V(4));
        buf.insert("c".into(), V(9));
        buf.delete("b".into(), V(4));
        let mut n = buf.get(&mut reader, DbString::from("b"))?;
        assert_eq!(n.next(), None);
        Ok(())
    })
}

#[tokio::test(flavor = "multi_thread")]
async fn kvv_get_del_persisted() -> DatabaseResult<()> {
    observability::test_run().ok();
    let test_env = test_cell_env();
    let arc = test_env.env();
    let mut env = arc.guard();
    let db = env.open_multi("kv")?;

    {
        let mut buf = Store::new(db.clone());

        buf.insert("a".into(), V(1));
        buf.insert("b".into(), V(2));
        buf.insert("c".into(), V(3));

        arc.guard()
            .with_commit(|mut writer| buf.flush_to_txn(&mut writer))?;
    }

    arc.guard().with_reader(|mut reader| {
        let mut buf: Store = Store::new(db.clone());

        buf.delete("b".into(), V(2));
        {
            let mut n = buf.get(&mut reader, DbString::from("b"))?;
            assert_eq!(n.next(), None);
        }

        arc.guard()
            .with_commit(|mut writer| buf.flush_to_txn(&mut writer))
    })?;

    arc.guard().with_reader(|mut reader| {
        let buf: KvvBufUsed<_, V> = Store::new(db.clone());

        let mut n = buf.get(&mut reader, DbString::from("b"))?;
        assert_eq!(n.next(), None);
        Ok(())
    })
}