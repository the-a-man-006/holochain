use once_cell::sync::Lazy;
use rusqlite::Connection;

use crate::db::DbKind;

pub(crate) static SCHEMA_CELL: Lazy<Schema> = Lazy::new(|| {
    let migration_0 = Migration::initial(include_str!("schema/cell/initial.sql"));

    Schema {
        current_index: 0,
        migrations: vec![migration_0],
    }
});

pub struct Schema {
    current_index: usize,
    migrations: Vec<Migration>,
}

impl Schema {
    pub fn initialize(&self, conn: &mut Connection, db_kind: &DbKind) -> rusqlite::Result<()> {
        let user_version: u16 =
            conn.pragma_query_value(None, "user_version", |row| Ok(row.get(0)?))?;

        if user_version == 0 {
            // database just needs to be created / initialized
            self.migrations[self.current_index].initialize(conn)?;
            tracing::info!("database initialized: {}", db_kind);
            return Ok(());
        } else {
            let current_index = user_version as usize - 1;
            if current_index < self.current_index {
                // run forward migrations
                for v in current_index..self.current_index + 1 {
                    self.migrations[v].run(conn)?;
                }
                tracing::info!(
                    "database forward migrated: {} from {} to {}",
                    db_kind,
                    current_index,
                    self.current_index
                );
            } else if current_index > self.current_index {
                unimplemented!("backward migrations unimplemented");
            } else {
                tracing::debug!(
                    "database needed no migration or initialization, good to go: {}",
                    db_kind
                );
            }
        }

        Ok(())
    }
}

pub struct Migration {
    schema: Sql,
    _forward: Sql,
    _backward: Option<Sql>,
}

impl Migration {
    pub fn initial(schema: &str) -> Self {
        Self {
            schema: schema.into(),
            _forward: "".into(),
            _backward: None,
        }
    }

    pub fn initialize(&self, conn: &mut Connection) -> rusqlite::Result<()> {
        conn.execute_batch(&self.schema)?;
        Ok(())
    }

    pub fn run(&self, _conn: &mut Connection) -> rusqlite::Result<()> {
        unimplemented!("actual migrations not yet implemented")
    }
}

type Sql = String;