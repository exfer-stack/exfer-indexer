//! redb-backed storage layer for the indexer.
//!
//! Tables and full read/write API land in commits #13 and #14 of the
//! workflow plan (`exfer-indexer follower + extraction` and
//! `exfer-indexer RPC methods`). This module exists from the scaffold
//! commit so the rest of the crate can name `crate::db::Db`.

pub mod schema;

use std::path::Path;

use crate::error::{Error, Result};

/// Indexer storage handle. Wraps a single redb file at
/// `<datadir>/index.redb` and exposes typed read / write helpers.
///
/// Real CRUD methods land in the follower + queries commits; this
/// stub just owns the redb `Database` and creates the file on first
/// open.
pub struct Db {
    db: redb::Database,
}

impl Db {
    pub fn open(datadir: &Path) -> Result<Self> {
        std::fs::create_dir_all(datadir)
            .map_err(|e| Error::Storage(format!("create datadir: {e}")))?;
        let path = datadir.join("index.redb");
        let db = redb::Database::create(&path)
            .map_err(|e| Error::Storage(format!("open {}: {e}", path.display())))?;

        // Pre-create every table so subsequent read txns don't trip on
        // "table not found" on a fresh datadir.
        let write = db.begin_write()?;
        {
            schema::open_all_tables(&write)?;
        }
        write.commit()?;

        Ok(Self { db })
    }

    /// Raw redb handle. Held `pub(crate)` so the follower and query
    /// modules can manage their own write / read transactions
    /// without re-wrapping every operation.
    pub(crate) fn raw(&self) -> &redb::Database {
        &self.db
    }
}
