use redb::{Database, ReadableTable, TableDefinition, TableError};
use std::sync::Arc;

const TABLE: TableDefinition<u64, bool> = TableDefinition::new("broadcast");

pub struct Db {
    db: Arc<Database>,
}

impl Db {
    pub fn new(filename: &str) -> Result<Self, String> {
        let db = Database::create(format!("{}.redb", filename)).map_err(|e| e.to_string())?;
        Ok(Self { db: Arc::new(db) })
    }

    pub async fn set_broadcast_id(&self, id: u64) -> Result<(), String> {
        let db = self.db.clone();

        tokio::task::spawn_blocking(move || {
            let write_txn = db.begin_write().map_err(|e| e.to_string())?;
            {
                let mut table = write_txn.open_table(TABLE).map_err(|e| e.to_string())?;
                table.insert(id, true).map_err(|e| e.to_string())?;
            }
            write_txn.commit().map_err(|e| e.to_string())?;

            Ok(())
        })
        .await
        .map_err(|e| e.to_string())?
    }

    pub async fn seen_broadcast_values(&self) -> Result<Vec<u64>, String> {
        let mut values = vec![];

        let db = self.db.clone();
        tokio::task::spawn_blocking(move || {
            let read_txn = db.begin_read().map_err(|e| e.to_string())?;
            {
                let table = match read_txn.open_table(TABLE) {
                    Ok(table) => table,
                    Err(TableError::TableDoesNotExist(_)) => return Ok(values),
                    Err(e) => return Err(e.to_string()),
                };

                let iter = table.iter().map_err(|e| e.to_string())?;
                for res in iter {
                    if let Ok(val) = res {
                        values.push(val.0.value());
                    } else {
                        return Err("Failed to read broadcast values".to_string());
                    }
                }
            }

            Ok(values)
        })
        .await
        .map_err(|e| e.to_string())?
    }
}
