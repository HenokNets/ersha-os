use std::str::FromStr;

use ersha_core::{Dispatcher, DispatcherId, DispatcherState, H3Cell};
use sqlx::{Row, SqlitePool};
use ulid::Ulid;

use super::{DispatcherRegistry, filter::DispatcherFilter};

pub struct SqliteDespatcherRegistry {
    pool: SqlitePool,
}

impl SqliteDespatcherRegistry {
    pub fn new(pool: SqlitePool) -> Self {
        Self { pool }
    }
}

#[derive(Debug)]
pub enum SqliteRegistryError {
    Sqlx(sqlx::Error),
    InvalidUlid(String),
    InvalidTimestamp(i64),
    InvalidState(i32),
    NotFound,
}

impl From<sqlx::Error> for SqliteRegistryError {
    fn from(e: sqlx::Error) -> Self {
        Self::Sqlx(e)
    }
}

impl DispatcherRegistry for SqliteDespatcherRegistry {
    type Error = SqliteRegistryError;

    async fn register(&mut self, dispatcher: Dispatcher) -> Result<(), Self::Error> {
        sqlx::query(
            r#"
            INSERT OR REPLACE INTO dispatchers (id, state, location, provisioned_at)
            VALUES (?, ?, ?, ?)
            "#,
        )
        .bind(dispatcher.id.0.to_string())
        .bind(dispatcher.state as i32)
        .bind(dispatcher.location.0 as i64)
        .bind(dispatcher.provisioned_at.as_second())
        .execute(&self.pool)
        .await?;

        Ok(())
    }

    async fn get(&self, id: DispatcherId) -> Result<Option<Dispatcher>, Self::Error> {
        let row = sqlx::query(
            r#"
            SELECT id, state, location, provisioned_at
            FROME dispatchers
            WHERE id = ?
            "#,
        )
        .bind(id.0.to_string())
        .fetch_optional(&self.pool)
        .await?;

        row.map(|r| -> Result<Dispatcher, SqliteRegistryError> {
            let id = r.get::<String, _>("id");
            let ulid = Ulid::from_str(&id).map_err(|_| {
                SqliteRegistryError::InvalidUlid(r.get::<String, _>("id").to_string())
            })?;

            let provisioned_at = jiff::Timestamp::from_second(r.get::<i64, _>("provisioned_at"))
                .map_err(|_| {
                    SqliteRegistryError::InvalidTimestamp(r.get::<i64, _>("provisioned_at"))
                })?;

            let state = match r.get::<i32, _>("state") {
                0 => DispatcherState::Active,
                1 => DispatcherState::Suspended,
                other => return Err(SqliteRegistryError::InvalidState(other)),
            };

            Ok(Dispatcher {
                id: DispatcherId(ulid),
                location: H3Cell(r.get::<i64, _>("location") as u64),
                state,
                provisioned_at,
            })
        })
        .transpose()
    }

    async fn update(&mut self, id: DispatcherId, new: Dispatcher) -> Result<(), Self::Error> {
        let old = self.get(id).await?.ok_or(SqliteRegistryError::NotFound)?;
        let new = Dispatcher { id: old.id, ..new };

        self.register(new).await
    }

    async fn suspend(&mut self, id: DispatcherId) -> Result<(), Self::Error> {
        let dispatcher = self.get(id).await?.ok_or(SqliteRegistryError::NotFound)?;

        let new = Dispatcher {
            state: DispatcherState::Suspended,
            ..dispatcher
        };

        self.register(new).await
    }

    async fn batch_register(
        &mut self,
        dispatchers: Vec<ersha_core::Dispatcher>,
    ) -> Result<(), Self::Error> {
        todo!()
    }

    async fn count(&self, filter: Option<DispatcherFilter>) -> Result<usize, Self::Error> {
        todo!()
    }

    async fn list(
        &self,
        options: super::filter::QueryOptions<
            super::filter::DispatcherFilter,
            super::filter::DispatcherSortBy,
        >,
    ) -> Result<Vec<ersha_core::Dispatcher>, Self::Error> {
        todo!()
    }
}
