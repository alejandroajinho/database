use scylla::{
  cql_to_rust::FromRowError, frame::value::LegacySerializedValues,
  prepared_statement::PreparedStatement, serialize::row::SerializeRow, FromRow, IntoTypedRows,
  Session, SessionBuilder,
};
use std::{future::Future, vec::Vec};

pub struct ScyllaCredentials<'a> {
  pub uri: &'a str,
  pub user: &'a str,
  pub password: &'a str,
}

use thiserror::Error;

#[derive(Error, Debug)]
pub enum ScyllaError {
  #[error("[CreateSessionError]: An error has ocurred while creating the session.\nDetail: {0}")]
  CreateSessionError(String),
  #[error("[QueryPreparationError]: An error has ocurred while preparing a query.\nDetail: {0}")]
  QueryPreparationError(String),
  #[error("[QueryError]: An error has ocurred while making a query.\nDetail: {0}")]
  QueryError(String),
  #[error("[InvalidQuery]: An invalid query has been requests for datatype.\nDetail")]
  InvalidQuery,
  #[error("[CreateError]: An error has ocurred while creating data.\nDetail: {0}")]
  CreateError(String),
  #[error("[DeleteError]: An error has ocurred while deleting data.\nDetail: {0}")]
  DeleteError(String),
  #[error("[GetError]: An error has ocurred while getting data.\nDetail: {0}")]
  GetError(String),
  #[error("[RowError]: An error has ocurred while processing rows.")]
  RowError,
  #[error("[FetchError]: An error has ocurred while fetching data.\nDetail: {0}")]
  FetchError(String),
  #[error("[UpdateError]: An error has ocurred while updating data.\nDetail: {0}")]
  UpdateError(String),
}

pub enum Kind {
  Create,
  Update,
  Delete,
  Get,
  Fetch,
}

pub enum Query<'a> {
  Prepared(&'a PreparedStatement),
  Raw(&'a str),
}

pub trait ScyllaData: FromRow + SerializeRow {
  fn id(&self) -> String;
}

pub trait QueriesTrait {
  fn create_keyspace(session: &Session) -> impl Future<Output = Result<(), ScyllaError>> + Send;
  fn create_tables(session: &Session) -> impl Future<Output = Result<(), ScyllaError>> + Send;
  fn prepare_queries(sesssion: &Session) -> impl Future<Output = Result<Self, ScyllaError>> + Send
  where
    Self: Sized;
  fn get_query<'a, Data>(&self, kind: Kind) -> Result<Query<'a>, ScyllaError>
  where
    Data: ScyllaData;
}

pub struct Scylla<Queries>
where
  Queries: QueriesTrait,
{
  pub queries: Queries,
  pub session: Session,
}

impl<Queries> Scylla<Queries>
where
  Queries: QueriesTrait,
{
  pub async fn create_connections<'a>(
    credentials: &ScyllaCredentials<'a>,
  ) -> Result<Self, ScyllaError>
  where
    Queries: QueriesTrait,
  {
    let session = SessionBuilder::new()
      .known_node(credentials.uri)
      .user(credentials.user, credentials.password)
      .build()
      .await
      .map_err(|error| ScyllaError::CreateSessionError(error.to_string()))?;

    Queries::create_keyspace(&session).await?;
    Queries::create_tables(&session).await?;

    let queries = Queries::prepare_queries(&session).await?;

    Ok(Self { session, queries })
  }

  pub async fn create<Data>(&self, data: &Data) -> Result<(), ScyllaError>
  where
    Data: ScyllaData,
  {
    let query = self.queries.get_query::<Data>(Kind::Create);

    let Ok(query) = query else {
      return Err(ScyllaError::InvalidQuery);
    };

    match query {
      Query::Prepared(query) => {
        self
          .session
          .execute(query, data)
          .await
          .map_err(|error| ScyllaError::CreateError(error.to_string()))?;

        Ok(())
      }
      Query::Raw(query) => {
        self
          .session
          .query(query, data)
          .await
          .map_err(|error| ScyllaError::QueryError(error.to_string()))?;

        Ok(())
      }
    }
  }

  pub async fn delete<Data>(&self, data: &Data) -> Result<(), ScyllaError>
  where
    Data: ScyllaData,
  {
    let query = self.queries.get_query::<Data>(Kind::Delete);

    let Ok(query) = query else {
      return Err(ScyllaError::InvalidQuery);
    };

    match query {
      Query::Prepared(query) => {
        self
          .session
          .execute(query, &(&data.id(),))
          .await
          .map_err(|error| ScyllaError::DeleteError(error.to_string()))?;

        Ok(())
      }
      Query::Raw(query) => {
        self
          .session
          .query(query, &(&data.id(),))
          .await
          .map_err(|error| ScyllaError::DeleteError(error.to_string()))?;

        Ok(())
      }
    }
  }

  pub async fn get<Data>(&self, id: &str) -> Result<Data, ScyllaError>
  where
    Data: ScyllaData,
  {
    let query = self.queries.get_query::<Data>(Kind::Get);

    let Ok(query) = query else {
      return Err(ScyllaError::InvalidQuery);
    };

    let result = match query {
      Query::Prepared(query) => self
        .session
        .execute(query, &(id,))
        .await
        .map_err(|error| ScyllaError::GetError(error.to_string())),
      Query::Raw(query) => self
        .session
        .query(query, &(id,))
        .await
        .map_err(|error| ScyllaError::GetError(error.to_string())),
    }?;

    let first_row = result.first_row().map_err(|_| ScyllaError::RowError)?;
    first_row
      .into_typed::<Data>()
      .map_err(|_| ScyllaError::RowError)
  }

  pub async fn fetch<Data>(
    &self,
    query_data: &LegacySerializedValues,
    ammount: usize,
  ) -> Result<Vec<Data>, ScyllaError>
  where
    Data: ScyllaData,
  {
    let ammount = if ammount == 0 { 10 } else { ammount };
    let query = self.queries.get_query::<Data>(Kind::Fetch);

    let Ok(query) = query else {
      return Err(ScyllaError::InvalidQuery);
    };

    let result = match query {
      Query::Prepared(query) => self
        .session
        .execute(query, query_data)
        .await
        .map_err(|error| ScyllaError::FetchError(error.to_string())),
      Query::Raw(query) => self
        .session
        .query(query, query_data)
        .await
        .map_err(|error| ScyllaError::FetchError(error.to_string())),
    }?;

    let Ok(raw_rows) = result.rows() else {
      return Err(ScyllaError::RowError);
    };

    let typed_rows = raw_rows.into_typed::<Data>();

    typed_rows
      .take(ammount)
      .collect::<Result<Vec<Data>, FromRowError>>()
      .map_err(|_| ScyllaError::RowError)
  }

  pub async fn update<Data>(&self, data: &Data) -> Result<(), ScyllaError>
  where
    Data: ScyllaData,
  {
    let query = self.queries.get_query::<Data>(Kind::Update);

    let Ok(query) = query else {
      return Err(ScyllaError::InvalidQuery);
    };

    match query {
      Query::Prepared(query) => {
        self
          .session
          .execute(query, &(&data.id(),))
          .await
          .map_err(|error| ScyllaError::DeleteError(error.to_string()))?;

        Ok(())
      }
      Query::Raw(query) => {
        self
          .session
          .query(query, &(&data.id(),))
          .await
          .map_err(|error| ScyllaError::DeleteError(error.to_string()))?;

        Ok(())
      }
    }
  }
}
