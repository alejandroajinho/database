use redis::{aio::MultiplexedConnection, AsyncCommands, Client, FromRedisValue, ToRedisArgs};

use thiserror::Error;

pub struct RedisCredentials<'a> {
  pub uri: &'a str,
}

#[derive(Error, Debug)]
pub enum RedisError {
  #[error("[CreateClientError]: An error has ocurred while creating the client.\nDetail: {0}")]
  CreateClientError(String),
  #[error(
    "[CreateConnectionError]: An error has ocurred while creating connections.\nDetail: {0}"
  )]
  CreateConnectionError(String),
  #[error("[CreateError]: An error has ocurred while creating some data.\nDetail: {0}")]
  CreateError(String),
  #[error("[DeleteError]: An error has ocurred while deleting some data.\nDetail: {0}")]
  DeleteError(String),
  #[error("[GetError]: An error has ocurred while trying to get some data.\nDetail: {0}")]
  GetError(String),
  #[error("[SerializeError]: An error has ocurred while serializing the data.\nDetail: {0}")]
  SerializeError(String),
}

pub struct Redis {
  pub connection: MultiplexedConnection,
  pub client: Client,
}

pub trait RedisData: FromRedisValue + ToRedisArgs {
  fn key(&self) -> &String;
  fn default_expiration() -> u64;
}

impl Redis {
  pub async fn create_connections<'a>(
    credentials: &RedisCredentials<'a>,
  ) -> Result<Self, RedisError> {
    let client = Client::open(credentials.uri)
      .map_err(|error| RedisError::CreateClientError(error.to_string()))?;

    let connection = client
      .get_multiplexed_async_connection()
      .await
      .map_err(|error| RedisError::CreateConnectionError(error.to_string()))?;

    Ok(Self { client, connection })
  }

  pub async fn create<Data>(&mut self, data: &Data, expiration: u64) -> Result<(), RedisError>
  where
    Data: RedisData,
  {
    let expiration_time = if expiration == 0 {
      Data::default_expiration()
    } else {
      expiration
    };

    self
      .connection
      .set_ex(&data.key(), &data.to_redis_args(), expiration_time)
      .await
      .map_err(|error| RedisError::CreateError(error.to_string()))
  }

  pub async fn delete<Data>(&mut self, data: &Data) -> Result<(), RedisError>
  where
    Data: RedisData,
  {
    self
      .connection
      .del(&data.key())
      .await
      .map_err(|error| RedisError::DeleteError(error.to_string()))
  }

  pub async fn delete_by_key(&mut self, key: &str) -> Result<(), RedisError> {
    self
      .connection
      .del(key)
      .await
      .map_err(|error| RedisError::DeleteError(error.to_string()))
  }

  pub async fn update<Data>(&mut self, data: &Data) -> Result<(), RedisError>
  where
    Data: RedisData,
  {
    self.create(data, 0).await
  }

  pub async fn get<Data>(&mut self, key: &str) -> Result<Data, RedisError>
  where
    Data: FromRedisValue,
  {
    self
      .connection
      .get::<&str, Data>(key)
      .await
      .map_err(|error| RedisError::GetError(error.to_string()))
  }
}
