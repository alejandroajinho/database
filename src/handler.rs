use crate::{
  redis::{Redis, RedisCredentials, RedisData, RedisError},
  scylla::{QueriesTrait, Scylla, ScyllaCredentials, ScyllaData, ScyllaError},
};

use scylla::frame::value::LegacySerializedValues;
use std::rc::Rc;
use tokio::select;

pub enum HandlerError {
  RedisError(RedisError),
  ScyllaError(ScyllaError),
}

impl From<RedisError> for HandlerError {
  fn from(error: RedisError) -> Self {
    HandlerError::RedisError(error)
  }
}

impl From<ScyllaError> for HandlerError {
  fn from(error: ScyllaError) -> Self {
    HandlerError::ScyllaError(error)
  }
}

pub struct HandlerConfiguration<'a, 'b> {
  redis_credentials: &'a RedisCredentials<'b>,
  scylla_credentials: &'a ScyllaCredentials<'b>,
}

pub struct Handler<Queries>
where
  Queries: QueriesTrait,
{
  pub redis: Redis,
  pub scylla: Rc<Scylla<Queries>>,
}

impl<Queries> Handler<Queries>
where
  Queries: QueriesTrait,
{
  pub async fn create_connections<'a, 'b>(
    configuration: &HandlerConfiguration<'a, 'b>,
  ) -> Result<Self, HandlerError>
  where
    Queries: QueriesTrait,
  {
    let redis = Redis::create_connections(configuration.redis_credentials).await?;

    let scylla: Scylla<Queries> =
      Scylla::create_connections(configuration.scylla_credentials).await?;

    Ok(Self {
      redis,
      scylla: Rc::new(scylla),
    })
  }

  pub async fn create<Data>(&mut self, data: &Data, expiration: u64) -> Result<(), HandlerError>
  where
    Data: RedisData + ScyllaData,
  {
    self.scylla.create(data).await?;
    self.redis.create(data, expiration).await?;

    Ok(())
  }

  pub async fn delete<Data>(&mut self, data: &Data) -> Result<(), HandlerError>
  where
    Data: RedisData + ScyllaData,
  {
    self.scylla.delete(data).await?;
    self.redis.delete(data).await?;

    Ok(())
  }

  pub async fn get<Data>(&mut self, scylla_id: &str, redis_key: &str) -> Result<Data, HandlerError>
  where
    Data: ScyllaData + RedisData,
  {
    let scylla_future = self.scylla.get::<Data>(scylla_id);
    let redis_future = self.redis.get::<Data>(redis_key);

    select! {
      value = scylla_future => {
        let resolved = value?;
        return Ok(resolved);
      },
      value = redis_future => {
        let resolved = value?;
        return Ok(resolved)
      }
    }
  }

  pub async fn fetch<Data>(
    &self,
    data: &LegacySerializedValues,
    ammount: usize,
  ) -> Result<Vec<Data>, HandlerError>
  where
    Data: ScyllaData,
  {
    let data = self.scylla.fetch(data, ammount).await?;
    Ok(data)
  }

  pub async fn udpate<Data>(&mut self, data: &Data) -> Result<(), HandlerError>
  where
    Data: RedisData + ScyllaData,
  {
    self.scylla.update::<Data>(data).await?;
    self.redis.update::<Data>(data).await?;

    Ok(())
  }
}
