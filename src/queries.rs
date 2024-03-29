pub static CREATE_KEYSPACE: &str = r"CREATE KEYSPACE IF NOT EXISTS taomiko WITH replication = {
  'class': 'SimpleStrategy',
  'replication_factor': 1
};";

pub static CREATE_USER: &str = r"INSERT INTO taomiko.users (
  id,
  
  avatar,
  birthdate,
  created_at,
  language,
  last_activity,
  last_update,
  mail,
  password,
  permissions,
  phone,
  privacies,
  secret,
  username
) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?);";

pub static CREATE_USERS_TABLE: &str = r"CREATE TABLE IF NOT EXISTS taomiko.users (
  id UUID PRIMARY KEY,

  avatar VARCHAR,
  birthdate TIMESTAMP,
  created_at TIMESTAMP,
  language INT,
  last_activity TIMESTAMP,
  last_update TIMESTAMP,
  mail VARCHAR,
  password VARCHAR,
  permissions BIGINT,
  phone VARCHAR,
  privacies BIGINT,
  secret VARCHAR,
  username VARCHAR
);";

pub static DELETE_USER_BY_ID: &str = r"DELETE FROM taomiko.users WHERE id = ?;";

pub static GET_USER_BY_ID: &str = r"SELECT * FROM taomiko.users WHERE id = ?;";
