CREATE TABLE users (
  user_id STRING(36) NOT NULL,
  username STRING(50) NOT NULL,
  email STRING(100) NOT NULL,
  created_at TIMESTAMP NOT NULL OPTIONS (allow_commit_timestamp=true),
  last_login TIMESTAMP,
) PRIMARY KEY (user_id);

CREATE UNIQUE INDEX users_by_email ON users(email);
CREATE INDEX users_by_username ON users(username);
