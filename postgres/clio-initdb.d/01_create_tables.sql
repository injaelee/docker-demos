-- connect to the 'rxcliodev' database
--
\c rxcliodev;

-- entry states
--
CREATE TABLE clio.entry_state (
  id SMALLINT PRIMARY KEY,
  name VARCHAR(16) UNIQUE NOT NULL
);

INSERT INTO clio.entry_state (id, name)
VALUES
(0, 'invalid'),
(1, 'active'),
(2, 'revoked');


-- create user table
--
CREATE TABLE clio.entries (
  entry_id    UUID PRIMARY KEY,
  content     VARCHAR(128) UNIQUE NOT NULL,
  state       SMALLINT NOT NULL DEFAULT 1,
  FOREIGN KEY (state) REFERENCES clio.entry_state(id)
);
