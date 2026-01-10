CREATE TABLE IF NOT EXISTS dispatchers (
    id TEXT PRIMARY KEY NOT NULL,
    state INTEGER NOT NULL,
    location INTEGER NOT NULL,
    provisioned_at DATETIME NOT NULL
);
