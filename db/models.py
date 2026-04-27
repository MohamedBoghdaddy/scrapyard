"""
Raw SQL DDL for both SQLite and PostgreSQL backends.
All table creation is idempotent (IF NOT EXISTS / ON CONFLICT).
"""

# ---------------------------------------------------------------------------
# SQLite DDL
# ---------------------------------------------------------------------------

SQLITE_CREATE_SCRAPED_URLS = """
CREATE TABLE IF NOT EXISTS scraped_urls (
    id          INTEGER PRIMARY KEY AUTOINCREMENT,
    url         TEXT    UNIQUE NOT NULL,
    site        TEXT    NOT NULL DEFAULT '',
    scraped_at  TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    status      TEXT    NOT NULL DEFAULT 'success',
    metadata    TEXT
);
"""

SQLITE_CREATE_CHECKPOINTS = """
CREATE TABLE IF NOT EXISTS checkpoints (
    id                  INTEGER PRIMARY KEY AUTOINCREMENT,
    site                TEXT NOT NULL,
    category            TEXT NOT NULL,
    last_page           INTEGER DEFAULT 1,
    last_product_index  INTEGER DEFAULT 0,
    updated_at          TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(site, category)
);
"""

SQLITE_CREATE_PRODUCT_SNAPSHOTS = """
CREATE TABLE IF NOT EXISTS product_snapshots (
    product_id    TEXT PRIMARY KEY,
    site          TEXT NOT NULL DEFAULT '',
    product_url   TEXT NOT NULL DEFAULT '',
    price         REAL,
    stock_status  TEXT,
    last_seen     TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    raw_data      TEXT
);
"""

SQLITE_CREATE_PRODUCTS = """
CREATE TABLE IF NOT EXISTS products (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    url             TEXT    UNIQUE,
    name            TEXT,
    price           REAL,
    raw_price       TEXT,
    vendor          TEXT,
    part_number     TEXT,
    image_url       TEXT,
    stock_status    TEXT,
    category        TEXT,
    source          TEXT,
    description     TEXT,
    specifications  TEXT,
    variants        TEXT,
    scraped_at      TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
"""

SQLITE_ALL_DDL = [
    SQLITE_CREATE_SCRAPED_URLS,
    SQLITE_CREATE_CHECKPOINTS,
    SQLITE_CREATE_PRODUCT_SNAPSHOTS,
    SQLITE_CREATE_PRODUCTS,
]

# ---------------------------------------------------------------------------
# PostgreSQL DDL
# ---------------------------------------------------------------------------

POSTGRES_CREATE_SCRAPED_URLS = """
CREATE TABLE IF NOT EXISTS scraped_urls (
    id          SERIAL PRIMARY KEY,
    url         TEXT    UNIQUE NOT NULL,
    site        TEXT    NOT NULL DEFAULT '',
    scraped_at  TIMESTAMPTZ DEFAULT NOW(),
    status      TEXT    NOT NULL DEFAULT 'success',
    metadata    JSONB
);
"""

POSTGRES_CREATE_CHECKPOINTS = """
CREATE TABLE IF NOT EXISTS checkpoints (
    id                  SERIAL PRIMARY KEY,
    site                TEXT NOT NULL,
    category            TEXT NOT NULL,
    last_page           INTEGER DEFAULT 1,
    last_product_index  INTEGER DEFAULT 0,
    updated_at          TIMESTAMPTZ DEFAULT NOW(),
    UNIQUE(site, category)
);
"""

POSTGRES_CREATE_PRODUCT_SNAPSHOTS = """
CREATE TABLE IF NOT EXISTS product_snapshots (
    product_id    TEXT PRIMARY KEY,
    site          TEXT NOT NULL DEFAULT '',
    product_url   TEXT NOT NULL DEFAULT '',
    price         NUMERIC(12, 2),
    stock_status  TEXT,
    last_seen     TIMESTAMPTZ DEFAULT NOW(),
    raw_data      JSONB
);
"""

POSTGRES_CREATE_PRODUCTS = """
CREATE TABLE IF NOT EXISTS products (
    id              SERIAL PRIMARY KEY,
    url             TEXT    UNIQUE,
    name            TEXT,
    price           NUMERIC(12, 2),
    raw_price       TEXT,
    vendor          TEXT,
    part_number     TEXT,
    image_url       TEXT,
    stock_status    TEXT,
    category        TEXT,
    source          TEXT,
    description     TEXT,
    specifications  JSONB,
    variants        JSONB,
    scraped_at      TIMESTAMPTZ DEFAULT NOW()
);
"""

POSTGRES_ALL_DDL = [
    POSTGRES_CREATE_SCRAPED_URLS,
    POSTGRES_CREATE_CHECKPOINTS,
    POSTGRES_CREATE_PRODUCT_SNAPSHOTS,
    POSTGRES_CREATE_PRODUCTS,
]

# ---------------------------------------------------------------------------
# MySQL DDL
# ---------------------------------------------------------------------------

MYSQL_CREATE_SCRAPED_URLS = """
CREATE TABLE IF NOT EXISTS scraped_urls (
    id          INT AUTO_INCREMENT PRIMARY KEY,
    url         VARCHAR(2048) NOT NULL,
    site        VARCHAR(64)   NOT NULL DEFAULT '',
    scraped_at  DATETIME      DEFAULT CURRENT_TIMESTAMP,
    status      VARCHAR(32)   NOT NULL DEFAULT 'success',
    metadata    JSON,
    UNIQUE KEY uq_url (url(512))
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
"""

MYSQL_CREATE_CHECKPOINTS = """
CREATE TABLE IF NOT EXISTS checkpoints (
    id                  INT AUTO_INCREMENT PRIMARY KEY,
    site                VARCHAR(64) NOT NULL,
    category            VARCHAR(512) NOT NULL,
    last_page           INT DEFAULT 1,
    last_product_index  INT DEFAULT 0,
    updated_at          DATETIME DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    UNIQUE KEY uq_site_cat (site, category(256))
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
"""

MYSQL_CREATE_PRODUCTS = """
CREATE TABLE IF NOT EXISTS products (
    id              INT AUTO_INCREMENT PRIMARY KEY,
    url             VARCHAR(2048),
    name            TEXT,
    price           DECIMAL(12,2),
    raw_price       VARCHAR(256),
    vendor          VARCHAR(256),
    part_number     VARCHAR(256),
    image_url       VARCHAR(2048),
    stock_status    VARCHAR(64),
    category        VARCHAR(512),
    source          VARCHAR(64),
    description     LONGTEXT,
    specifications  JSON,
    variants        JSON,
    scraped_at      DATETIME DEFAULT CURRENT_TIMESTAMP,
    UNIQUE KEY uq_url (url(512))
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
"""

MYSQL_ALL_DDL = [
    MYSQL_CREATE_SCRAPED_URLS,
    MYSQL_CREATE_CHECKPOINTS,
    MYSQL_CREATE_PRODUCTS,
]
