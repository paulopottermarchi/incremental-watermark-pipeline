-- local/seed/01_schema.sql
-- ===========================================================================
-- Minimal stand-in for the source system: the four tables the pipeline
-- reads, with the columns it selects and nothing else.
--
-- The indexes on update_date are not decoration. Without them the
-- incremental predicate degrades to a scan and the benchmark measures the
-- wrong thing — an incremental pull that has to read every row to find the
-- recent ones is not incremental in any useful sense.
-- ===========================================================================

IF DB_ID('collections_demo') IS NULL
    CREATE DATABASE collections_demo;
GO

USE collections_demo;
GO

IF SCHEMA_ID('collections') IS NULL
    EXEC('CREATE SCHEMA collections');
GO

DROP TABLE IF EXISTS collections.dialer;
DROP TABLE IF EXISTS collections.contact;
DROP TABLE IF EXISTS collections.case_attribute;
DROP TABLE IF EXISTS collections.[case];
GO

CREATE TABLE collections.[case] (
    case_id          INT            NOT NULL PRIMARY KEY,
    client_id        INT            NOT NULL,
    ref_number       VARCHAR(32)    NOT NULL,
    debtor_id        INT            NOT NULL,
    original_capital DECIMAL(18, 2) NOT NULL,
    actual_capital   DECIMAL(18, 2) NOT NULL,
    case_statute_id  INT            NOT NULL,
    update_date      DATETIME2(3)   NOT NULL
);
CREATE INDEX ix_case_update_date ON collections.[case] (update_date) INCLUDE (client_id);
GO

CREATE TABLE collections.case_attribute (
    case_attribute_id      INT          IDENTITY(1,1) PRIMARY KEY,
    case_id                INT          NOT NULL,
    case_attribute_type_id INT          NOT NULL,
    case_attribute_value   VARCHAR(64)  NULL,
    update_date            DATETIME2(3) NOT NULL
);
CREATE INDEX ix_attr_update_date ON collections.case_attribute (update_date) INCLUDE (case_attribute_type_id);
CREATE INDEX ix_attr_case        ON collections.case_attribute (case_id, case_attribute_type_id);
GO

CREATE TABLE collections.dialer (
    uniqueid         VARCHAR(48)  NOT NULL PRIMARY KEY,
    case_id          INT          NOT NULL,
    client_id        INT          NOT NULL,
    telecom_id       INT          NOT NULL,
    phone_number     VARCHAR(20)  NOT NULL,
    disposition_code INT          NOT NULL,
    call_date        DATETIME2(3) NOT NULL,
    update_date      DATETIME2(3) NOT NULL
);
CREATE INDEX ix_dialer_update_date ON collections.dialer (update_date) INCLUDE (client_id);
CREATE INDEX ix_dialer_call_date   ON collections.dialer (call_date)   INCLUDE (client_id, case_id);
GO

CREATE TABLE collections.contact (
    contact_id     INT          NOT NULL PRIMARY KEY,
    case_id        INT          NOT NULL,
    status_type_id INT          NOT NULL,
    target_type_id INT          NOT NULL,
    contact_date   DATETIME2(3) NOT NULL,
    insert_user    VARCHAR(64)  NOT NULL,
    update_date    DATETIME2(3) NOT NULL
);
CREATE INDEX ix_contact_update_date ON collections.contact (update_date);
GO

PRINT 'Schema ready.';
GO
