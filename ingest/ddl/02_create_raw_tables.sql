-- One-time setup: create the three raw tables that sync_wge.py writes into.
-- Run AFTER 01_create_raw_schema.sql, in your warehouse DB.

IF OBJECT_ID('raw.csm_connection_master', 'U') IS NULL
BEGIN
    CREATE TABLE raw.csm_connection_master (
        location_id        varchar(32)  NOT NULL,
        equipment_id       varchar(32)  NOT NULL,
        connection_date    date         NULL,
        disconnection_date date         NULL,
        connection_status  varchar(32)  NULL,
        fixed_mult         int          NULL
    );
END;
GO

IF OBJECT_ID('raw.csm_equipment_master', 'U') IS NULL
BEGIN
    CREATE TABLE raw.csm_equipment_master (
        equipment_id varchar(32) NOT NULL,
        equip_class  varchar(64) NULL,
        CONSTRAINT pk_csm_equipment_master PRIMARY KEY (equipment_id)
    );
END;
GO

IF OBJECT_ID('raw.um00403', 'U') IS NULL
BEGIN
    CREATE TABLE raw.um00403 (
        location_id  varchar(32) NOT NULL,
        equipment_id varchar(32) NOT NULL,
        connect_seq  int         NOT NULL,
        rate1        varchar(16) NULL
    );
END;
GO
