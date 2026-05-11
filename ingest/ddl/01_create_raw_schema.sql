-- One-time setup: create the "raw" schema in the warehouse database.
-- Run this in SSMS connected to your warehouse DB once, before sync_wge.py.

IF NOT EXISTS (SELECT 1 FROM sys.schemas WHERE name = 'raw')
    EXEC('CREATE SCHEMA raw');
GO
