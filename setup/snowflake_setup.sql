-- Preparación de tu cuenta trial de Snowflake (ejecútalo una sola vez, desde una hoja de Snowsight).
--
-- 1) Crea un trial en https://signup.snowflake.com (edición Enterprise, en Azure).
-- 2) Genera tu par de claves (no las subas al repo):
--      mkdir -p secrets
--      openssl genrsa 2048 | openssl pkcs8 -topk8 -inform PEM -out secrets/rsa_key.p8 -nocrypt
--      openssl rsa -in secrets/rsa_key.p8 -pubout -out secrets/rsa_key.pub
-- 3) Pega el contenido de rsa_key.pub (sin las líneas BEGIN/END ni saltos de línea) en RSA_PUBLIC_KEY.
-- 4) Ejecuta el script y rellena tu .env.
--
-- En la evaluación haremos lo mismo en nuestra cuenta: tu `make deploy` recibirá una base de datos y un
-- warehouse ya creados, con un rol propietario de la base de datos. No asumas otros privilegios.

USE ROLE ACCOUNTADMIN;

CREATE ROLE IF NOT EXISTS DE_CANDIDATE;
GRANT ROLE DE_CANDIDATE TO ROLE SYSADMIN;

CREATE WAREHOUSE IF NOT EXISTS DE_WH
  WAREHOUSE_SIZE = XSMALL AUTO_SUSPEND = 60 AUTO_RESUME = TRUE INITIALLY_SUSPENDED = TRUE;
GRANT USAGE, OPERATE ON WAREHOUSE DE_WH TO ROLE DE_CANDIDATE;

CREATE DATABASE IF NOT EXISTS DE_TEST;
GRANT OWNERSHIP ON DATABASE DE_TEST TO ROLE DE_CANDIDATE COPY CURRENT GRANTS;

CREATE USER IF NOT EXISTS DE_SVC
  TYPE = SERVICE
  DEFAULT_ROLE = DE_CANDIDATE
  DEFAULT_WAREHOUSE = DE_WH
  RSA_PUBLIC_KEY = '<PEGA_AQUI_TU_CLAVE_PUBLICA>';
GRANT ROLE DE_CANDIDATE TO USER DE_SVC;

-- Recomendado: un límite de créditos para no llevarte sorpresas.
CREATE RESOURCE MONITOR IF NOT EXISTS DE_MONITOR WITH CREDIT_QUOTA = 20
  TRIGGERS ON 90 PERCENT DO SUSPEND;
ALTER WAREHOUSE DE_WH SET RESOURCE_MONITOR = DE_MONITOR;
