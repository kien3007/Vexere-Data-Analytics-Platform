-- Tạo/cập nhật ROLE 'metabase'
DO $$
BEGIN
   IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'metabase') THEN
      CREATE ROLE metabase WITH LOGIN PASSWORD 'metabase';
   ELSE
      ALTER ROLE metabase WITH LOGIN PASSWORD 'metabase';
   END IF;
END
$$;

-- Tạo DB 'metabase' nếu chưa có (CHẠY NGOÀI DO)
-- (Entry point sẽ chạy file này bằng superuser $POSTGRES_USER)
CREATE DATABASE metabase OWNER metabase ENCODING 'UTF8';

GRANT ALL PRIVILEGES ON DATABASE metabase TO metabase;
