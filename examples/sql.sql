-- Группировка по месяцам и ИНН контрагента с подсчетом количества транзакций, суммы и названия контрагента

SELECT date_trunc('month',date_registered_at)::date AS month_year,
counterparty_inn,
COUNT(transactions.id) AS transaction_count,
MAX(transactions.counterparty_name) AS name,
SUM(transactions.amount) AS total_amount
FROM transactions
INNER JOIN tag_transactions ON tag_transactions.transaction_id = transactions.id
INNER JOIN tags ON tags.id = tag_transactions.tag_id
WHERE transactions.account_id
IN ( SELECT id FROM accounts WHERE accounts.company_id = :company_id )
AND tags.name = :tag_name
AND tag_transactions.paint_value = :paint_value
GROUP BY month_year, counterparty_inn;


-- Создание уменьшенной версии таблицы с партицированнием для аналитики с автоматическим обновлением через траггер

CREATE TABLE analytic_jobs
(
  id                               integer,
  private                          boolean,
  company_id                       integer,
  city                             varchar,
  archived                         boolean,
  staffing_items_path              ltree,
  job_type                         varchar,
  company_department_id            integer
) PARTITION BY HASH(company_id);

CREATE TABLE analytic_jobs_0 PARTITION OF analytic_jobs FOR VALUES WITH (MODULUS 10,REMAINDER 0);
CREATE TABLE analytic_jobs_1 PARTITION OF analytic_jobs FOR VALUES WITH (MODULUS 10,REMAINDER 1);
CREATE TABLE analytic_jobs_2 PARTITION OF analytic_jobs FOR VALUES WITH (MODULUS 10,REMAINDER 2);
CREATE TABLE analytic_jobs_3 PARTITION OF analytic_jobs FOR VALUES WITH (MODULUS 10,REMAINDER 3);
CREATE TABLE analytic_jobs_4 PARTITION OF analytic_jobs FOR VALUES WITH (MODULUS 10,REMAINDER 4);
CREATE TABLE analytic_jobs_5 PARTITION OF analytic_jobs FOR VALUES WITH (MODULUS 10,REMAINDER 5);
CREATE TABLE analytic_jobs_6 PARTITION OF analytic_jobs FOR VALUES WITH (MODULUS 10,REMAINDER 6);
CREATE TABLE analytic_jobs_7 PARTITION OF analytic_jobs FOR VALUES WITH (MODULUS 10,REMAINDER 7);
CREATE TABLE analytic_jobs_8 PARTITION OF analytic_jobs FOR VALUES WITH (MODULUS 10,REMAINDER 8);
CREATE TABLE analytic_jobs_9 PARTITION OF analytic_jobs FOR VALUES WITH (MODULUS 10,REMAINDER 9);

CREATE UNIQUE INDEX index_analytic_jobs_on_company_id_id ON analytic_jobs(company_id, id);

CREATE OR REPLACE FUNCTION sync_analytic_jobs_func()
RETURNS trigger
LANGUAGE 'plpgsql'
AS $$
  BEGIN
    IF (TG_OP = 'INSERT' OR TG_OP = 'UPDATE') THEN
      INSERT INTO analytic_jobs(
        "id","private", "company_id", "city", "archived", "staffing_items_path", "job_type", "company_department_id"
      )
      SELECT
        new.id,
        new."private",
        new.company_id,
        new.city,
        new.archived,
        new.staffing_items_path,
        new.job_type,
        new."company_department_id"
      ON CONFLICT (company_id, id) DO UPDATE SET
        id = new.id,
        "private" = new."private",
        company_id = new.company_id,
        city = new.city,
        archived = new.archived,
        staffing_items_path = new.staffing_items_path,
        job_type = new.job_type,
        company_department_id = new.company_department_id;
    ELSEIF TG_OP = 'DELETE' THEN
      DELETE FROM analytic_jobs WHERE id = old.id AND company_id = old.company_id;
    END IF;
    RETURN NULL;
  END;
$$;

CREATE TRIGGER sync_analytic_jobs_trigger
AFTER INSERT OR UPDATE OR DELETE ON jobs FOR EACH ROW
EXECUTE PROCEDURE sync_analytic_jobs_func();
