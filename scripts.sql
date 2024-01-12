drop schema ds cascade
CREATE SCHEMA IF NOT EXISTS ds;
CREATE SCHEMA IF NOT EXISTS logs;
CREATE SCHEMA IF NOT EXISTS scss;


CREATE TABLE IF NOT EXISTS logs.etl_execution (
	run_id SERIAL PRIMARY KEY,
	start_dttm TIMESTAMP WITHOUT TIME ZONE DEFAULT NOW(),
	end_dttm TIMESTAMP WITHOUT TIME ZONE
);


CREATE OR REPLACE FUNCTION log_etl_execution() RETURNS INT AS $$
	DECLARE
		etl_run_id INT;
	BEGIN
		INSERT INTO logs.etl_execution DEFAULT VALUES returning run_id INTO etl_run_id;
		RETURN etl_run_id;
	END;
$$ LANGUAGE plpgsql;


CREATE OR REPLACE FUNCTION upd_insert(src TEXT, target TEXT) RETURNS VOID AS $$
	DECLARE
		pk_constraint TEXT;
		separated_target TEXT[];
		target_schema TEXT;
		target_table TEXT;
		col TEXT;
		on_conflict_opt TEXT := '';
		pk_fields TEXT[];
	BEGIN
		separated_target = STRING_TO_ARRAY(target, '.');
		target_schema = separated_target[1];
		target_table = separated_target[2];
		select constraint_name INTO pk_constraint from information_schema.table_constraints
		where 1=1
			  and table_schema = target_schema 
			  and table_name = target_table
			  and constraint_type = 'PRIMARY KEY';
	   SELECT ARRAY_AGG(DISTINCT column_name) into pk_fields
	   FROM information_schema.constraint_column_usage
	   where 1=1
		  and table_schema = target_schema
		  and table_name = target_table
		  and constraint_name = pk_constraint;
		
	   FOREACH col IN ARRAY 
	   (SELECT array_agg(DISTINCT column_name) FROM information_schema.columns
		where 1=1
		  and table_schema = target_schema
		  and table_name = target_table
		  and column_name not in (select UNNEST(pk_fields)
								 ) 
		)

	    LOOP
	   	    on_conflict_opt = on_conflict_opt || ' ' || col || ' = EXCLUDED.' || col || ', ' ;
	    END LOOP;
		on_conflict_opt = RTRIM(on_conflict_opt, ', ');
	    on_conflict_opt = FORMAT('INSERT INTO %s SELECT DISTINCT * FROM %s ON CONFLICT (%s) DO UPDATE SET ', target, src, RTRIM(ARRAY_TO_STRING(pk_fields, ', '), ', ')) || on_conflict_opt;
		EXECUTE on_conflict_opt;
	END;
$$ LANGUAGE plpgsql;


CREATE TABLE IF NOT EXISTS ds.ft_balance_f (
	on_date DATE NOT NULL,
	account_rk INT NOT NULL,
	currency_rk INT,
	balance_out FLOAT,
	CONSTRAINT pk_ft_balance_f PRIMARY KEY (account_rk, on_date)
);


CREATE TABLE IF NOT EXISTS ds.FT_POSTING_F (
	oper_date DATE NOT NULL,
	credit_account_rk INT NOT NULL,
	debet_account_rk INT NOT NULL,
	credit_amount FLOAT,
	debet_amount FLOAT,
	CONSTRAINT pk_ft_posting_f PRIMARY KEY (OPER_DATE, CREDIT_ACCOUNT_RK,
	DEBET_ACCOUNT_RK)
);


CREATE TABLE IF NOT EXISTS DS.MD_ACCOUNT_D (
	data_actual_date DATE NOT NULL,
	data_actual_end_date DATE NOT NULL,
	account_rk INT NOT NULL,
	account_number VARCHAR(20) not null,
	char_type VARCHAR(1) not null,
	currency_rk INT NOT NULL,
	currency_code VARCHAR(3) not null,
	CONSTRAINT pk_md_account_d PRIMARY KEY(DATA_ACTUAL_DATE, ACCOUNT_RK)
);


CREATE TABLE IF NOT EXISTS DS.MD_CURRENCY_D (
	currency_rk INT NOT NULL,
	data_actual_date DATE NOT NULL,
	data_actual_end_date DATE,
	currency_code VARCHAR(3),
	code_iso_char VARCHAR(3),
	CONSTRAINT pk_md_currency_d PRIMARY KEY (CURRENCY_RK, DATA_ACTUAL_DATE)
);


CREATE TABLE IF NOT EXISTS DS.MD_EXCHANGE_RATE_D (
	data_actual_date DATE not null,
	data_actual_end_date DATE,
	currency_rk INT not null,
	reduced_cource FLOAT,
	code_iso_num VARCHAR(3),
	CONSTRAINT pk_md_exchange_rate_d PRIMARY KEY (DATA_ACTUAL_DATE, CURRENCY_RK)
);


CREATE TABLE IF NOT EXISTS DS.MD_LEDGER_ACCOUNT_S (
	chapter CHAR(1),
	chapter_name VARCHAR(16),
	section_number INTEGER,
	section_name VARCHAR(22),
	subsection_name VARCHAR(21),
	ledger1_account INT,
	ledger1_account_name VARCHAR(47),
	ledger_account INT not null,
	ledger_account_name VARCHAR(153),
	characteristic CHAR(1),
	is_resident INT,
	is_reservу INT,
	is_reserved INT,
	is_loan INT,
	is_reserved_assets INT,
	is_overdue INT,
	is_interest INT,
	pair_account VARCHAR(5),
	start_date DATE NOT NULL,
	end_date DATE,
	is_rub_only INT,
	min_term VARCHAR(1),
	min_term_measure VARCHAR(1),
	max_term VARCHAR(1),
	max_term_measure VARCHAR(1),
	ledger_acc_full_name_translit VARCHAR(1),
	is_revaluation VARCHAR(1),
	is_correct VARCHAR(1),
	CONSTRAINT pk_md_ledger_account_s PRIMARY KEY(LEDGER_ACCOUNT, START_DATE)
);


insert into ds.ft_balance_f values 
	('2018-01-01', 1, 100, 200.0);
SELECT * FROM ds.ft_balance_f
where on_date = '2018-01-01' AND account_rk = 1

SELECT * FROM scss.ft_balance_f;
insert into scss.ft_balance_f values 
	('2018-01-01', 1, 300, 1.0);
SELECT * FROM scss.ft_balance_f;

select * from logs.etl_execution