DROP TABLE IF EXISTS dwh.customer_report_datamart;
CREATE TABLE IF NOT EXISTS dwh.customer_report_datamart (
	id BIGINT NOT NULL GENERATED ALWAYS AS IDENTITY NOT NULL,
	customer_id BIGINT NOT NULL,
	customer_name varchar NOT NULL,	
	customer_address varchar NOT NULL,
	customer_birthday date NOT NULL,
	customer_email varchar NOT NULL,
	customer_money numeric(15,2) NOT NULL,	
	platform_money numeric(15,2) NOT NULL,
	count_order int8 NOT NULL,
	avg_price_order numeric(10, 2) NOT NULL,
	median_time_order_completed numeric(10, 1) NULL,
	top_product_type varchar NOT NULL,
	top_craftsman_id BIGINT NOT NULL,
	count_order_created BIGINT NOT NULL,
	count_order_in_progress BIGINT NOT NULL,
	count_order_delivery BIGINT NOT NULL,
	count_order_done BIGINT NOT NULL,
	count_order_not_done BIGINT NOT NULL,
	report_period varchar NOT NULL,
	CONSTRAINT customer_report_datamart_pk PRIMARY KEY (id)
);


