DROP TABLE IF EXISTS dwh.load_dates_customer_report_datamart;

CREATE TABLE IF NOT EXISTS dwh.load_dates_customer_report_datamart (
    id bigint GENERATED ALWAYS AS IDENTITY NOT NULL,
    load_dttm date NOT NULL,
    CONSTRAINT load_dates_customer_report_datamart_pk PRIMARY KEY (id)
);

WITH
load_date AS (-- last update date of datamart
	SELECT 
		COALESCE(MAX(load_dttm),'1900-01-01') AS last_date
	FROM 
		dwh.load_dates_customer_report_datamart
),

top_craftsman AS (
	SELECT 
		top_cr.customer_id AS customer_id,
		top_cr.craftsman_id AS craftsman_id
	FROM
		(SELECT
			fo.customer_id,
			fo.craftsman_id,
			ROW_NUMBER() OVER (PARTITION BY fo.customer_id ORDER BY COUNT(*) DESC) row_rank
		FROM 
			dwh.f_order fo
		GROUP BY
			fo.customer_id,
			fo.craftsman_id
		) top_cr
	WHERE top_cr.row_rank = 1
),

top_product_type AS (-- top product type of every customer_id
	SELECT 
		top_prod.customer_id AS customer_id,
		top_prod.product_type AS product_type
	FROM
		(SELECT
			fo.customer_id as customer_id,
			prod.product_type as product_type,
			ROW_NUMBER() OVER (PARTITION BY fo.customer_id ORDER BY COUNT(*) DESC) AS row_rank
		FROM 
			dwh.f_order fo
			INNER join dwh.d_product prod ON fo.product_id = prod.product_id
		GROUP BY
			fo.customer_id,
			prod.product_type
		) top_prod
	WHERE top_prod.row_rank = 1
),

dwh_delta AS ( -- total delta: new customers and new orders of existing customers
    SELECT   
		cu.customer_id AS customer_id,
		cu.customer_name AS customer_name,
		cu.customer_address AS customer_address,
		cu.customer_birthday AS customer_birthday,
		cu.customer_email AS customer_email,
		crd.customer_id AS exist_customer_id,
		fo.order_id AS order_id,
		prod.product_id AS product_id,
		prod.product_price AS product_price,
		prod.product_type AS product_type,
		fo.order_completion_date - fo.order_created_date AS diff_order_date, 
		fo.order_status AS order_status,
		TO_CHAR(fo.order_created_date, 'yyyy-mm') AS report_period,
		cr.load_dttm AS craftsman_load_dttm,
		cu.load_dttm AS customer_load_dttm,
		prod.load_dttm AS product_load_dttm
	FROM dwh.f_order fo
		INNER JOIN dwh.d_craftsman cr ON fo.craftsman_id = cr.craftsman_id
		INNER JOIN dwh.d_customer cu ON fo.customer_id = cu.customer_id
		INNER JOIN dwh.d_product prod ON fo.product_id = prod.product_id
		LEFT JOIN dwh.customer_report_datamart crd ON fo.customer_id = crd.customer_id
	WHERE 
		(fo.load_dttm > (SELECT COALESCE(MAX(load_dttm),'1900-01-01') FROM dwh.load_dates_customer_report_datamart)) OR
		(cr.load_dttm > (SELECT COALESCE(MAX(load_dttm),'1900-01-01') FROM dwh.load_dates_customer_report_datamart)) OR
		(cu.load_dttm > (SELECT COALESCE(MAX(load_dttm),'1900-01-01') FROM dwh.load_dates_customer_report_datamart)) OR
		(prod.load_dttm > (SELECT COALESCE(MAX(load_dttm),'1900-01-01') FROM dwh.load_dates_customer_report_datamart))
),

dwh_update_delta AS ( -- generate list of existing customer_id for updating in datamart
    SELECT     
        DISTINCT exist_customer_id AS customer_id
    FROM 
		dwh_delta 
    WHERE 
		exist_customer_id IS NOT NULL        
),

dwh_delta_insert_result AS ( -- делаем расчёт витрины по новым клиентам. Их можно просто вставить (insert) в витрину без обновления
    SELECT -- в этой выборке делаем расчёт по большинству столбцов, так как все они требуют одной и той же группировки, кроме столбца с самой популярной категорией товаров у мастера. Для этого столбца сделаем отдельную выборку с другой группировкой и выполним JOIN
		del.customer_id AS customer_id,
		del.customer_name AS customer_name,
		del.customer_address AS customer_address,
		del.customer_birthday AS customer_birthday,
		del.customer_email AS customer_email,
		SUM(del.product_price) AS customer_money,
		SUM(del.product_price) * 0.1 AS platform_money,
		COUNT(del.order_id) AS count_order,
		AVG(del.product_price) AS avg_price_order,
		top_prod.product_type AS top_product_type,
		top_cr.craftsman_id AS top_craftsman_id,
		PERCENTILE_CONT(0.5) WITHIN GROUP(ORDER BY diff_order_date) AS median_time_order_completed,
		SUM(CASE WHEN del.order_status = 'created' THEN 1 ELSE 0 END) AS count_order_created,
		SUM(CASE WHEN del.order_status = 'in progress' THEN 1 ELSE 0 END) AS count_order_in_progress,
		SUM(CASE WHEN del.order_status = 'delivery' THEN 1 ELSE 0 END) AS count_order_delivery, 
		SUM(CASE WHEN del.order_status = 'done' THEN 1 ELSE 0 END) AS count_order_done, 
		SUM(CASE WHEN del.order_status != 'done' THEN 1 ELSE 0 END) AS count_order_not_done,
		del.report_period AS report_period
	FROM dwh_delta del
	INNER JOIN top_craftsman top_cr ON del.customer_id = top_cr.customer_id
	INNER JOIN top_product_type top_prod ON del.customer_id = top_prod.customer_id
	WHERE 
		del.exist_customer_id IS NULL
	GROUP BY 
		del.customer_id, 
		del.customer_name, 
		del.customer_address, 
		del.customer_birthday, 
		del.customer_email, 
		top_prod.product_type,
		top_cr.craftsman_id,
		del.report_period
),

dwh_delta_update_result AS ( -- делаем перерасчёт для существующих записей витрины, так как данные обновились за отчётные периоды. Логика похожа на insert, но нужно достать конкретные данные из DWH
	SELECT     -- в этой выборке достаём из DWH обновлённые или новые данные по клиентам, которые уже есть в витрине
		cu.customer_id AS customer_id,
		cu.customer_name AS customer_name,
		cu.customer_address AS customer_address,
		cu.customer_birthday AS customer_birthday,
		cu.customer_email AS customer_email,
		SUM(prod.product_price) AS customer_money,
		SUM(prod.product_price)*0.1 AS platform_money,
		COUNT(fo.order_id) AS count_order,
		AVG(prod.product_price) AS avg_price_order,
		PERCENTILE_CONT(0.5) WITHIN GROUP(ORDER BY (fo.order_completion_date - fo.order_created_date)) AS median_time_order_completed,
		top_prod.product_type AS top_product_type,
		top_cr.craftsman_id AS top_craftsman_id,
		SUM(CASE WHEN fo.order_status = 'created' THEN 1 ELSE 0 END) AS count_order_created, 
		SUM(CASE WHEN fo.order_status = 'in progress' THEN 1 ELSE 0 END) AS count_order_in_progress, 
		SUM(CASE WHEN fo.order_status = 'delivery' THEN 1 ELSE 0 END) AS count_order_delivery, 
		SUM(CASE WHEN fo.order_status = 'done' THEN 1 ELSE 0 END) AS count_order_done, 
		SUM(CASE WHEN fo.order_status != 'done' THEN 1 ELSE 0 END) AS count_order_not_done,
		TO_CHAR(fo.order_created_date, 'yyyy-mm') AS report_period
	FROM dwh.f_order fo 
	INNER JOIN dwh.d_customer cu ON fo.customer_id = cu.customer_id
	INNER JOIN dwh_update_delta upd ON fo.customer_id = upd.customer_id
	INNER JOIN dwh.d_product prod ON fo.product_id = prod.product_id
	INNER JOIN top_craftsman top_cr ON fo.customer_id = top_cr.customer_id
	INNER JOIN top_product_type top_prod ON fo.customer_id = top_prod.customer_id
	GROUP BY 
		cu.customer_id, 
		cu.customer_name, 
		cu.customer_address, 
		cu.customer_birthday, 
		cu.customer_email, 
		top_prod.product_type,
		top_cr.craftsman_id,
		TO_CHAR(fo.order_created_date, 'yyyy-mm')
),

insert_delta AS ( -- insert new data into datamart
    INSERT INTO dwh.customer_report_datamart (
        customer_id,
		customer_name,
		customer_address,
		customer_birthday,
		customer_email,
		customer_money,
		platform_money,
		count_order,
		avg_price_order,
		median_time_order_completed,
		top_product_type,
		top_craftsman_id,
		count_order_created,
		count_order_in_progress,
		count_order_delivery,
		count_order_done,
		count_order_not_done,
		report_period		
    ) SELECT 		
		customer_id,
		customer_name,
		customer_address,
		customer_birthday,
		customer_email,
		customer_money,
		platform_money,
		count_order,
		avg_price_order,
		median_time_order_completed,
		top_product_type,
		top_craftsman_id,
		count_order_created,
		count_order_in_progress, 
		count_order_delivery, 
		count_order_done, 
		count_order_not_done,
		report_period
    FROM dwh_delta_insert_result
),

update_delta AS ( -- update existing customers in datamart
    UPDATE dwh.customer_report_datamart SET
		customer_name = upd.customer_name,
		customer_address = upd.customer_address,
		customer_birthday = upd.customer_birthday,
		customer_email = upd.customer_email,
		customer_money = upd.customer_money,
		platform_money = upd.platform_money,
		count_order = upd.count_order,
		avg_price_order = upd.avg_price_order,
		median_time_order_completed = upd.median_time_order_completed,
		top_product_type = upd.top_product_type,
		top_craftsman_id = upd.top_craftsman_id,
		count_order_created = upd.count_order_created,
		count_order_in_progress = upd.count_order_in_progress, 
		count_order_delivery = upd.count_order_delivery, 
		count_order_done = upd.count_order_done, 
		count_order_not_done = upd.count_order_not_done,
		report_period = upd.report_period
    FROM (
        SELECT 
            customer_id,
			customer_name,
			customer_address,
			customer_birthday,
			customer_email,
			customer_money,
			platform_money,
			count_order,
			avg_price_order,
			median_time_order_completed,
			top_product_type,
			top_craftsman_id,
			count_order_created,
			count_order_in_progress, 
			count_order_delivery, 
			count_order_done, 
			count_order_not_done,
			report_period
        FROM dwh_delta_update_result) upd
    WHERE dwh.customer_report_datamart.customer_id = upd.customer_id
),

insert_load_date AS ( -- insert new update date
    INSERT INTO dwh.load_dates_customer_report_datamart (load_dttm)
    SELECT 
		GREATEST(COALESCE(MAX(craftsman_load_dttm), NOW()), 
                 COALESCE(MAX(customer_load_dttm), NOW()),
                 COALESCE(MAX(product_load_dttm), NOW()))
    FROM dwh_delta
)

SELECT 'Updating Datamart dwh.customer_report_datamart';