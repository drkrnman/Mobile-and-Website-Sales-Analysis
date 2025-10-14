create view dm_transactions as
	with trans_prods_cte (booking_id, dist_prod_id, prods_cnt, has_mens_prods) 
	AS  
	(
	select 
		tp.booking_id as booking_id,
		count( distinct tp.product_id) as dist_prod_id,
		sum(tp.quantity ) as prods_cnt,
		max(case when p.gender = 'Men' then 1 else 0 end) as has_mens_prods
	from rd_transactions_prods tp 
		inner join rd_prods p on tp.product_id = p.prod_id 
	group by tp.booking_id 
	)
	SELECT
		t.created_at as created_dt
		, t.booking_id  as booking_id
		, t.customer_id as customer_id
		, t.session_id as session_id
		, t.payment_method as payment_method
		, t.payment_status as payment_status
		, t.promo_code as promo_code
		, t.promo_amount as promo_amount
		, t.shipment_fee as shipment_fee
		, t.shipment_date_limit as shipment_date_limit
		, t.total_amount as total_amount
		, dbo.fn_AmountCategory(t.total_amount) as total_amount_category
		, case when t.promo_amount>0 then 1 else 0 end as has_promo
		, DATEDIFF(DAY, t.created_at, t.shipment_date_limit) as days_to_delivery
		, dbo.fn_AgeCategory(c.birthdate, t.created_at) as customer_age_category
		, pcte.dist_prod_id as dist_prod_id
		, pcte.prods_cnt as prods_cnt
		, pcte.has_mens_prods as has_mens_prods
		, row_number() over (partition by t.customer_id order by t.created_at) as booking_number
	FROM rd_transactions t 
		inner join rd_customers c on t.customer_id = c.customer_id 
		inner join trans_prods_cte pcte on pcte.booking_id = t.booking_id 




