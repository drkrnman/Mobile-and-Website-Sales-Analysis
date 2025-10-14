create view dm_sessions as
select 
	 s.session_id as session_id
	,s.traffic_source as traffic_source
	,case when s.SEARCH_cnt >0 then 1 else 0 end as has_SEARCH
	,case when s.PROMO_PAGE_cnt >0 then 1 else 0 end as has_PROMO_PAGE_visit
	,case when s.ADD_PROMO_time is not null then 1 else 0 end as has_ADD_PROMO
	,case when s.CLICK_cnt >0 then 1 else 0 end as has_CLICK
	,case when s.HOMEPAGE_cnt >0 then 1 else 0 end as has_HOMEPAGE
	,case when s.ITEM_DETAIL_cnt >0 then 1 else 0 end as has_ITEM_DETAIL
	,case when s.SCROLL_cnt >0 then 1 else 0 end as has_SCROLL
	,case when s.ADD_TO_CART_cnt >0 then 1 else 0 end as has_ADD_TO_CART
	,case when s.BOOKING_time is not null then 1 else 0 end as has_BOOKING
	,s.session_events_cnt as session_events_cnt
	,s.session_start_time as session_start_time 
	,session_end_time as session_end_time
FROM rd_sessions s


