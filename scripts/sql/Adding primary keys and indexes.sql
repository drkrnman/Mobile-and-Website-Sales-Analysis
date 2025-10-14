
------------------------------------------------
-- adding primary keys
------------------------------------------------

------------- rd_customers

SELECT COUNT(*) AS null_cnt
FROM rd_customers
WHERE customer_id IS NULL;
-- 0

ALTER TABLE rd_customers
ALTER COLUMN customer_id BIGINT NOT NULL;
--refresh

ALTER TABLE rd_customers
ADD CONSTRAINT PK_Customers PRIMARY KEY (customer_id);
-- check if exists


------------- rd_events_add_to_cart

SELECT COUNT(*) AS null_cnt
FROM rd_events_add_to_cart
WHERE event_id IS NULL;

SELECT count(*) - count(distinct q.event_id) as diff
FROM rd_events_add_to_cart q;
-- 0 0

SELECT MAX(LEN(event_id)) AS max_len
FROM rd_events_add_to_cart;
--36

ALTER TABLE rd_events_add_to_cart
ALTER COLUMN event_id nvarchar(36) NOT NULL;
--refresh

ALTER TABLE rd_events_add_to_cart
ADD CONSTRAINT PK_event_id PRIMARY KEY (event_id);
-- check if exists


------------- rd_prods

SELECT COUNT(*) AS null_cnt
FROM rd_prods
WHERE prod_id IS NULL;

SELECT count(*) - count(distinct q.prod_id) as diff
FROM rd_prods q;
-- 0 0

ALTER TABLE rd_prods
ALTER COLUMN prod_id bigint NOT NULL;
--refresh

ALTER TABLE rd_prods
ADD CONSTRAINT PK_prod_id PRIMARY KEY (prod_id);
-- check if exists


------------- rd_sessions

SELECT COUNT(*) AS null_cnt
FROM rd_sessions
WHERE session_id IS NULL;
SELECT count(*) - count(distinct q.session_id) as diff
FROM rd_sessions q;
-- 0 0

SELECT MAX(LEN(session_id)) AS max_len
FROM rd_sessions;
--36

ALTER TABLE rd_sessions
ALTER COLUMN session_id nvarchar(36) NOT NULL;
--refresh

ALTER TABLE rd_sessions
ADD CONSTRAINT PK_session_id PRIMARY KEY (session_id);
-- check if exists


------------- rd_transactions

SELECT COUNT(*) AS null_cnt
FROM rd_transactions
WHERE booking_id IS NULL;
SELECT count(*) - count(distinct q.booking_id) as diff
FROM rd_transactions q;
-- 0 0

SELECT MAX(LEN(booking_id)) AS max_len
FROM rd_transactions;
--36

ALTER TABLE rd_transactions
ALTER COLUMN booking_id nvarchar(36) NOT NULL;
--refresh

ALTER TABLE rd_transactions
ADD CONSTRAINT PK_booking_id PRIMARY KEY (booking_id);
-- check if exists


------------- rd_transactions_prods

ALTER TABLE rd_transactions_prods
ADD id BIGINT IDENTITY(1,1) NOT NULL; -- adding autoincrement

ALTER TABLE rd_transactions_prods
ADD CONSTRAINT PK_rd_transactions_prods PRIMARY KEY (id);



-----------------------------
---------------adding indexes
-----------------------------

--- rd_events_add_to_cart

ALTER TABLE rd_events_add_to_cart
ALTER COLUMN session_id nvarchar(36) NOT NULL;
--refresh

CREATE INDEX ind_rd_events_add_to_cart_session_id
ON rd_events_add_to_cart(session_id);
-- refresh, check if exists


--- rd_transactions

ALTER TABLE rd_transactions
ALTER COLUMN session_id nvarchar(36) NOT NULL;
--refresh

CREATE INDEX ind_rd_transactions_session_id
ON rd_transactions(session_id);
-- refresh, check if exists

CREATE INDEX ind_rd_transactions_customer_id
ON rd_transactions(customer_id);
-- refresh, check if exists


--- rd_transactions_prods

CREATE INDEX ind_rd_transactions_prods_booking_id
ON rd_transactions_prods(booking_id);
-- refresh, check if exists

CREATE INDEX ind_rd_transactions_prods_product_id
ON rd_transactions_prods(product_id);
-- refresh, check if exists










