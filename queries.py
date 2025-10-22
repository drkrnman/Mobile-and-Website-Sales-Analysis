
t_test_queries = {
    1: {
        'name': 'Средний чек',
        'sql_male_female': """
            SELECT 
                c.gender AS group_name,
                AVG(t.total_amount) AS mean,
                STDEV(t.total_amount) AS stddev,
                COUNT(*) AS count
            FROM rd_transactions t
            JOIN rd_customers c ON t.customer_id = c.customer_id
            WHERE c.gender IN ('M', 'F')
            GROUP BY c.gender
        """,
        'sql_web_mobile': """
            SELECT 
                s.traffic_source AS group_name,
                AVG(t.total_amount) AS mean,
                STDEV(t.total_amount) AS stddev,
                COUNT(*) AS count
            FROM rd_transactions t
            JOIN rd_sessions s ON t.session_id = s.session_id
            WHERE s.traffic_source IN ('WEB', 'MOBILE')
            GROUP BY s.traffic_source
        """
    },
    2: {
        'name': 'Количество товаров в заказе',
        'sql_male_female': """
            WITH prods AS (
                SELECT 
                    t.booking_id,
                    t.customer_id,
                    SUM(tp.quantity) AS total_quantity
                FROM rd_transactions t
                JOIN rd_transactions_prods tp ON t.booking_id = tp.booking_id
                GROUP BY t.booking_id, t.customer_id
            )
            SELECT 
                c.gender AS group_name,
                AVG(CAST(p.total_quantity AS FLOAT)) AS mean,
                STDEV(p.total_quantity) AS stddev,
                COUNT(*) AS count
            FROM prods p
            JOIN rd_customers c ON p.customer_id = c.customer_id
            WHERE c.gender IN ('M', 'F')
            GROUP BY c.gender
        """,
        'sql_web_mobile': """
            WITH prods AS (
                SELECT 
                    t.booking_id,
                    t.session_id,
                    SUM(tp.quantity) AS total_quantity
                FROM rd_transactions t
                JOIN rd_transactions_prods tp ON t.booking_id = tp.booking_id
                GROUP BY t.booking_id, t.session_id
            )
            SELECT 
                s.traffic_source AS group_name,
                AVG(CAST(p.total_quantity AS FLOAT)) AS mean,
                STDEV(p.total_quantity) AS stddev,
                COUNT(*) AS count
            FROM prods p
            JOIN rd_sessions s ON p.session_id = s.session_id
            WHERE s.traffic_source IN ('WEB', 'MOBILE')
            GROUP BY s.traffic_source
        """
    },
    3: {
        'name': 'Количество кликов до букинга',
        'sql_male_female': """
            SELECT 
                c.gender AS group_name,
                AVG(CAST(s.CLICK_cnt AS FLOAT)) AS mean,
                STDEV(s.CLICK_cnt) AS stddev,
                COUNT(*) AS count
            FROM rd_sessions s
            JOIN rd_transactions t ON s.session_id = t.session_id
            JOIN rd_customers c ON t.customer_id = c.customer_id
            WHERE c.gender IN ('M', 'F')
            GROUP BY c.gender
        """,
        'sql_web_mobile': """
            SELECT 
                traffic_source AS group_name,
                AVG(CAST(CLICK_cnt AS FLOAT)) AS mean,
                STDEV(CLICK_cnt) AS stddev,
                COUNT(*) AS count
            FROM rd_sessions
            WHERE traffic_source IN ('WEB', 'MOBILE')
            GROUP BY traffic_source
        """

    },
    4: {
        'name': 'Средняя стоимость доставки',
        'sql_male_female': """
            SELECT 
                c.gender AS group_name,
                AVG(t.shipment_fee) AS mean,
                STDEV(t.shipment_fee) AS stddev,
                COUNT(*) AS count
            FROM rd_transactions t
            JOIN rd_customers c ON t.customer_id = c.customer_id
            WHERE c.gender IN ('M', 'F')
            GROUP BY c.gender
        """,
        'sql_web_mobile': """
            SELECT 
                s.traffic_source AS group_name,
                AVG(t.shipment_fee) AS mean,
                STDEV(t.shipment_fee) AS stddev,
                COUNT(*) AS count
            FROM rd_transactions t
            JOIN rd_sessions s ON t.session_id = s.session_id
            WHERE s.traffic_source IN ('WEB', 'MOBILE')
            GROUP BY s.traffic_source
        """
    },
    5: {
        'name': 'Количество уникальных товаров',
        'sql_male_female': """
            WITH unique_prods AS (
                SELECT 
                    t.booking_id,
                    t.customer_id,
                    t.session_id,
                    COUNT(DISTINCT tp.product_id) AS unique_prods
                FROM rd_transactions t
                JOIN rd_transactions_prods tp ON t.booking_id = tp.booking_id
                GROUP BY t.booking_id, t.customer_id, t.session_id
            )
            SELECT 
                c.gender AS group_name,
                AVG(CAST(u.unique_prods AS FLOAT)) AS mean,
                STDEV(u.unique_prods) AS stddev,
                COUNT(*) AS count
            FROM unique_prods u
            JOIN rd_customers c ON u.customer_id = c.customer_id
            WHERE c.gender IN ('M', 'F')
            GROUP BY c.gender
        """,
        'sql_web_mobile': """
            WITH unique_prods AS (
                SELECT 
                    t.booking_id,
                    t.session_id,
                    COUNT(DISTINCT tp.product_id) AS unique_prods
                FROM rd_transactions t
                JOIN rd_transactions_prods tp ON t.booking_id = tp.booking_id
                GROUP BY t.booking_id, t.session_id
            )
            SELECT 
                s.traffic_source AS group_name,
                AVG(CAST(u.unique_prods AS FLOAT)) AS mean,
                STDEV(u.unique_prods) AS stddev,
                COUNT(*) AS count
            FROM unique_prods u
            JOIN rd_sessions s ON u.session_id = s.session_id
            WHERE s.traffic_source IN ('WEB', 'MOBILE')
            GROUP BY s.traffic_source
        """
    }
}


chi_square_queries = {
    1:{
        'sql_male_female':"""
        SELECT 
            t.payment_method AS payment_method,
            c.gender AS gender, 
            c.customer_id AS customer_id, 
            COUNT(*) AS transactions_cnt 
        FROM dm_transactions t INNER JOIN rd_customers c ON t.customer_id = c.customer_id 
        WHERE c.gender IN ('M','F') 
        GROUP BY t.payment_method, c.gender, c.customer_id"""
    },

    2:
    {
        'sql_web_mobile': """
        SELECT t.payment_method AS payment_method, 
            s.traffic_source AS traffic_source, 
            c.customer_id AS customer_id, 
            COUNT(*) AS transactions_cnt 
        FROM rd_transactions t 
        INNER JOIN rd_sessions s ON t.session_id = s.session_id 
        INNER JOIN rd_customers c ON t.customer_id = c.customer_id 
        WHERE s.traffic_source IN ('WEB','MOBILE') 
        GROUP BY t.payment_method, s.traffic_source, c.customer_id"""
        }
    }