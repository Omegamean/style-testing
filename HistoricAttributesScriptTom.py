# -*- coding: utf-8 -*-
"""
Created on Tue Aug 20 11:10:31 2019

@author: Conor Glasman
"""


import psycopg2
import datetime
from dateutil.relativedelta import relativedelta  

def RedshiftInsert (attribute_date):
    redshift = psycopg2.connect(database = "*", user = "*"
                                ,password = "*"
                                ,host = "*"
                                , port = "*")
    cur = redshift.cursor()
    
    cur.execute("TRUNCATE TABLE di_staging.di_wrk_user_attributes_old")
    
    sql_table_insert = """
    INSERT INTO di_staging.di_wrk_user_attributes_old
    (       domain_user_id,
    	registration_datetime    ,
    	registration_date_id    ,
    	last_purch_date    ,
    	purchases    ,
    	user_commission    ,
    	job_run_datetime    ,
    	di_domain_id,
    	user_activity_classification_id ,
    	user_activity_classification_name,
    	user_cashback_band_id ,
    	user_cashback_band_name,
    	user_membership_stage_id ,
    	user_membership_stage_name
    	
    )
    SELECT
    a.domain_user_id,
    registration_datetime,
    registration_date_id,
    MAX(purchase_datetime) as last_purch_date,
    COUNT(di_purchase_id) as Purchases,
    SUM(user_commission) as User_Commission,
    CURRENT_DATE,
    a.di_domain_id,
    NULL as user_activity_classification_id,
    NULL as user_activity_classification_name,
    NULL as	user_cashback_band_id ,
    NULL as	user_cashback_band_name,
    NULL as	user_behavioural_stage_id ,
    NULL as	user_behavioural_stage_name
    FROM di_fact_purchase a
    
    INNER JOIN di_dim_user b
        ON a.domain_user_id=b.domain_user_id
        AND b.di_domain_id=1
        AND is_cobrand=0
        
    INNER JOIN di_dim_merchant c
     ON a.domain_merchant_id=c.domain_merchant_id
     AND merchant_hierarchy_l3_id<>17
     AND c.di_domain_id=1
                     
    WHERE is_gross = 1
    AND a.di_domain_id=1
    AND domain_network Not In ('QCO','RTV','VISA','COS','Seopa')
    AND purchase_datetime >= DATEADD(year,-1,'{attribute_insert_date}') 
    and purchase_datetime < '{attribute_insert_date}'
    and registration_datetime <= '{attribute_insert_date}'
    
    GROUP BY 1,2,3,8; 
    
    INSERT INTO di_staging.di_wrk_user_attributes_old
    (       domain_user_id,
    	registration_datetime    ,
    	registration_date_id    ,
    	last_purch_date    ,
    	purchases    ,
    	user_commission    ,
    	job_run_datetime    ,
    	di_domain_id,
    	user_activity_classification_id ,
    	user_activity_classification_name,
    	user_cashback_band_id ,
    	user_cashback_band_name,
    	user_membership_stage_id ,
    	user_membership_stage_name
    	
    )
    SELECT
    domain_user_id,
    registration_datetime,
    registration_date_id,
    null as last_purch_date,
    0 as Purchases,
    0 as User_Commission,
    CURRENT_DATE,
    di_domain_id,
    NULL as user_activity_classification_id,
    NULL as user_activity_classification_name,
    NULL as	user_cashback_band_id ,
    NULL as	user_cashback_band_name,
    NULL as	user_behavioural_stage_id ,
    NULL as	user_behavioural_stage_name
    from
    di_dim_user
    where di_domain_id=1
    AND is_cobrand=0
    and domain_user_id not in (select distinct domain_user_id from di_staging.di_wrk_user_attributes)
    and registration_datetime <= '{attribute_insert_date}'
    """.format(attribute_insert_date = attribute_date.strftime("%Y-%m-%d"))

    cur.execute(sql_table_insert)

    sql_table_update = """
    UPDATE  di_staging.di_wrk_user_attributes_old
    set user_activity_classification_id = 
    CASE 
    WHEN purchases = 0 THEN 1
    WHEN purchases = 1 THEN 2
    WHEN purchases >= 2 and purchases <=4 THEN 3
    WHEN purchases >= 5 and purchases <=11 THEN 4
    WHEN purchases >= 12 and purchases <=25 THEN 5
    WHEN purchases >= 26 and purchases <= 50 THEN 6
    WHEN purchases >= 51 THEN 7
    ELSE -1
    END;
    
    
    UPDATE  di_staging.di_wrk_user_attributes_old
    set user_activity_classification_name = 
    CASE 
    WHEN purchases = 0 THEN 'Onboarding'
    WHEN purchases = 1 THEN 'One Timer'
    WHEN purchases >= 2 and purchases <=4 THEN 'Lowly Engaged'
    WHEN purchases >= 5 and purchases <=11 THEN 'Engaged'
    WHEN purchases >= 12 and purchases <=25 THEN 'Highly Engaged'
    WHEN purchases >= 26 and purchases <= 50 THEN 'Super User'
    WHEN purchases >= 51 THEN 'Champion'
    ELSE 'ERROR' 
    END;
    
    UPDATE di_staging.di_wrk_user_attributes_old
    set user_cashback_band_id = 
    CASE 
    WHEN user_commission < 20 THEN 1
    WHEN user_commission >= 20 and user_commission < 50 THEN 2
    WHEN user_commission >= 50 and user_commission < 100 THEN 3
    WHEN user_commission >= 100 and user_commission < 200 THEN 4
    WHEN user_commission >= 200 THEN 5
    
    
    ELSE -1
    END;
    
    
    UPDATE di_staging.di_wrk_user_attributes_old
    set user_cashback_band_name = 
    CASE 
    WHEN user_commission < 20 THEN 'Blue'
    WHEN user_commission >= 20 and user_commission < 50 THEN 'Bronze'
    WHEN user_commission >= 50 and user_commission < 100 THEN 'Silver'
    WHEN user_commission >= 100 and user_commission < 200 THEN 'Gold'
    WHEN user_commission >= 200 THEN 'Platinum'
    
    
    ELSE 'ERROR' 
    END;
    """
    cur.execute(sql_table_update)
    
    sql_log_update = """
    UPDATE di_dim_user_attributes_log
    set valid_to_date = neu.valid_to_date_new
    from (
    select TO_DATE('{attribute_valid_date}','YYYY-MM-DD') as valid_to_date_new,
    log.domain_user_id, log.valid_from_date
    from di_warehouse.di_dim_user_attributes_log log 
    join di_staging.di_wrk_user_attributes_old  wrk on log.domain_user_id = wrk.domain_user_id
    where valid_to_date is null 
    and user_attribute = 'user_activity_classification_id'
    and log.attribute_value != wrk.user_activity_classification_id
    ) neu
    where di_dim_user_attributes_log.user_attribute = 'user_activity_classification_id'
    and neu.valid_to_date_new is not null
    and neu.domain_user_id = di_dim_user_attributes_log.domain_user_id
    and di_dim_user_attributes_log.valid_to_date is null;
    
    INSERT INTO di_dim_user_attributes_log
    select domain_user_id, 'user_activity_classification_id' as user_attribute  
    ,wrk.user_activity_classification_id as attribute_value,'{attribute_valid_date}' as valid_from, null as valid_to 
    from di_staging.di_wrk_user_attributes_old wrk
    where not exists (
    select * from di_warehouse.di_dim_user_attributes_log log 
    where wrk.domain_user_id=log.domain_user_id and log.user_attribute = 'user_activity_classification_id' and log.valid_to_date is null
    );
    
    UPDATE di_dim_user_attributes_log
    set valid_to_date = neu.valid_to_date_new
    from (
    select TO_DATE('{attribute_valid_date}','YYYY-MM-DD') as valid_to_date_new,
    log.domain_user_id, log.valid_from_date
    from di_warehouse.di_dim_user_attributes_log log 
    join di_staging.di_wrk_user_attributes_old  wrk on log.domain_user_id = wrk.domain_user_id
    where valid_to_date is null 
    and user_attribute = 'user_cashback_band_id'
    and log.attribute_value != wrk.user_cashback_band_id
    ) neu
    where di_dim_user_attributes_log.user_attribute = 'user_cashback_band_id'
    and neu.valid_to_date_new is not null
    and neu.domain_user_id = di_dim_user_attributes_log.domain_user_id
    and di_dim_user_attributes_log.valid_to_date is null;
    
    INSERT INTO di_dim_user_attributes_log
    select domain_user_id, 'user_cashback_band_id' as user_attribute  
    ,wrk.user_cashback_band_id as attribute_value,'{attribute_valid_date}' as valid_from, null as valid_to 
    from di_staging.di_wrk_user_attributes_old wrk
    where not exists (
    select * from di_warehouse.di_dim_user_attributes_log log 
    where wrk.domain_user_id=log.domain_user_id and log.user_attribute = 'user_cashback_band_id' and log.valid_to_date is null
    );
    """.format(attribute_valid_date = attribute_date.strftime("%Y-%m-%d") )
    
    cur.execute(sql_log_update)

    redshift.commit()
    cur.close
    redshift.close

def RedshiftLogBetweenDates( start_date, end_date):
    
    while start_date < end_date:
        RedshiftInsert(start_date)
        start_date = start_date + relativedelta(months=1)
    
attribute_start_date = datetime.datetime(2014,1,3)
attribute_end_date = datetime.datetime(2014,2,4)

RedshiftLogBetweenDates(attribute_start_date, attribute_end_date)
