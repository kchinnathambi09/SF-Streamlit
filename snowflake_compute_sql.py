import pandas as pd
import streamlit as st
import snowflake_connection as conn

CostSpent_YTD            =      """
                                    select 
                                        round(sum(credits_used * {}), 2) as Cost_per_warehouse 
                                    from account_usage.warehouse_metering_history;
                                """ 

CostSpent_curryear      =       """
                                    select 
                                        round(sum(credits_used * {}), 2) as Cost_per_warehouse 
                                    from account_usage.warehouse_metering_history
                                    where year(start_time) = year(current_date);
                                """

qtd_metrics =    """
                        select  
                            quarter(start_time) as qtr,
                            year(start_time) as year_num,
                            round((sum(credits_used) * {}), 2) as compute_cost_qtd
                        from account_usage.warehouse_metering_history 
                        group by 1, 2
                        order by 2 desc, 1 asc;
                    """    

date_details =   """ select 
                            month(current_date) as current_mth,
                            month(add_months(current_date, -1)) as last_mth,
                            quarter(current_date) as current_qtr, 
                            year(current_date) as current_yr;
                    """    

mtd_metrics     =   """
                      select 
                            month(start_time) as mth,
                            year(start_time) as year_num,
                            concat( monthname(start_time) , '/', year_num) as time_in_months,
                            monthname(start_time) as monthname,
                            round(sum(credits_used), 2) as Credits_consumed_MTD,  
                            round((sum(credits_used) * {}), 2) as Overall_Cost_MTD,
                            round((sum(credits_used_cloud_services) * {}),2) as Cloudservices_cost_MTD,
                            round((sum(credits_used_compute) * {}),2) as Compute_cost_MTD 
                        from account_usage.warehouse_metering_history
                        group by 1, 2, 3, 4
                        order by 2 desc, 1 desc;                           
                    """

Overall_cost    =   """
                        with compute_cost as (
                            select 
                                month(start_time) as mth,
                                year(start_time) as year_num,
                                monthname(start_time) as monthname, 
                                concat(monthname,'/', year_num)  as time_in_months,                                 
                                round((sum(credits_used) * {}), 2) as cost
                                --round((sum(credits_used_cloud_services) * 3),2) as Cloudservices_cost_MTD,
                                --round((sum(credits_used_compute) * 3),2) as Compute_cost_MTD 
                            from account_usage.warehouse_metering_history
                            group by 1, 2, 3, 4
                            order by 2 desc, 1 desc                          

                        ),

                        storage_cost as (
                        select
                        month(usage_date) as mth,
                        year(usage_date) as year_num,
                        monthname(usage_date) as monthname,
                        concat(monthname,'/', year_num)  as time_in_months,                                 
                        round(sum(storage_bytes + stage_bytes + failsafe_bytes) / (power(1024, 4)), 0) as billable_tb, 
                        billable_tb * {} as cost
                        from account_usage.storage_usage
                        group by 1, 2, 3, 4
                        order by 2 desc, 1 desc

                        )

                        select mth, year_num, time_in_months, monthname, cost, 'Compute' as category
                        from compute_cost
                        union
                        select mth, year_num, time_in_months, monthname, cost, 'Storage' as category
                        from storage_cost
                        order by 6 desc, 2 desc, 1 desc;
                    """

hours_by_day    =   """
                      select 
                            case 
                                when len(day(start_time)) < 2
                                then concat('0',day(start_time), '/', month(start_time)) 
                                when len(day(start_time)) = 2
                                then concat(day(start_time), '/', month(start_time)) 
                            end as Date, 
                            date(start_time) as d,
                            hour(start_time) as Hours, 
                            round((sum(credits_used) * 3), 5) as cost_spent
                        from account_usage.warehouse_metering_history
                        where date(start_time) > dateadd( 'days', -7, current_date )
                        group by 2, 1, 3
                        order by 2  desc, 3 desc;
                    """

warehouse_spend_top10   =   """
                                select top 10
                                    warehouse_name,
                                    round((sum(credits_used) * {}), 2) as OverallCost
                                from account_usage.warehouse_metering_history 
                                where credits_used <> '0.000000000'
                                group by 1
                                having OverallCost <> 0                            
                                order by 2 desc;  
                            """

cost_split              =   """
                                with cloudservice_cost as (
                                    select
                                        'cloudservice cost' as category,
                                        round((sum(credits_used_cloud_services) * {}),2) as cost_spent
                                    from account_usage.warehouse_metering_history
                                ),
                                computeservice_cost as (
                                    select
                                        'compute service cost' as category,
                                        round((sum(credits_used_compute) * {}),2) as cost_spent
                                    from account_usage.warehouse_metering_history
                                ),
                                storage_cost as (
                                    select
                                        'storage cost' as  category,
                                        round(sum(storage_bytes + stage_bytes + failsafe_bytes) / (power(1024, 4)), 2) * {} as cost_spent
                                    from account_usage.storage_usage
                                )
                            select * from cloudservice_cost
                            union
                            select * from computeservice_cost
                            union
                            select * from storage_cost; 
                    """                    
