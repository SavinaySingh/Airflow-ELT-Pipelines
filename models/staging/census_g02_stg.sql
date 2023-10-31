{{
    config(
        unique_key='LGA_CODE_2016'
    )
}}
SELECT lga_code_2016,
       COALESCE(median_age_persons, 0)            AS Median_age_persons,
       COALESCE(median_mortgage_repay_monthly, 0) AS Median_mortgage_repay_monthly,
       COALESCE(median_tot_prsnl_inc_weekly, 0)   AS Median_tot_prsnl_inc_weekly,
       COALESCE(median_rent_weekly, 0)            AS Median_rent_weekly,
       COALESCE(median_tot_fam_inc_weekly, 0)     AS Median_tot_fam_inc_weekly,
       COALESCE(average_num_psns_per_bedroom, 0)  AS Average_num_psns_per_bedroom,
       COALESCE(median_tot_hhd_inc_weekly, 0)     AS Median_tot_hhd_inc_weekly,
       COALESCE(average_household_size, 0)        AS Average_household_size
FROM   "postgres"."raw"."census_g02" 