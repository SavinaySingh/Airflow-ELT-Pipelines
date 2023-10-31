{{
    config(
        unique_key='LGA_CODE_2016'
    )
}}

SELECT CASE
         WHEN lga_code_2016 IS NULL
               OR lga_code_2016 = 'NaN' THEN '0'
         ELSE lga_code_2016
       END                                        AS lga_code_2016,
       COALESCE(tot_p_m, 0)                       AS Tot_P_M,
       COALESCE(tot_p_f, 0)                       AS Tot_P_F,
       COALESCE(tot_p_p, 0)                       AS Tot_P_P,
       COALESCE(age_0_4_yr_m, 0)                  AS Age_0_4_yr_M,
       COALESCE(age_0_4_yr_f, 0)                  AS Age_0_4_yr_F,
       COALESCE(age_0_4_yr_p, 0)                  AS Age_0_4_yr_P,
       COALESCE(age_5_14_yr_m, 0)                 AS Age_5_14_yr_M,
       COALESCE(age_5_14_yr_f, 0)                 AS Age_5_14_yr_F,
       COALESCE(age_5_14_yr_p, 0)                 AS Age_5_14_yr_P,
       COALESCE(age_15_19_yr_m, 0)                AS Age_15_19_yr_M,
       COALESCE(age_15_19_yr_f, 0)                AS Age_15_19_yr_F,
       COALESCE(age_15_19_yr_p, 0)                AS Age_15_19_yr_P,
       COALESCE(age_20_24_yr_m, 0)                AS Age_20_24_yr_M,
       COALESCE(age_20_24_yr_f, 0)                AS Age_20_24_yr_F,
       COALESCE(age_20_24_yr_p, 0)                AS Age_20_24_yr_P,
       COALESCE(age_25_34_yr_m, 0)                AS Age_25_34_yr_M,
       COALESCE(age_25_34_yr_f, 0)                AS Age_25_34_yr_F,
       COALESCE(age_25_34_yr_p, 0)                AS Age_25_34_yr_P,
       COALESCE(age_35_44_yr_m, 0)                AS Age_35_44_yr_M,
       COALESCE(age_35_44_yr_f, 0)                AS Age_35_44_yr_F,
       COALESCE(age_35_44_yr_p, 0)                AS Age_35_44_yr_P,
       COALESCE(age_45_54_yr_m, 0)                AS Age_45_54_yr_M,
       COALESCE(age_45_54_yr_f, 0)                AS Age_45_54_yr_F,
       COALESCE(age_45_54_yr_p, 0)                AS Age_45_54_yr_P,
       COALESCE(age_55_64_yr_m, 0)                AS Age_55_64_yr_M,
       COALESCE(age_55_64_yr_f, 0)                AS Age_55_64_yr_F,
       COALESCE(age_55_64_yr_p, 0)                AS Age_55_64_yr_P,
       COALESCE(age_65_74_yr_m, 0)                AS Age_65_74_yr_M,
       COALESCE(age_65_74_yr_f, 0)                AS Age_65_74_yr_F,
       COALESCE(age_65_74_yr_p, 0)                AS Age_65_74_yr_P,
       COALESCE(age_75_84_yr_m, 0)                AS Age_75_84_yr_M,
       COALESCE(age_75_84_yr_f, 0)                AS Age_75_84_yr_F,
       COALESCE(age_75_84_yr_p, 0)                AS Age_75_84_yr_P,
       COALESCE(age_85ov_m, 0)                    AS Age_85ov_M,
       COALESCE(age_85ov_f, 0)                    AS Age_85ov_F,
       COALESCE(age_85ov_p, 0)                    AS Age_85ov_P,
       COALESCE(counted_census_night_home_m, 0)   AS Counted_Census_Night_home_M
       ,
       COALESCE(counted_census_night_home_f, 0)   AS
       Counted_Census_Night_home_F,
       COALESCE(counted_census_night_home_p, 0)   AS Counted_Census_Night_home_P
       ,
       COALESCE(count_census_nt_ewhere_aust_m, 0) AS
       Count_Census_Nt_Ewhere_Aust_M,
       COALESCE(count_census_nt_ewhere_aust_f, 0) AS
       Count_Census_Nt_Ewhere_Aust_F,
       COALESCE(count_census_nt_ewhere_aust_p, 0) AS
       Count_Census_Nt_Ewhere_Aust_P,
       COALESCE(indigenous_psns_aboriginal_m, 0)  AS
       Indigenous_psns_Aboriginal_M,
       COALESCE(indigenous_psns_aboriginal_f, 0)  AS
       Indigenous_psns_Aboriginal_F,
       COALESCE(indigenous_psns_aboriginal_p, 0)  AS
       Indigenous_psns_Aboriginal_P,
       COALESCE(indig_psns_torres_strait_is_m, 0) AS
       Indig_psns_Torres_Strait_Is_M,
       COALESCE(indig_psns_torres_strait_is_f, 0) AS
       Indig_psns_Torres_Strait_Is_F,
       COALESCE(indig_psns_torres_strait_is_p, 0) AS
       Indig_psns_Torres_Strait_Is_P,
       COALESCE(indig_bth_abor_torres_st_is_m, 0) AS
       Indig_Bth_Abor_Torres_St_Is_M,
       COALESCE(indig_bth_abor_torres_st_is_f, 0) AS
       Indig_Bth_Abor_Torres_St_Is_F,
       COALESCE(indig_bth_abor_torres_st_is_p, 0) AS
       Indig_Bth_Abor_Torres_St_Is_P,
       COALESCE(indigenous_p_tot_m, 0)            AS Indigenous_P_Tot_M,
       COALESCE(indigenous_p_tot_f, 0)            AS Indigenous_P_Tot_F,
       COALESCE(indigenous_p_tot_p, 0)            AS Indigenous_P_Tot_P,
       COALESCE(birthplace_australia_m, 0)        AS Birthplace_Australia_M,
       COALESCE(birthplace_australia_f, 0)        AS Birthplace_Australia_F,
       COALESCE(birthplace_australia_p, 0)        AS Birthplace_Australia_P,
       COALESCE(birthplace_elsewhere_m, 0)        AS Birthplace_Elsewhere_M,
       COALESCE(birthplace_elsewhere_f, 0)        AS Birthplace_Elsewhere_F,
       COALESCE(birthplace_elsewhere_p, 0)        AS Birthplace_Elsewhere_P,
       COALESCE(lang_spoken_home_eng_only_m, 0)   AS Lang_spoken_home_Eng_only_M
       ,
       COALESCE(lang_spoken_home_eng_only_f, 0)   AS
       Lang_spoken_home_Eng_only_F,
       COALESCE(lang_spoken_home_eng_only_p, 0)   AS Lang_spoken_home_Eng_only_P
       ,
       COALESCE(lang_spoken_home_oth_lang_m, 0)   AS
       Lang_spoken_home_Oth_Lang_M,
       COALESCE(lang_spoken_home_oth_lang_f, 0)   AS Lang_spoken_home_Oth_Lang_F
       ,
       COALESCE(lang_spoken_home_oth_lang_p, 0)   AS
       Lang_spoken_home_Oth_Lang_P,
       COALESCE(australian_citizen_m, 0)          AS Australian_citizen_M,
       COALESCE(australian_citizen_f, 0)          AS Australian_citizen_F,
       COALESCE(australian_citizen_p, 0)          AS Australian_citizen_P,
       COALESCE(age_psns_att_educ_inst_0_4_m, 0)  AS
       Age_psns_att_educ_inst_0_4_M,
       COALESCE(age_psns_att_educ_inst_0_4_f, 0)  AS
       Age_psns_att_educ_inst_0_4_F,
       COALESCE(age_psns_att_educ_inst_0_4_p, 0)  AS
       Age_psns_att_educ_inst_0_4_P,
       COALESCE(age_psns_att_educ_inst_5_14_m, 0) AS
       Age_psns_att_educ_inst_5_14_M,
       COALESCE(age_psns_att_educ_inst_5_14_f, 0) AS
       Age_psns_att_educ_inst_5_14_F,
       COALESCE(age_psns_att_educ_inst_5_14_p, 0) AS
       Age_psns_att_educ_inst_5_14_P,
       COALESCE(age_psns_att_edu_inst_15_19_m, 0) AS
       Age_psns_att_edu_inst_15_19_M,
       COALESCE(age_psns_att_edu_inst_15_19_f, 0) AS
       Age_psns_att_edu_inst_15_19_F,
       COALESCE(age_psns_att_edu_inst_15_19_p, 0) AS
       Age_psns_att_edu_inst_15_19_P,
       COALESCE(age_psns_att_edu_inst_20_24_m, 0) AS
       Age_psns_att_edu_inst_20_24_M,
       COALESCE(age_psns_att_edu_inst_20_24_f, 0) AS
       Age_psns_att_edu_inst_20_24_F,
       COALESCE(age_psns_att_edu_inst_20_24_p, 0) AS
       Age_psns_att_edu_inst_20_24_P,
       COALESCE(age_psns_att_edu_inst_25_ov_m, 0) AS
       Age_psns_att_edu_inst_25_ov_M,
       COALESCE(age_psns_att_edu_inst_25_ov_f, 0) AS
       Age_psns_att_edu_inst_25_ov_F,
       COALESCE(age_psns_att_edu_inst_25_ov_p, 0) AS
       Age_psns_att_edu_inst_25_ov_P,
       COALESCE(high_yr_schl_comp_yr_12_eq_m, 0)  AS
       High_yr_schl_comp_Yr_12_eq_M,
       COALESCE(high_yr_schl_comp_yr_12_eq_f, 0)  AS
       High_yr_schl_comp_Yr_12_eq_F,
       COALESCE(high_yr_schl_comp_yr_12_eq_p, 0)  AS
       High_yr_schl_comp_Yr_12_eq_P,
       COALESCE(high_yr_schl_comp_yr_11_eq_m, 0)  AS
       High_yr_schl_comp_Yr_11_eq_M,
       COALESCE(high_yr_schl_comp_yr_11_eq_f, 0)  AS
       High_yr_schl_comp_Yr_11_eq_F,
       COALESCE(high_yr_schl_comp_yr_11_eq_p, 0)  AS
       High_yr_schl_comp_Yr_11_eq_P,
       COALESCE(high_yr_schl_comp_yr_10_eq_m, 0)  AS
       High_yr_schl_comp_Yr_10_eq_M,
       COALESCE(high_yr_schl_comp_yr_10_eq_f, 0)  AS
       High_yr_schl_comp_Yr_10_eq_F,
       COALESCE(high_yr_schl_comp_yr_10_eq_p, 0)  AS
       High_yr_schl_comp_Yr_10_eq_P,
       COALESCE(high_yr_schl_comp_yr_9_eq_m, 0)   AS High_yr_schl_comp_Yr_9_eq_M
       ,
       COALESCE(high_yr_schl_comp_yr_9_eq_f, 0)   AS
       High_yr_schl_comp_Yr_9_eq_F,
       COALESCE(high_yr_schl_comp_yr_9_eq_p, 0)   AS High_yr_schl_comp_Yr_9_eq_P
       ,
       COALESCE(high_yr_schl_comp_yr_8_belw_m, 0) AS
       High_yr_schl_comp_Yr_8_belw_M,
       COALESCE(high_yr_schl_comp_yr_8_belw_f, 0) AS
       High_yr_schl_comp_Yr_8_belw_F,
       COALESCE(high_yr_schl_comp_yr_8_belw_p, 0) AS
       High_yr_schl_comp_Yr_8_belw_P,
       COALESCE(high_yr_schl_comp_d_n_g_sch_m, 0) AS
       High_yr_schl_comp_D_n_g_sch_M,
       COALESCE(high_yr_schl_comp_d_n_g_sch_f, 0) AS
       High_yr_schl_comp_D_n_g_sch_F,
       COALESCE(high_yr_schl_comp_d_n_g_sch_p, 0) AS
       High_yr_schl_comp_D_n_g_sch_P,
       COALESCE(count_psns_occ_priv_dwgs_m, 0)    AS Count_psns_occ_priv_dwgs_M,
       COALESCE(count_psns_occ_priv_dwgs_f, 0)    AS Count_psns_occ_priv_dwgs_F,
       COALESCE(count_psns_occ_priv_dwgs_p, 0)    AS Count_psns_occ_priv_dwgs_P,
       COALESCE(count_persons_other_dwgs_m, 0)    AS Count_Persons_other_dwgs_M,
       COALESCE(count_persons_other_dwgs_f, 0)    AS Count_Persons_other_dwgs_F,
       COALESCE(count_persons_other_dwgs_p, 0)    AS Count_Persons_other_dwgs_P
FROM   "postgres"."raw"."census_g01" 
