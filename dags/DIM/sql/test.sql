create table stg.D_KHO_LV3 as 
    SELECT
        S1."ma_kho" AS ma_kho,
        TRIM(TRAILING '.0' FROM CAST(S1.id_kho_v1 AS TEXT)) as id_kho_v1,
        TRIM(TRAILING '.0' FROM CAST(S1.id_kho_v2 AS TEXT)) as id_kho_v2,
        S1.ten_kho,
        S1.loai_hinh_kho,
        S1.level,
        S4.kho_bi_id as kho_cha_bi_id,
        S1.MA_KHO_CHA,
        S1.GPS,
        S1.dien_thoai,
        S1.dia_chi,
        S1.ma_khu_vuc,
        S3.ten_khu_vuc,
        S1.ghi_chu,
        S1.ma_trang_thai,
        S2.ten_trang_thai,
        S2.mo_ta,
        S1.etl_date
    FROM stg.D_KHO S1 
    LEFT JOIN stg.D_TRANG_THAI_KHO S2 ON UPPER(TRIM(S1.MA_TRANG_THAI)) = S2.MA_TRANG_THAI AND S2.etl_date = S1.etl_date
    LEFT JOIN stg.D_KHU_VUC S3 ON UPPER(TRIM(S1.MA_KHU_VUC)) = S3.MA_KHU_VUC AND S3.etl_date = S1.etl_date
    LEFT JOIN dwh.D_KHO S4 ON UPPER(TRIM(S1.MA_KHO_CHA)) = S4.MA_KHO AND S1.etl_date BETWEEN S4.start_date AND S4.end_date
    WHERE S1.etl_date = TO_DATE('2024/03/19', 'YYYY/MM/DD') and S1.level = '3'