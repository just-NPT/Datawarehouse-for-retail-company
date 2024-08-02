DELETE FROM stg.d_cay_nhom_san_pham
WHERE etl_date = TO_DATE('{{ params.runday }}', 'YYYY/MM/DD');

INSERT INTO stg.d_cay_nhom_san_pham (
    ma_nhom_san_pham,
    ten_nhom_san_pham,
    ma_nhom_hang,
    ten_nhom_hang,
    ma_nganh_hang,
    ten_nganh_hang,
    etl_date
)
SELECT
    dnsp.ma_nhom_san_pham,
    dnsp.ten_nhom_san_pham,
    dnsp2.ma_nhom_san_pham AS ma_nhom_hang,
    dnsp2.ten_nhom_san_pham AS ten_nhom_hang,
    dnsp3.ma_nhom_san_pham AS ma_nganh_hang,
    dnsp3.ten_nhom_san_pham AS ten_nganh_hang,
    dnsp.etl_date
FROM
    stg.d_nhom_san_pham dnsp
    LEFT JOIN stg.d_nhom_san_pham dnsp2 ON dnsp2.ma_nhom_san_pham = dnsp.ma_nhom_san_pham_cha and dnsp.etl_date = dnsp2.etl_date 
    LEFT JOIN stg.d_nhom_san_pham dnsp3 ON dnsp3.ma_nhom_san_pham = dnsp2.ma_nhom_san_pham_cha and dnsp3.etl_date = dnsp2.etl_date 
where dnsp.etl_date = TO_DATE('{{ params.runday }}', 'YYYY/MM/DD');

DELETE FROM stg.d_cay_nhom_san_pham
WHERE etl_date <= TO_DATE('{{ params.runday }}', 'YYYY/MM/DD') - 3;