DELETE FROM dwh.FCT_INVOICE_DETAILS 
WHERE etl_date = TO_DATE('{{ params.runday }}', 'YYYY/MM/DD');

insert into dwh.FCT_INVOICE_DETAILS 
(
select
A3.code as MA_HOA_DON,
CASE
    WHEN A3.src_ancestors = '[]' THEN 0
    ELSE array_length(string_to_array(trim(BOTH '[]' FROM A3.src_ancestors), ','), 1)
END AS CAP_KHO,
cast(A3.note_type as int) as LOAI_HOA_DON,
case A3.note_type
when '2' then 'HD'
when '3' then 'HDDC'
else A3.note_type end as TEN_LOAI_HOA_DON,
-- A3.saler_code as MA_NHAN_VIEN,
A3.payment_type as PT_THANH_TOAN,
cast(A3.status as int) as MA_TRANG_THAI,
case A3.status
when '11' then 'HUY_DON_BH'
when '12' then 'BH_THANH_CONG'
else A3.status end as TEN_TRANG_THAI,
A3.vendor_id as MA_NCC,
CASE 
    WHEN A3.issue_on IS NULL OR A3.issue_on = 'None' THEN NULL
    ELSE TO_TIMESTAMP(A3.issue_on, 'YYYY-MM-DD HH24:MI:SS')
END AS NGAY_XUAT,
A3.so_chung_tu as SO_CHUNG_TU,
F3.KHO_BI_ID,
A3.src_stock_code MA_KHO_BAN,
cast(A3.src_stock as int) as ID_KHO_BAN,
TO_TIMESTAMP(A3.created_on, 'YYYY-MM-DD HH24:MI:SS') as NGAY_TAO,
CASE
    WHEN B3.discount_rate = '[]' THEN NULL
    ELSE CAST(trim(both '''' FROM split_part(trim(BOTH '[]' FROM B3.discount_rate), ',', 1)) AS FLOAT8)
END AS TYLE_CK_TINH,
cast(B3.quantity_change as float) as SL_THAY_DOI,
cast(B3.issue_type as int) as MA_LOAI_HANG,
B3.issue_code as TEN_LOAI_HANG,
cast(B3.price as float) as DON_GIA,
D3.SP_BI_ID as ID_SAN_PHAM,
B3.product_code as MA_SAN_PHAM,
CASE
    WHEN B3.discount = '[]' THEN NULL
    ELSE CAST(
        split_part(
            trim(BOTH '[]' FROM B3.discount),
            ',', 1
        ) AS FLOAT8
    )
END AS DT_SAU_CK_TINH,
cast(B3.quantity as float) as SO_LUONG,
cast(B3.pkg_quantity as float) as SO_LUONG_UNIT,
case E3.MA_SAN_PHAM
when null then 10
else E3.VAT end as VAT,
cast(B3.price as float) * cast(B3.quantity as float) as THANH_TIEN,
cast(A3.total_discount as float) as TONG_GT_SAU_CK,
'V1' as NGUON,
TO_DATE('{{ params.runday }}', 'YYYY/MM/DD') as ETL_DATE
FROM stg.V1_INVOICE A3
LEFT JOIN stg.V1_INVOICE_ITEMS B3 ON A3.code = B3.invoice_code
LEFT JOIN dwh.D_SAN_PHAM D3 ON B3.product_code = D3.MA_SAN_PHAM
  AND DATE(A3.created_on) BETWEEN D3.START_DATE AND D3.END_DATE
LEFT JOIN dwh.D_GIA_BAN E3 ON B3.product_code  = E3.MA_SAN_PHAM
  AND E3.NGUON = 'V1'
  AND DATE(A3.created_on) BETWEEN E3.START_DATE AND E3.END_DATE
LEFT JOIN dwh.D_KHO F3 ON
  A3.src_stock = F3.ID_KHO_V1
  AND array_length(string_to_array(trim(BOTH '[]' FROM A3.src_ancestors), ','), 1) = F3.level::int
  -- AND F3.NGUON = 'V2'
  AND DATE(A3.created_on) BETWEEN F3.START_DATE AND F3.END_DATE
WHERE DATE(A3.created_on) >= TO_DATE('{{ params.runday }}', 'YYYY/MM/DD')  - 1

UNION

select
A3.code as MA_HOA_DON,
CASE
    WHEN A3.src_ancestors = '[]' THEN 0
    ELSE array_length(string_to_array(trim(BOTH '[]' FROM A3.src_ancestors), ','), 1)
END AS CAP_KHO,
cast(A3.note_type as int) as LOAI_HOA_DON,
case A3.note_type
when '2' then 'HD'
when '3' then 'HDDC'
else A3.note_type end as TEN_LOAI_HOA_DON,
-- A3.saler_code as MA_NHAN_VIEN,
A3.payment_type as PT_THANH_TOAN,
cast(A3.status as int) as MA_TRANG_THAI,
case A3.status
when '11' then 'HUY_DON_BH'
when '12' then 'BH_THANH_CONG'
else A3.status end as TEN_TRANG_THAI,
A3.vendor_id as MA_NCC,
CASE 
    WHEN A3.issue_on IS NULL OR A3.issue_on = 'None' THEN NULL
    ELSE TO_TIMESTAMP(A3.issue_on, 'YYYY-MM-DD HH24:MI:SS')
END AS NGAY_XUAT,
A3.so_chung_tu as SO_CHUNG_TU,
F3.KHO_BI_ID,
A3.src_stock_code MA_KHO_BAN,
cast(A3.src_stock as float)::int as ID_KHO_BAN,
TO_TIMESTAMP(A3.created_on, 'YYYY-MM-DD HH24:MI:SS') as NGAY_TAO,
CASE
    WHEN B3.discount_rate = '[]' THEN NULL
    ELSE CAST(trim(both '''' FROM split_part(trim(BOTH '[]' FROM B3.discount_rate), ',', 1)) AS FLOAT8)
END AS TYLE_CK_TINH,
cast(B3.quantity_change as float) as SL_THAY_DOI,
cast(B3.issue_type as int) as MA_LOAI_HANG,
B3.issue_code as TEN_LOAI_HANG,
cast(B3.price as float) as DON_GIA,
D3.SP_BI_ID as ID_SAN_PHAM,
B3.product_code as MA_SAN_PHAM,
CASE
    WHEN B3.discount = '[]' THEN NULL
    ELSE CAST(
        split_part(
            trim(BOTH '[]' FROM B3.discount),
            ',', 1
        ) AS FLOAT8
    )
END AS DT_SAU_CK_TINH,
cast(B3.quantity as float) as SO_LUONG,
cast(B3.pkg_quantity as float) as SO_LUONG_UNIT,
case E3.MA_SAN_PHAM
when null then 10
else E3.VAT end as VAT,
cast(B3.price as float) * cast(B3.quantity as float) as THANH_TIEN,
cast(A3.total_discount as float) as TONG_GT_SAU_CK,
'V2' as NGUON,
TO_DATE('{{ params.runday }}', 'YYYY/MM/DD') as ETL_DATE
FROM stg.V2_INVOICE A3
LEFT JOIN stg.V2_INVOICE_ITEMS B3 ON A3.code = B3.invoice_code
LEFT JOIN dwh.D_SAN_PHAM D3 ON B3.product_code = D3.MA_SAN_PHAM
  AND DATE(A3.created_on) BETWEEN D3.START_DATE AND D3.END_DATE
LEFT JOIN dwh.D_GIA_BAN E3 ON B3.product_code  = E3.MA_SAN_PHAM
  AND E3.NGUON = 'V2'
  AND DATE(A3.created_on) BETWEEN E3.START_DATE AND E3.END_DATE
LEFT JOIN dwh.D_KHO F3 ON
  A3.src_stock = F3.ID_KHO_V2
  AND array_length(string_to_array(trim(BOTH '[]' FROM A3.src_ancestors), ','), 1) = F3.level::int
  AND DATE(A3.created_on) BETWEEN F3.START_DATE AND F3.END_DATE
WHERE DATE(A3.created_on) >= TO_DATE('{{ params.runday }}', 'YYYY/MM/DD') - 1
)
