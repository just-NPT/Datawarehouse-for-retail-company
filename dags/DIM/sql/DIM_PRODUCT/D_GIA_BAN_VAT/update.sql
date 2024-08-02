DELETE FROM stg.d_gia_ban
WHERE etl_date >= TO_DATE('{{ params.runday }}', 'YYYY/MM/DD') ;

insert into stg.d_gia_ban
(
select gia.*
from stg.d_gia_ban_vat gia
left join (
select ma_san_pham ,nguon, ap_dung_tu, row_number() over(partition by ma_san_pham, ma_kenh_ban, nguon order by ap_dung_tu desc) as rn
from stg.d_gia_ban_vat dgbv 
where etl_date = TO_DATE('{{ params.runday }}', 'YYYY/MM/DD')
) as tmp on gia.ma_san_pham = tmp.ma_san_pham and gia.nguon = tmp.nguon and gia.ap_dung_tu = tmp.ap_dung_tu
where rn = 1 and etl_date = TO_DATE('{{ params.runday }}', 'YYYY/MM/DD')
);

DELETE FROM stg.d_gia_ban
WHERE etl_date <= TO_DATE('{{ params.runday }}', 'YYYY/MM/DD') - INTERVAL '3 DAY';