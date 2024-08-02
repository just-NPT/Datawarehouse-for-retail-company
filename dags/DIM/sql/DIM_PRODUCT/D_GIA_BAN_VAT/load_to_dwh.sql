MERGE INTO dwh.D_GIA_BAN target
USING
(
SELECT
  ma_kenh_ban,
	ma_san_pham,
	gia,
	vat,
	ap_dung_tu,
	nguon
FROM stg.d_gia_ban
WHERE etl_date = TO_DATE('{{ params.runday }}', 'YYYY/MM/DD')
) as SOURCE
ON CONCAT(
  COALESCE(target.ma_kenh_ban,'#*#'),
  COALESCE(target.ma_san_pham,'#*#'),
  COALESCE(target.gia_niem_yet::text,'#*#'),
  COALESCE(target.vat::text,'#*#'),
  COALESCE(cast(target.ngay_thiet_lap as timestamp)::text,'#*#'),
  COALESCE(target.nguon,'#*#')
)= CONCAT(
  COALESCE(SOURCE.ma_kenh_ban,'#*#'),
  COALESCE(SOURCE.ma_san_pham,'#*#'),
  COALESCE((SOURCE.gia::float)::text,'#*#'),
  COALESCE((SOURCE.vat::float)::text,'#*#'),
  COALESCE(SOURCE.ap_dung_tu,'#*#'),
  COALESCE(SOURCE.nguon,'#*#')
)
AND target.STATUS = TRUE
WHEN NOT MATCHED THEN
INSERT (ma_kenh_ban, ma_san_pham, gia_niem_yet, vat, ngay_thiet_lap, nguon, start_date, end_date, status)
VALUES (source.ma_kenh_ban, source.ma_san_pham, source.gia::float8, source.vat::float8, source.ap_dung_tu::date, source.nguon, source.ap_dung_tu::date , DATE('2400-01-01'), true);

UPDATE dwh.D_GIA_BAN
SET END_DATE = source.END_DATE - 1,
    STATUS = source.STATUS
from (
select t1.ma_kenh_ban, t1.ma_san_pham, t1.gia_niem_yet, t1.vat, t1.ngay_thiet_lap, t1.nguon, t1.start_date, t2.start_date as end_date, 
  case when t2.start_date != '2400-01-01' then false
  else true end status
from (select *, row_number() over(partition by MA_SAN_PHAM, NGUON order by start_date asc) as rn
from dwh.D_GIA_BAN) t1
join (select *, row_number() over(partition by MA_SAN_PHAM, NGUON order by start_date asc) as rn
from dwh.D_GIA_BAN) t2 on t1.MA_SAN_PHAM = t2.MA_SAN_PHAM and t1.NGUON = t2.NGUON
where t1.rn = t2.rn - 1)as source
where dwh.D_GIA_BAN.MA_SAN_PHAM = source.MA_SAN_PHAM and dwh.D_GIA_BAN.NGAY_THIET_LAP = source.NGAY_THIET_LAP and dwh.D_GIA_BAN.NGUON = source.NGUON;

INSERT INTO dwh.D_GIA_BAN
SELECT ROW_NUMBER() OVER(ORDER BY MA_SAN_PHAM, ngay_thiet_lap ,NGUON ) + (SELECT MAX(COALESCE(gb_vat_id, 0)) FROM dwh.D_GIA_BAN) as gb_vat_id, ma_kenh_ban, ma_san_pham, gia_niem_yet, vat, ngay_thiet_lap, nguon, start_date, end_date, status
FROM dwh.D_GIA_BAN
WHERE gb_vat_id IS NULL;

DELETE FROM dwh.D_GIA_BAN
WHERE gb_vat_id IS NULL;
