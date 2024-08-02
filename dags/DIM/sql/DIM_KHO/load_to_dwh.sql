MERGE INTO dwh.D_KHO target
USING 
(
SELECT 
    ma_kho ,
    id_kho_v1 ,
    id_kho_v2 ,
    ten_kho ,
    loai_hinh_kho ,
    level ,
    kho_cha_bi_id ,
    MA_KHO_CHA ,
    gps ,
    dien_thoai ,
    dia_chi ,
    ma_khu_vuc ,
    ten_khu_vuc ,
    ghi_chu ,
    ma_trang_thai ,
    ten_trang_thai ,
    mo_ta_trang_thai 
FROM stg.D_KHO_LV{{ params.level }}
WHERE etl_date = TO_DATE('{{ params.runday }}', 'YYYY/MM/DD')
) AS source
ON CONCAT(
    COALESCE(target.ma_kho,'#*#'), 
    COALESCE(target.id_kho_v1,'#*#'), 
    COALESCE(target.id_kho_v2,'#*#'), 
    COALESCE(target.ten_kho,'#*#'), 
    COALESCE(target.loai_hinh_kho,'#*#'), 
    COALESCE(target.level::text,'#*#'), 
    COALESCE(target.kho_cha_bi_id::text,'#*#'), 
    COALESCE(target.ma_kho_cha,'#*#'), 
    COALESCE(target.gps,'#*#'), 
    COALESCE(target.dien_thoai,'#*#'), 
    COALESCE(target.dia_chi,'#*#'), 
    COALESCE(target.ma_khu_vuc,'#*#'), 
    COALESCE(target.ten_khu_vuc,'#*#'), 
    COALESCE(target.ghi_chu,'#*#'), 
    COALESCE(target.ma_trang_thai,'#*#'), 
    COALESCE(target.ten_trang_thai,'#*#'), 
    COALESCE(target.mo_ta_trang_thai,'#*#')
) = CONCAT(
    COALESCE(source.ma_kho,'#*#'), 
    COALESCE(source.id_kho_v1,'#*#'), 
    COALESCE(source.id_kho_v2,'#*#'), 
    COALESCE(source.ten_kho,'#*#'), 
    COALESCE(source.loai_hinh_kho,'#*#'), 
    COALESCE(source.level::text,'#*#'), 
    COALESCE(source.kho_cha_bi_id::text,'#*#'), 
    COALESCE(source.ma_kho_cha,'#*#'), 
    COALESCE(source.gps,'#*#'), 
    COALESCE(source.dien_thoai,'#*#'), 
    COALESCE(source.dia_chi,'#*#'), 
    COALESCE(source.ma_khu_vuc,'#*#'), 
    COALESCE(source.ten_khu_vuc,'#*#'), 
    COALESCE(source.ghi_chu,'#*#'), 
    COALESCE(source.ma_trang_thai,'#*#'), 
    COALESCE(source.ten_trang_thai,'#*#'), 
    COALESCE(source.mo_ta_trang_thai,'#*#')
)
AND target.STATUS = true
WHEN NOT MATCHED THEN
INSERT (id_kho_v1, id_kho_v2, ma_kho, ten_kho, loai_hinh_kho, level, kho_cha_bi_id, MA_KHO_CHA, gps, dien_thoai, dia_chi, ma_khu_vuc, ten_khu_vuc, ghi_chu, ma_trang_thai, ten_trang_thai, mo_ta_trang_thai, start_date, end_date, status)
VALUES (source.id_kho_v1, source.id_kho_v2, source.ma_kho, source.ten_kho, source.loai_hinh_kho, source.level::int, source.kho_cha_bi_id, source.MA_KHO_CHA, source.gps, source.dien_thoai, source.dia_chi, source.ma_khu_vuc, source.ten_khu_vuc, source.ghi_chu, source.ma_trang_thai, source.ten_trang_thai, source.mo_ta_trang_thai, TO_DATE('{{ params.runday }}', 'YYYY/MM/DD') , DATE('2400-01-01'), true);

UPDATE dwh.D_KHO
SET END_DATE = source.END_DATE - 1,
    STATUS = source.STATUS
from (
select t1.ma_kho, t1.id_kho_v1, t1.id_kho_v2, t1.ten_kho, t1.loai_hinh_kho, t1.level, t1.kho_cha_bi_id, t1.MA_KHO_CHA, t1.gps, t1.dien_thoai, t1.dia_chi, t1.ma_khu_vuc, t1.ten_khu_vuc, t1.ghi_chu, t1.ma_trang_thai, t1.ten_trang_thai, t1.mo_ta_trang_thai, t1.start_date, t2.start_date as end_date, 
  case when t2.start_date != '2400-01-01' then false
  else true end status
from (select *, row_number() over(partition by ma_kho order by start_date asc) as rn
from dwh.D_KHO) t1
join (select *, row_number() over(partition by ma_kho order by start_date asc) as rn
from dwh.D_KHO) t2 on t1.ma_kho = t2.ma_kho 
where t1.rn = t2.rn - 1)as source
where dwh.D_KHO.ma_kho = source.ma_kho and dwh.D_KHO.start_date = source.start_date;

INSERT INTO dwh.D_KHO
-- (kho_bi_id, ma_kho, id_kho_v1, id_kho_v2, ten_kho, loai_hinh_kho, level, ma_kho_cha, gps, dien_thoai, dia_chi, ma_khu_vuc, ten_khu_vuc, ghi_chu, ma_trang_thai, ten_trang_thai, mo_ta_trang_thai,  start_date, end_date, status)
SELECT ROW_NUMBER() OVER(ORDER BY ma_kho) + (SELECT MAX(COALESCE(kho_bi_id, 0)) FROM dwh.D_KHO) as kho_bi_id, ma_kho, id_kho_v1, id_kho_v2, ten_kho, loai_hinh_kho, level, kho_cha_bi_id, ma_kho_cha, gps, dien_thoai, dia_chi, ma_khu_vuc, ten_khu_vuc, ghi_chu, ma_trang_thai, ten_trang_thai, mo_ta_trang_thai,  start_date, end_date, status
FROM dwh.D_KHO
WHERE kho_bi_id IS NULL;

DELETE FROM dwh.D_KHO
WHERE kho_bi_id IS NULL;

-- update dwh.D_KHO
-- set kho_cha_bi_id = source.kho_cha_bi_id
-- from (
-- 	select a.ma_kho,b.kho_bi_id as kho_cha_bi_id
-- 	from dwh.d_kho a
-- 	left join dwh.d_kho b on a.ma_kho_cha = b.ma_kho 
-- 	where a.kho_cha_bi_id is null
-- ) as source
-- where dwh.D_KHO.ma_kho = source.ma_kho;

