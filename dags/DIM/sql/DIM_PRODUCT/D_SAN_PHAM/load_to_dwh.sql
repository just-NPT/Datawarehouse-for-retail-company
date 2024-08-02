--TRUNCATE TABLE dwh.D_SAN_PHAM;

MERGE into dwh.D_SAN_PHAM target
USING 
(
SELECT MA_SAN_PHAM,
       TEN_SAN_PHAM,
       MA_LOAI_HANG,
       TEN_LOAI_HANG,
       MO_TA_LOAI_HANG,
       MA_TRANG_THAI,
       TRANG_THAI,
       MO_TA_TRANG_THAI,
       MA_PL_NGANH_HANG,
       TEN_PL_NGANH_HANG,
       MA_NHOM_SAN_PHAM,
       TEN_NHOM_SAN_PHAM,
       MA_NHOM_HANG,
       TEN_NHOM_HANG,
       MA_NGANH_HANG,
       TEN_NGANH_HANG,
       BIEN_THE,
       MA_THUONG_HIEU,
       TEN_THUONG_HIEU,
       MA_DON_VI_LE,
       TEN_DON_VI_LE,
       CO_DONG_GOI,
       MA_DON_VI_TINH,
       TEN_DON_VI_TINH,
       MA_PACKSIZE,
       TEN_PACKSIZE,
       MA_DON_VI_CHAN,
       TEN_DON_VI_CHAN,
       QUY_CACH_DONG_GOI,
       TONG_TRONG_LUONG,
       MA_HINH_THUC_DONG_GOI,
       TEN_HINH_THUC_DONG_GOI,
       MA_NGUON_GOC_SAN_PHAM,
       MA_VACH,
       TEN_NGUON_GOC,
       BARCODE_SAN_PHAM,
       BARCODE_THUNG,
       MA_NCC_SAN_PHAM,
       TEN_DK_KINH_DOANH,
       TEN_VIET_TAT,
       LOAI_NCC,
       MST,
       SDT,
       DIA_CHI,
       GHI_CHU,
       NHA_SAN_XUAT,
       MA_MO_HINH_MUA,
       TEN_MO_HINH_MUA,
       NGON_NGU_SAN_PHAM,
       HAN_SU_DUNG
FROM stg.D_SAN_PHAM_TEMP
where etl_date = TO_DATE('{{ params.runday }}', 'YYYY/MM/DD') 
) source
ON
CONCAT(COALESCE(target.MA_SAN_PHAM,'#*#'),
       COALESCE(target.TEN_SAN_PHAM,'#*#'),
       COALESCE(target.MA_LOAI_HANG,'#*#'),
       COALESCE(target.TEN_LOAI_HANG,'#*#'),
       COALESCE(target.MO_TA_LOAI_HANG,'#*#'),
       COALESCE(target.MA_TRANG_THAI,'#*#'),
       COALESCE(target.TRANG_THAI,'#*#'),
       COALESCE(target.MO_TA_TRANG_THAI,'#*#'),
       COALESCE(target.MA_PL_NGANH_HANG,'#*#'),
       COALESCE(target.TEN_PL_NGANH_HANG,'#*#'),
       COALESCE(target.MA_NHOM_SAN_PHAM,'#*#'),
       COALESCE(target.TEN_NHOM_SAN_PHAM,'#*#'),
       COALESCE(target.MA_NHOM_HANG,'#*#'),
       COALESCE(target.TEN_NHOM_HANG,'#*#'),
       COALESCE(target.MA_NGANH_HANG,'#*#'),
       COALESCE(target.TEN_NGANH_HANG,'#*#'),
       COALESCE(target.BIEN_THE,'#*#'),
       COALESCE(target.MA_THUONG_HIEU,'#*#'),
       COALESCE(target.TEN_THUONG_HIEU,'#*#'),
       COALESCE(target.MA_DON_VI_LE,'#*#'),
       COALESCE(target.TEN_DON_VI_LE,'#*#'),
       COALESCE(target.CO_DONG_GOI,'#*#'),
       COALESCE(target.MA_DON_VI_TINH,'#*#'),
       COALESCE(target.TEN_DON_VI_TINH,'#*#'),
       COALESCE(target.MA_PACKSIZE,'#*#'),
       COALESCE(target.TEN_PACKSIZE,'#*#'),
       COALESCE(target.MA_DON_VI_CHAN,'#*#'), 
       COALESCE(target.TEN_DON_VI_CHAN,'#*#'), 
       COALESCE(target.QUY_CACH_DONG_GOI,'#*#'),
       COALESCE(target.TONG_TRONG_LUONG::TEXT,'#*#'),
       COALESCE(target.MA_HINH_THUC_DONG_GOI,'#*#'),
       COALESCE(target.TEN_HINH_THUC_DONG_GOI,'#*#'),
       COALESCE(target.MA_NGUON_GOC_SAN_PHAM,'#*#'),
       COALESCE(target.MA_VACH,'#*#'),
       COALESCE(target.TEN_NGUON_GOC,'#*#'),
       COALESCE(target.BARCODE_SAN_PHAM,'#*#'),
       COALESCE(target.BARCODE_THUNG,'#*#'),
       COALESCE(target.MA_NCC_SAN_PHAM,'#*#'),
       COALESCE(target.TEN_DK_KINH_DOANH,'#*#'),
       COALESCE(target.TEN_VIET_TAT,'#*#'),
       COALESCE(target.LOAI_NCC,'#*#'),
       COALESCE(target.MST,'#*#'),
       COALESCE(target.SDT,'#*#'),
       COALESCE(target.DIA_CHI,'#*#'),
       COALESCE(target.GHI_CHU,'#*#'),
       COALESCE(target.NHA_SAN_XUAT,'#*#'),
       COALESCE(target.MA_MO_HINH_MUA,'#*#'),
       COALESCE(target.TEN_MO_HINH_MUA,'#*#'),
       COALESCE(target.NGON_NGU_SAN_PHAM,'#*#'),
       COALESCE(target.HAN_SU_DUNG,'#*#')) =
CONCAT(COALESCE(source.MA_SAN_PHAM,'#*#'),
       COALESCE(source.TEN_SAN_PHAM,'#*#'),
       COALESCE(source.MA_LOAI_HANG,'#*#'),
       COALESCE(source.TEN_LOAI_HANG,'#*#'),
       COALESCE(source.MO_TA_LOAI_HANG,'#*#'),
       COALESCE(source.MA_TRANG_THAI,'#*#'), 
       COALESCE(source.TRANG_THAI,'#*#'), 
       COALESCE(source.MO_TA_TRANG_THAI,'#*#'), 
       COALESCE(source.MA_PL_NGANH_HANG,'#*#'),
       COALESCE(source.TEN_PL_NGANH_HANG,'#*#'),
       COALESCE(source.MA_NHOM_SAN_PHAM,'#*#'), 
       COALESCE(source.TEN_NHOM_SAN_PHAM,'#*#'), 
       COALESCE(source.MA_NHOM_HANG,'#*#'), 
       COALESCE(source.TEN_NHOM_HANG,'#*#'), 
       COALESCE(source.MA_NGANH_HANG,'#*#'), 
       COALESCE(source.TEN_NGANH_HANG,'#*#'), 
       COALESCE(source.BIEN_THE,'#*#'),
       COALESCE(source.MA_THUONG_HIEU,'#*#'),
       COALESCE(source.TEN_THUONG_HIEU,'#*#'),
       COALESCE(source.MA_DON_VI_LE,'#*#'),
       COALESCE(source.TEN_DON_VI_LE,'#*#'),
       COALESCE(source.CO_DONG_GOI,'#*#'),
       COALESCE(source.MA_DON_VI_TINH,'#*#'),
       COALESCE(source.TEN_DON_VI_TINH,'#*#'),
       COALESCE(source.MA_PACKSIZE,'#*#'),
       COALESCE(source.TEN_PACKSIZE,'#*#'),
       COALESCE(source.MA_DON_VI_CHAN,'#*#'), 
       COALESCE(source.TEN_DON_VI_CHAN,'#*#'), 
       COALESCE(source.QUY_CACH_DONG_GOI::TEXT,'#*#'),
       COALESCE(source.TONG_TRONG_LUONG::TEXT,'#*#'),
       COALESCE(source.MA_HINH_THUC_DONG_GOI,'#*#'),
       COALESCE(source.TEN_HINH_THUC_DONG_GOI,'#*#'),
       COALESCE(source.MA_NGUON_GOC_SAN_PHAM,'#*#'),
       COALESCE(source.MA_VACH,'#*#'),
       COALESCE(source.TEN_NGUON_GOC,'#*#'),
       COALESCE(source.BARCODE_SAN_PHAM::TEXT,'#*#'),
       COALESCE(source.BARCODE_THUNG::TEXT,'#*#'),
       COALESCE(source.MA_NCC_SAN_PHAM,'#*#'),
       COALESCE(source.TEN_DK_KINH_DOANH,'#*#'),
       COALESCE(source.TEN_VIET_TAT,'#*#'),
       COALESCE(source.LOAI_NCC,'#*#'),
       COALESCE(source.MST,'#*#'),
       COALESCE(source.SDT,'#*#'),
       COALESCE(source.DIA_CHI,'#*#'),
       COALESCE(source.GHI_CHU,'#*#'),
       COALESCE(source.NHA_SAN_XUAT::TEXT,'#*#'),
       COALESCE(source.MA_MO_HINH_MUA,'#*#'),
       COALESCE(source.TEN_MO_HINH_MUA,'#*#'),
       COALESCE(source.NGON_NGU_SAN_PHAM,'#*#'),
       COALESCE(source.HAN_SU_DUNG::TEXT,'#*#'))
AND target.STATUS = true
WHEN NOT MATCHED THEN
INSERT (MA_SAN_PHAM, TEN_SAN_PHAM, 
       MA_LOAI_HANG, TEN_LOAI_HANG, MO_TA_LOAI_HANG, 
       MA_TRANG_THAI, TRANG_THAI, MO_TA_TRANG_THAI,MA_PL_NGANH_HANG, TEN_PL_NGANH_HANG,
       MA_NHOM_SAN_PHAM, TEN_NHOM_SAN_PHAM, MA_NHOM_HANG, TEN_NHOM_HANG, 
       MA_NGANH_HANG, TEN_NGANH_HANG, BIEN_THE, MA_THUONG_HIEU, TEN_THUONG_HIEU, 
       MA_DON_VI_LE, TEN_DON_VI_LE, CO_DONG_GOI, MA_DON_VI_TINH, TEN_DON_VI_TINH,
       MA_PACKSIZE, TEN_PACKSIZE, MA_DON_VI_CHAN, TEN_DON_VI_CHAN, QUY_CACH_DONG_GOI,
       TONG_TRONG_LUONG, MA_HINH_THUC_DONG_GOI, TEN_HINH_THUC_DONG_GOI, 
       MA_NGUON_GOC_SAN_PHAM, MA_VACH, TEN_NGUON_GOC, BARCODE_SAN_PHAM, BARCODE_THUNG,
       MA_NCC_SAN_PHAM, TEN_DK_KINH_DOANH, TEN_VIET_TAT, LOAI_NCC, MST, SDT, DIA_CHI, GHI_CHU, 
       NHA_SAN_XUAT, MA_MO_HINH_MUA, TEN_MO_HINH_MUA, NGON_NGU_SAN_PHAM, HAN_SU_DUNG, 
       start_date, end_date, status)
VALUES (source.MA_SAN_PHAM, source.TEN_SAN_PHAM, 
        source.MA_LOAI_HANG, source.TEN_LOAI_HANG, source.MO_TA_LOAI_HANG,
        source.MA_TRANG_THAI, source.TRANG_THAI, source.MO_TA_TRANG_THAI,source.MA_PL_NGANH_HANG, source.TEN_PL_NGANH_HANG,
        source.MA_NHOM_SAN_PHAM, source.TEN_NHOM_SAN_PHAM, source.MA_NHOM_HANG, source.TEN_NHOM_HANG, 
        source.MA_NGANH_HANG, source.TEN_NGANH_HANG, source.BIEN_THE, source.MA_THUONG_HIEU, source.TEN_THUONG_HIEU,
        source.MA_DON_VI_LE, source.TEN_DON_VI_LE,source.CO_DONG_GOI, source.MA_DON_VI_TINH, source.TEN_DON_VI_TINH, 
        source.MA_PACKSIZE, source.TEN_PACKSIZE, source.MA_DON_VI_CHAN, source.TEN_DON_VI_CHAN, source.QUY_CACH_DONG_GOI,
        source.TONG_TRONG_LUONG::FLOAT, source.MA_HINH_THUC_DONG_GOI, source.TEN_HINH_THUC_DONG_GOI, 
        source.MA_NGUON_GOC_SAN_PHAM, source.MA_VACH, source.TEN_NGUON_GOC, source.BARCODE_SAN_PHAM, source.BARCODE_THUNG,
        source.MA_NCC_SAN_PHAM, source.TEN_DK_KINH_DOANH, source.TEN_VIET_TAT, source.LOAI_NCC, source.MST, source.SDT, source.DIA_CHI, source.GHI_CHU,
        source.NHA_SAN_XUAT, source.MA_MO_HINH_MUA, source.TEN_MO_HINH_MUA, source.NGON_NGU_SAN_PHAM,source.HAN_SU_DUNG, 
        TO_DATE('{{ params.runday }}', 'YYYY/MM/DD') , DATE('2400-01-01'), true);

UPDATE dwh.D_SAN_PHAM
SET END_DATE = source.END_DATE - 1,
    STATUS = source.STATUS
from (
select t1.MA_SAN_PHAM, t1.TEN_SAN_PHAM, 
       t1.MA_LOAI_HANG, t1.TEN_LOAI_HANG, t1.MO_TA_LOAI_HANG, 
       t1.MA_TRANG_THAI, t1.TRANG_THAI, t1.MO_TA_TRANG_THAI,t1.MA_PL_NGANH_HANG, t1.TEN_PL_NGANH_HANG, 
       t1.MA_NHOM_SAN_PHAM, t1.TEN_NHOM_SAN_PHAM, t1.MA_NHOM_HANG, t1.TEN_NHOM_HANG, 
       t1.MA_NGANH_HANG, t1.TEN_NGANH_HANG, t1.BIEN_THE, t1.MA_THUONG_HIEU, t1.TEN_THUONG_HIEU, 
       t1.MA_DON_VI_LE, t1.TEN_DON_VI_LE, t1.CO_DONG_GOI, t1.MA_DON_VI_TINH, t1.TEN_DON_VI_TINH, 
       t1.MA_PACKSIZE, t1.TEN_PACKSIZE, t1.MA_DON_VI_CHAN, t1.TEN_DON_VI_CHAN, t1.QUY_CACH_DONG_GOI, 
       t1.TONG_TRONG_LUONG, t1.MA_HINH_THUC_DONG_GOI, t1.TEN_HINH_THUC_DONG_GOI, 
       t1.MA_NGUON_GOC_SAN_PHAM, t1.MA_VACH, t1.TEN_NGUON_GOC, t1.BARCODE_SAN_PHAM, t1.BARCODE_THUNG, 
       t1.MA_NCC_SAN_PHAM, t1.TEN_DK_KINH_DOANH, t1.TEN_VIET_TAT, t1.LOAI_NCC, t1.MST, t1.SDT, t1.DIA_CHI, t1.GHI_CHU, 
       t1.NHA_SAN_XUAT, t1.MA_MO_HINH_MUA, t1.TEN_MO_HINH_MUA, t1.NGON_NGU_SAN_PHAM, t1.HAN_SU_DUNG, 
       t1.start_date,t2.start_date as end_date, 
  case when t2.start_date != '2400-01-01' then false
  else true end status
from (select *, row_number() over(partition by MA_SAN_PHAM order by start_date asc) as rn
from dwh.D_SAN_PHAM) t1
join (select *, row_number() over(partition by MA_SAN_PHAM order by start_date asc) as rn
from dwh.D_SAN_PHAM) t2 on t1.MA_SAN_PHAM = t2.MA_SAN_PHAM 
where t1.rn = t2.rn - 1) as source
where dwh.D_SAN_PHAM.MA_SAN_PHAM = source.MA_SAN_PHAM and dwh.D_SAN_PHAM.start_date = source.start_date;


INSERT INTO dwh.D_SAN_PHAM (
       SP_BI_ID,MA_SAN_PHAM, TEN_SAN_PHAM, 
       MA_LOAI_HANG, TEN_LOAI_HANG, MO_TA_LOAI_HANG, 
       MA_TRANG_THAI, TRANG_THAI, MO_TA_TRANG_THAI,MA_PL_NGANH_HANG, TEN_PL_NGANH_HANG, 
       MA_NHOM_SAN_PHAM, TEN_NHOM_SAN_PHAM, MA_NHOM_HANG, TEN_NHOM_HANG, 
       MA_NGANH_HANG, TEN_NGANH_HANG, BIEN_THE, MA_THUONG_HIEU, TEN_THUONG_HIEU, 
       MA_DON_VI_LE, TEN_DON_VI_LE, CO_DONG_GOI, MA_DON_VI_TINH, TEN_DON_VI_TINH, 
       MA_PACKSIZE, TEN_PACKSIZE, MA_DON_VI_CHAN, TEN_DON_VI_CHAN, QUY_CACH_DONG_GOI, 
       TONG_TRONG_LUONG, MA_HINH_THUC_DONG_GOI, TEN_HINH_THUC_DONG_GOI, 
       MA_NGUON_GOC_SAN_PHAM, MA_VACH, TEN_NGUON_GOC, BARCODE_SAN_PHAM, BARCODE_THUNG, 
       MA_NCC_SAN_PHAM, TEN_DK_KINH_DOANH, TEN_VIET_TAT, LOAI_NCC, MST, SDT, DIA_CHI, GHI_CHU, 
       NHA_SAN_XUAT, MA_MO_HINH_MUA, TEN_MO_HINH_MUA, NGON_NGU_SAN_PHAM, HAN_SU_DUNG, 
       start_date, end_date, status)
SELECT ROW_NUMBER() OVER(ORDER BY MA_SAN_PHAM) + (SELECT MAX(COALESCE(SP_BI_ID, 0)) FROM dwh.D_SAN_PHAM) as SP_BI_ID,
       t1.MA_SAN_PHAM, t1.TEN_SAN_PHAM, 
       t1.MA_LOAI_HANG, t1.TEN_LOAI_HANG, t1.MO_TA_LOAI_HANG, 
       t1.MA_TRANG_THAI, t1.TRANG_THAI, t1.MO_TA_TRANG_THAI, t1.MA_PL_NGANH_HANG, t1.TEN_PL_NGANH_HANG, 
       t1.MA_NHOM_SAN_PHAM, t1.TEN_NHOM_SAN_PHAM, t1.MA_NHOM_HANG, t1.TEN_NHOM_HANG, 
       t1.MA_NGANH_HANG, t1.TEN_NGANH_HANG, t1.BIEN_THE, t1.MA_THUONG_HIEU, t1.TEN_THUONG_HIEU, 
       t1.MA_DON_VI_LE, t1.TEN_DON_VI_LE, t1.CO_DONG_GOI, t1.MA_DON_VI_TINH, t1.TEN_DON_VI_TINH, 
       t1.MA_PACKSIZE, t1.TEN_PACKSIZE, t1.MA_DON_VI_CHAN, t1.TEN_DON_VI_CHAN, t1.QUY_CACH_DONG_GOI, 
       t1.TONG_TRONG_LUONG, t1.MA_HINH_THUC_DONG_GOI, t1.TEN_HINH_THUC_DONG_GOI, 
       t1.MA_NGUON_GOC_SAN_PHAM, t1.MA_VACH, t1.TEN_NGUON_GOC, t1.BARCODE_SAN_PHAM, t1.BARCODE_THUNG, 
       t1.MA_NCC_SAN_PHAM, t1.TEN_DK_KINH_DOANH, t1.TEN_VIET_TAT, t1.LOAI_NCC, t1.MST, t1.SDT, t1.DIA_CHI, t1.GHI_CHU, 
       t1.NHA_SAN_XUAT, t1.MA_MO_HINH_MUA, t1.TEN_MO_HINH_MUA, t1.NGON_NGU_SAN_PHAM, t1.HAN_SU_DUNG, 
       t1.start_date, t1.end_date, t1.status
FROM dwh.D_SAN_PHAM t1
WHERE SP_BI_ID IS NULL;

DELETE FROM dwh.D_SAN_PHAM
WHERE SP_BI_ID IS NULL;