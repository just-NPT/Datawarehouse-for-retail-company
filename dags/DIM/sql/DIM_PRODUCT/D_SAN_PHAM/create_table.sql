CREATE TABLE dwh.d_san_pham (
	SP_ID INT4 NULL,
	MA_SAN_PHAM TEXT NULL,
	TEN_SAN_PHAM TEXT NULL,
	MA_LOAI_HANG TEXT NULL,
  TEN_LOAI_HANG TEXT NULL,
  MO_TA_LOAI_HANG TEXT NULL,
	MA_TRANG_THAI TEXT NULL,
  TRANG_THAI TEXT NULL,
  MO_TA_TRANG_THAI TEXT NULL,
	MA_PL_NGANH_HANG TEXT NULL,
  TEN_PL_NGANH_HANG TEXT NULL,
	MA_NHOM_SAN_PHAM TEXT NULL,
  TEN_NHOM_SAN_PHAM TEXT NULL,
  MA_NHOM_HANG TEXT NULL,
  TEN_NHOM_HANG TEXT NULL,
  MA_NGANH_HANG TEXT NULL,
  TEN_NGANH_HANG TEXT NULL,
	BIEN_THE TEXT NULL,
	MA_THUONG_HIEU TEXT NULL,
  TEN_THUONG_HIEU TEXT NULL,
	MA_DON_VI_LE TEXT NULL,
  TEN_DON_VI_LE TEXT NULL,
	CO_DONG_GOI TEXT NULL,
	MA_DON_VI_TINH TEXT NULL,
  TEN_DON_VI_TINH TEXT NULL,
	MA_PACKSIZE TEXT NULL,
  TEN_PACKSIZE TEXT NULL,
	MA_DON_VI_CHAN TEXT NULL,
  TEN_DON_VI_CHAN TEXT NULL,
	QUY_CACH_DONG_GOI TEXT NULL,
	TONG_TRONG_LUONG FLOAT8 NULL,
	MA_HINH_THUC_DONG_GOI TEXT NULL,
  TEN_HINH_THUC_DONG_GOI TEXT NULL,
  MA_NGUON_GOC_SAN_PHAM TEXT NULL,
  MA_VACH TEXT NULL,
  TEN_NGUON_GOC TEXT NULL,
	BARCODE_SAN_PHAM TEXT NULL,
	BARCODE_THUNG TEXT NULL,
	MA_NCC_SAN_PHAM TEXT NULL,
  TEN_DK_KINH_DOANH TEXT NULL,
  TEN_VIET_TAT TEXT NULL,
  LOAI_NCC TEXT NULL,
  MST TEXT NULL,
  SDT TEXT NULL,
  DIA_CHI TEXT NULL,
  GHI_CHU TEXT NULL,
	NHA_SAN_XUAT TEXT NULL,
	MA_MO_HINH_MUA TEXT NULL,
	TEN_MO_HINH_MUA TEXT NULL,
	NGON_NGU_SAN_PHAM TEXT NULL,
	HAN_SU_DUNG TEXT NULL,
	START_DATE DATE NULL,
	END_DATE DATE NULL,
	STATUS BOOL NULL
);