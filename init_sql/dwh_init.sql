DROP SCHEMA IF EXISTS dwh CASCADE;

CREATE SCHEMA dwh AUTHORIZATION myuser;
-- dwh.d_gia_ban definition

-- Drop table

-- DROP TABLE dwh.d_gia_ban;

CREATE TABLE dwh.d_gia_ban (
	gb_vat_id int4 NULL,
	ma_kenh_ban text NULL,
	ma_san_pham text NULL,
	gia_niem_yet float8 NULL,
	vat float8 NULL,
	ngay_thiet_lap date NULL,
	nguon text NULL,
	start_date date NULL,
	end_date date NULL,
	status bool NULL
);


-- dwh.d_kho definition

-- Drop table

-- DROP TABLE dwh.d_kho;

CREATE TABLE dwh.d_kho (
	kho_bi_id int4 NULL,
	ma_kho text NULL,
	id_kho_v1 text NULL,
	id_kho_v2 text NULL,
	ten_kho text NULL,
	loai_hinh_kho text NULL,
	"level" int4 NULL,
	kho_cha_bi_id int4 NULL,
	ma_kho_cha text NULL,
	gps text NULL,
	dien_thoai text NULL,
	dia_chi text NULL,
	ma_khu_vuc text NULL,
	ten_khu_vuc text NULL,
	ghi_chu text NULL,
	ma_trang_thai text NULL,
	ten_trang_thai text NULL,
	mo_ta_trang_thai text NULL,
	start_date date NULL,
	end_date date NULL,
	status bool NULL
);


-- dwh.d_san_pham definition

-- Drop table

-- DROP TABLE dwh.d_san_pham;

CREATE TABLE dwh.d_san_pham (
	sp_bi_id int4 NULL,
	ma_san_pham text NULL,
	ten_san_pham text NULL,
	ma_loai_hang text NULL,
	ten_loai_hang text NULL,
	mo_ta_loai_hang text NULL,
	ma_trang_thai text NULL,
	trang_thai text NULL,
	mo_ta_trang_thai text NULL,
	ma_pl_nganh_hang text NULL,
	ten_pl_nganh_hang text NULL,
	ma_nhom_san_pham text NULL,
	ten_nhom_san_pham text NULL,
	ma_nhom_hang text NULL,
	ten_nhom_hang text NULL,
	ma_nganh_hang text NULL,
	ten_nganh_hang text NULL,
	bien_the text NULL,
	ma_thuong_hieu text NULL,
	ten_thuong_hieu text NULL,
	ma_don_vi_le text NULL,
	ten_don_vi_le text NULL,
	co_dong_goi text NULL,
	ma_don_vi_tinh text NULL,
	ten_don_vi_tinh text NULL,
	ma_packsize text NULL,
	ten_packsize text NULL,
	ma_don_vi_chan text NULL,
	ten_don_vi_chan text NULL,
	quy_cach_dong_goi text NULL,
	tong_trong_luong float8 NULL,
	ma_hinh_thuc_dong_goi text NULL,
	ten_hinh_thuc_dong_goi text NULL,
	ma_nguon_goc_san_pham text NULL,
	ma_vach text NULL,
	ten_nguon_goc text NULL,
	barcode_san_pham text NULL,
	barcode_thung text NULL,
	ma_ncc_san_pham text NULL,
	ten_dk_kinh_doanh text NULL,
	ten_viet_tat text NULL,
	loai_ncc text NULL,
	mst text NULL,
	sdt text NULL,
	dia_chi text NULL,
	ghi_chu text NULL,
	nha_san_xuat text NULL,
	ma_mo_hinh_mua text NULL,
	ten_mo_hinh_mua text NULL,
	ngon_ngu_san_pham text NULL,
	han_su_dung text NULL,
	start_date date NULL,
	end_date date NULL,
	status bool NULL
);


-- dwh.fct_invoice_details definition

-- Drop table

-- DROP TABLE dwh.fct_invoice_details;

CREATE TABLE dwh.fct_invoice_details (
	ma_hoa_don text NULL,
	cap_kho int4 NULL,
	loai_hoa_don int4 NULL,
	ten_loai_hoa_don text NULL,
	pt_thanh_toan text NULL,
	ma_trang_thai int4 NULL,
	ten_trang_thai text NULL,
	ma_ncc text NULL,
	ngay_xuat timestamp NULL,
	so_chung_tu text NULL,
	kho_bi_id int4 NULL,
	ma_kho_ban text NULL,
	id_kho_ban int4 NULL,
	ngay_tao timestamp NULL,
	tyle_ck_tinh float8 NULL,
	sl_thay_doi float8 NULL,
	ma_loai_hang int4 NULL,
	ten_loai_hang text NULL,
	don_gia float8 NULL,
	id_san_pham int4 NULL,
	ma_san_pham text NULL,
	dt_sau_ck_tinh float8 NULL,
	so_luong float8 NULL,
	so_luong_unit float8 NULL,
	vat float8 NULL,
	thanh_tien float8 NULL,
	tong_gt_sau_ck float8 NULL,
	nguon text NULL,
	etl_date date NULL
);