DROP SCHEMA IF EXISTS stg CASCADE;

CREATE SCHEMA stg AUTHORIZATION myuser;
-- stg.d_cay_nhom_san_pham definition

-- Drop table

DROP TABLE IF EXISTS stg.d_cay_nhom_san_pham;

CREATE TABLE stg.d_cay_nhom_san_pham (
	ma_nhom_san_pham text NULL,
	ten_nhom_san_pham text NULL,
	ma_nhom_hang text NULL,
	ten_nhom_hang text NULL,
	ma_nganh_hang text NULL,
	ten_nganh_hang text NULL,
	etl_date date NULL
);


-- stg.d_don_vi_dg definition

-- Drop table

DROP TABLE IF EXISTS stg.d_don_vi_dg;

CREATE TABLE stg.d_don_vi_dg (
	ma_don_vi text NULL,
	ten_don_vi text NULL,
	loai_dong_goi text NULL,
	etl_date date NULL
);


-- stg.d_don_vi_tinh definition

-- Drop table

DROP TABLE IF EXISTS stg.d_don_vi_tinh;

CREATE TABLE stg.d_don_vi_tinh (
	ma_dvt text NULL,
	ten_dvt text NULL,
	etl_date date NULL
);


-- stg.d_gia_ban definition

-- Drop table

DROP TABLE IF EXISTS stg.d_gia_ban;

CREATE TABLE stg.d_gia_ban (
	ma_kenh_ban text NULL,
	ma_san_pham text NULL,
	gia text NULL,
	vat text NULL,
	ap_dung_tu text NULL,
	nguon text NULL,
	etl_date date NULL
);


-- stg.d_gia_ban_vat definition

-- Drop table

DROP TABLE IF EXISTS stg.d_gia_ban_vat;

CREATE TABLE stg.d_gia_ban_vat (
	ma_kenh_ban text NULL,
	ma_san_pham text NULL,
	gia text NULL,
	vat text NULL,
	ap_dung_tu text NULL,
	nguon text NULL,
	etl_date date NULL
);


-- stg.d_ht_dong_goi definition

-- Drop table

DROP TABLE IF EXISTS stg.d_ht_dong_goi;

CREATE TABLE stg.d_ht_dong_goi (
	ma_ht_dong_goi text NULL,
	ten_ht_dong_goi text NULL,
	etl_date date NULL
);


-- stg.d_kho definition

-- Drop table

DROP TABLE IF EXISTS stg.d_kho;

CREATE TABLE stg.d_kho (
	ma_kho text NULL,
	id_kho_v1 text NULL,
	id_kho_v2 text NULL,
	ten_kho text NULL,
	loai_hinh_kho text NULL,
	"level" text NULL,
	ma_kho_cha text NULL,
	gps text NULL,
	dien_thoai text NULL,
	dia_chi text NULL,
	ma_khu_vuc text NULL,
	ghi_chu text NULL,
	ma_trang_thai text NULL,
	etl_date date NULL
);


-- stg.d_kho_lv0 definition

-- Drop table

DROP TABLE IF EXISTS stg.d_kho_lv0;

CREATE TABLE stg.d_kho_lv0 (
	ma_kho text NULL,
	id_kho_v1 text NULL,
	id_kho_v2 text NULL,
	ten_kho text NULL,
	loai_hinh_kho text NULL,
	"level" text NULL,
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
	etl_date date NULL
);


-- stg.d_kho_lv1 definition

-- Drop table

DROP TABLE IF EXISTS stg.d_kho_lv1;

CREATE TABLE stg.d_kho_lv1 (
	ma_kho text NULL,
	id_kho_v1 text NULL,
	id_kho_v2 text NULL,
	ten_kho text NULL,
	loai_hinh_kho text NULL,
	"level" text NULL,
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
	etl_date date NULL
);


-- stg.d_kho_lv2 definition

-- Drop table

DROP TABLE IF EXISTS stg.d_kho_lv2;

CREATE TABLE stg.d_kho_lv2 (
	ma_kho text NULL,
	id_kho_v1 text NULL,
	id_kho_v2 text NULL,
	ten_kho text NULL,
	loai_hinh_kho text NULL,
	"level" text NULL,
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
	etl_date date NULL
);


-- stg.d_kho_lv3 definition

-- Drop table

DROP TABLE IF EXISTS stg.d_kho_lv3;

CREATE TABLE stg.d_kho_lv3 (
	ma_kho text NULL,
	id_kho_v1 text NULL,
	id_kho_v2 text NULL,
	ten_kho text NULL,
	loai_hinh_kho text NULL,
	"level" text NULL,
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
	etl_date date NULL
);


-- stg.d_kho_temp definition

-- Drop table

DROP TABLE IF EXISTS stg.d_kho_temp;

CREATE TABLE stg.d_kho_temp (
	ma_kho text NULL,
	id_kho_v1 text NULL,
	id_kho_v2 text NULL,
	ten_kho text NULL,
	loai_hinh_kho text NULL,
	"level" text NULL,
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
	etl_date date NULL
);


-- stg.d_kho_temp_lv3 definition

-- Drop table

DROP TABLE IF EXISTS stg.d_kho_temp_lv3;

CREATE TABLE stg.d_kho_temp_lv3 (
	ma_kho text NULL,
	id_kho_v1 text NULL,
	id_kho_v2 text NULL,
	ten_kho text NULL,
	loai_hinh_kho text NULL,
	"level" text NULL,
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
	mo_ta text NULL,
	etl_date date NULL
);


-- stg.d_khu_vuc definition

-- Drop table

DROP TABLE IF EXISTS stg.d_khu_vuc;

CREATE TABLE stg.d_khu_vuc (
	ma_khu_vuc text NULL,
	ten_khu_vuc text NULL,
	etl_date date NULL
);


-- stg.d_loai_hang definition

-- Drop table

DROP TABLE IF EXISTS stg.d_loai_hang;

CREATE TABLE stg.d_loai_hang (
	ma_loai_hang text NULL,
	ten_loai_hang text NULL,
	mo_ta text NULL,
	etl_date date NULL
);


-- stg.d_mo_hinh_mua definition

-- Drop table

DROP TABLE IF EXISTS stg.d_mo_hinh_mua;

CREATE TABLE stg.d_mo_hinh_mua (
	ma_mo_hinh_mua text NULL,
	ten_mo_hinh_mua text NULL,
	etl_date date NULL
);


-- stg.d_ncc_sp definition

-- Drop table

DROP TABLE IF EXISTS stg.d_ncc_sp;

CREATE TABLE stg.d_ncc_sp (
	ma_ncc text NULL,
	ten_dk_kinh_doanh text NULL,
	ten_viet_tat text NULL,
	loai_ncc text NULL,
	mst text NULL,
	sdt text NULL,
	dia_chi text NULL,
	ghi_chu text NULL,
	etl_date date NULL
);


-- stg.d_nguon_goc_sp definition

-- Drop table

DROP TABLE IF EXISTS stg.d_nguon_goc_sp;

CREATE TABLE stg.d_nguon_goc_sp (
	ma_nguon_goc text NULL,
	ma_vach text NULL,
	ten_nguon_goc text NULL,
	etl_date date NULL
);


-- stg.d_nhom_san_pham definition

-- Drop table

DROP TABLE IF EXISTS stg.d_nhom_san_pham;

CREATE TABLE stg.d_nhom_san_pham (
	ma_nhom_san_pham text NULL,
	ten_nhom_san_pham text NULL,
	ma_nhom_san_pham_cha text NULL,
	etl_date date NULL
);


-- stg.d_packsize definition

-- Drop table

DROP TABLE IF EXISTS stg.d_packsize;

CREATE TABLE stg.d_packsize (
	ma_packsize text NULL,
	ten_packsize text NULL,
	etl_date date NULL
);


-- stg.d_phan_loai_nganh_hang definition

-- Drop table

DROP TABLE IF EXISTS stg.d_phan_loai_nganh_hang;

CREATE TABLE stg.d_phan_loai_nganh_hang (
	ma_pl_nganh_hang text NULL,
	ten_pl_nganh_hang text NULL,
	etl_date date NULL
);


-- stg.d_san_pham definition

-- Drop table

DROP TABLE IF EXISTS stg.d_san_pham;

CREATE TABLE stg.d_san_pham (
	ma_san_pham text NULL,
	ten_san_pham text NULL,
	ma_loai_hang text NULL,
	ma_trang_thai text NULL,
	ma_pl_nganh_hang text NULL,
	ma_nhom_san_pham text NULL,
	bien_the text NULL,
	ma_thuong_hieu text NULL,
	ma_dvl text NULL,
	co_dong_goi text NULL,
	ma_dvt text NULL,
	ma_packsize text NULL,
	ma_dvc text NULL,
	quy_cach_dong_goi text NULL,
	tong_trong_luong text NULL,
	ma_hinh_thuc_dong_goi text NULL,
	ma_nguon_goc_san_pham text NULL,
	barcode_san_pham text NULL,
	barcode_thung text NULL,
	ma_ncc_san_pham text NULL,
	nha_san_xuat text NULL,
	ma_mo_hinh_mua text NULL,
	ngon_ngu_san_pham text NULL,
	han_su_dung text NULL,
	etl_date date NULL
);


-- stg.d_san_pham_temp definition

-- Drop table

DROP TABLE IF EXISTS stg.d_san_pham_temp;

CREATE TABLE stg.d_san_pham_temp (
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
	tong_trong_luong text NULL,
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
	etl_date date NULL
);


-- stg.d_thuong_hieu definition

-- Drop table

DROP TABLE IF EXISTS stg.d_thuong_hieu;

CREATE TABLE stg.d_thuong_hieu (
	ma_thuong_hieu text NULL,
	ten_thuong_hieu text NULL,
	etl_date date NULL
);


-- stg.d_trang_thai_kho definition

-- Drop table

DROP TABLE IF EXISTS stg.d_trang_thai_kho;

CREATE TABLE stg.d_trang_thai_kho (
	ma_trang_thai text NULL,
	ten_trang_thai text NULL,
	mo_ta text NULL,
	etl_date date NULL
);


-- stg.d_trang_thai_sp definition

-- Drop table

DROP TABLE IF EXISTS stg.d_trang_thai_sp;

CREATE TABLE stg.d_trang_thai_sp (
	ma_trang_thai text NULL,
	trang_thai text NULL,
	mo_ta text NULL,
	etl_date date NULL
);


-- stg.v1_invoice definition

-- Drop table

DROP TABLE IF EXISTS stg.v1_invoice;

CREATE TABLE stg.v1_invoice (
	code text NULL,
	status text NULL,
	issue_on text NULL,
	created_on text NULL,
	org text NULL,
	total_value text NULL,
	total_quantity text NULL,
	src_stock_code text NULL,
	src_stock text NULL,
	vendor_id text NULL,
	payment_type text NULL,
	note_type text NULL,
	channel text NULL,
	total_discount text NULL,
	seq text NULL,
	src_ancestors text NULL,
	so_chung_tu text NULL,
	etl_date date NULL
);


-- stg.v1_invoice_items definition

-- Drop table

DROP TABLE IF EXISTS stg.v1_invoice_items;

CREATE TABLE stg.v1_invoice_items (
	discount_rate text NULL,
	quantity_change text NULL,
	issue_type text NULL,
	issue_code text NULL,
	price text NULL,
	invoice_code text NULL,
	product_code text NULL,
	discount text NULL,
	quantity text NULL,
	unit text NULL,
	pkg_quantity text NULL,
	etl_date date NULL
);


-- stg.v2_invoice definition

-- Drop table

DROP TABLE IF EXISTS stg.v2_invoice;

CREATE TABLE stg.v2_invoice (
	code text NULL,
	status text NULL,
	issue_on text NULL,
	created_on text NULL,
	org text NULL,
	total_value text NULL,
	total_quantity text NULL,
	src_stock_code text NULL,
	src_stock text NULL,
	vendor_id text NULL,
	payment_type text NULL,
	note_type text NULL,
	channel text NULL,
	total_discount text NULL,
	seq text NULL,
	src_ancestors text NULL,
	so_chung_tu text NULL,
	etl_date date NULL
);


-- stg.v2_invoice_items definition

-- Drop table

DROP TABLE IF EXISTS stg.v2_invoice_items;

CREATE TABLE stg.v2_invoice_items (
	discount_rate text NULL,
	quantity_change text NULL,
	issue_type text NULL,
	issue_code text NULL,
	price text NULL,
	invoice_code text NULL,
	product_code text NULL,
	discount text NULL,
	quantity text NULL,
	unit text NULL,
	pkg_quantity text NULL,
	etl_date date NULL
);