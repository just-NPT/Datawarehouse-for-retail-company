DROP SCHEMA IF EXISTS mdm CASCADE;

CREATE SCHEMA mdm AUTHORIZATION myuser;
-- mdm.don_vi_dg definition

-- Drop table

-- DROP TABLE mdm.don_vi_dg;

CREATE TABLE mdm.don_vi_dg (
	ma_don_vi text NOT NULL,
	ten_don_vi text NULL,
	loai_dong_goi text NULL,
	CONSTRAINT don_vi_dg_pkey PRIMARY KEY (ma_don_vi)
);


-- mdm.don_vi_tinh definition

-- Drop table

-- DROP TABLE mdm.don_vi_tinh;

CREATE TABLE mdm.don_vi_tinh (
	ma_dvt text NOT NULL,
	ten_dvt text NULL,
	CONSTRAINT don_vi_tinh_pkey PRIMARY KEY (ma_dvt)
);


-- mdm.gia_ban_vat definition

-- Drop table

-- DROP TABLE mdm.gia_ban_vat;

CREATE TABLE mdm.gia_ban_vat (
	ma_kenh_ban text NULL,
	ma_san_pham text NULL,
	gia float8 NULL,
	vat float8 NULL,
	ap_dung_tu timestamp NULL,
	nguon text NULL
);


-- mdm.ht_dong_goi definition

-- Drop table

-- DROP TABLE mdm.ht_dong_goi;

CREATE TABLE mdm.ht_dong_goi (
	ma_ht_dong_goi text NOT NULL,
	ten_ht_dong_goi text NULL,
	CONSTRAINT ht_dong_goi_pkey PRIMARY KEY (ma_ht_dong_goi)
);


-- mdm.khu_vuc definition

-- Drop table

-- DROP TABLE mdm.khu_vuc;

CREATE TABLE mdm.khu_vuc (
	ma_khu_vuc text NOT NULL,
	ten_khu_vuc text NULL,
	CONSTRAINT khu_vuc_pkey PRIMARY KEY (ma_khu_vuc)
);


-- mdm.loai_hang definition

-- Drop table

-- DROP TABLE mdm.loai_hang;

CREATE TABLE mdm.loai_hang (
	ma_loai_hang text NOT NULL,
	ten_loai_hang text NULL,
	mo_ta text NULL,
	CONSTRAINT loai_hang_pkey PRIMARY KEY (ma_loai_hang)
);


-- mdm.mo_hinh_mua definition

-- Drop table

-- DROP TABLE mdm.mo_hinh_mua;

CREATE TABLE mdm.mo_hinh_mua (
	ma_mo_hinh_mua text NOT NULL,
	ten_mo_hinh_mua text NULL,
	CONSTRAINT mo_hinh_mua_pkey PRIMARY KEY (ma_mo_hinh_mua)
);


-- mdm.ncc_sp definition

-- Drop table

-- DROP TABLE mdm.ncc_sp;

CREATE TABLE mdm.ncc_sp (
	ma_ncc text NOT NULL,
	ten_dk_kinh_doanh text NULL,
	ten_viet_tat text NULL,
	loai_ncc text NULL,
	mst text NULL,
	sdt text NULL,
	dia_chi text NULL,
	ghi_chu text NULL,
	CONSTRAINT ncc_sp_pkey PRIMARY KEY (ma_ncc)
);


-- mdm.nguon_goc_sp definition

-- Drop table

-- DROP TABLE mdm.nguon_goc_sp;

CREATE TABLE mdm.nguon_goc_sp (
	ma_nguon_goc text NOT NULL,
	ma_vach text NULL,
	ten_nguon_goc text NULL,
	CONSTRAINT nguon_goc_sp_pkey PRIMARY KEY (ma_nguon_goc)
);


-- mdm.packsize definition

-- Drop table

-- DROP TABLE mdm.packsize;

CREATE TABLE mdm.packsize (
	ma_packsize text NOT NULL,
	ten_packsize text NULL,
	CONSTRAINT packsize_pkey PRIMARY KEY (ma_packsize)
);


-- mdm.phan_loai_nganh_hang definition

-- Drop table

-- DROP TABLE mdm.phan_loai_nganh_hang;

CREATE TABLE mdm.phan_loai_nganh_hang (
	ma_pl_nganh_hang text NOT NULL,
	ten_pl_nganh_hang text NULL,
	CONSTRAINT phan_loai_nganh_hang_pkey PRIMARY KEY (ma_pl_nganh_hang)
);


-- mdm.thuong_hieu definition

-- Drop table

-- DROP TABLE mdm.thuong_hieu;

CREATE TABLE mdm.thuong_hieu (
	ma_thuong_hieu text NOT NULL,
	ten_thuong_hieu text NULL,
	CONSTRAINT thuong_hieu_pkey PRIMARY KEY (ma_thuong_hieu)
);


-- mdm.trang_thai_kho definition

-- Drop table

-- DROP TABLE mdm.trang_thai_kho;

CREATE TABLE mdm.trang_thai_kho (
	ma_trang_thai text NOT NULL,
	ten_trang_thai text NULL,
	mo_ta text NULL,
	CONSTRAINT trang_thai_kho_pkey PRIMARY KEY (ma_trang_thai)
);


-- mdm.trang_thai_sp definition

-- Drop table

-- DROP TABLE mdm.trang_thai_sp;

CREATE TABLE mdm.trang_thai_sp (
	ma_trang_thai text NOT NULL,
	trang_thai text NULL,
	mo_ta text NULL,
	CONSTRAINT trang_thai_sp_pkey PRIMARY KEY (ma_trang_thai)
);


-- mdm.kho definition

-- Drop table

-- DROP TABLE mdm.kho;

CREATE TABLE mdm.kho (
	ma_kho text NOT NULL,
	id_kho_v1 float8 NULL,
	id_kho_v2 float8 NULL,
	ten_kho text NULL,
	loai_hinh_kho text NULL,
	"level" int8 NULL,
	ma_kho_cha text NULL,
	gps text NULL,
	dien_thoai float8 NULL,
	dia_chi float8 NULL,
	ma_khu_vuc text NULL,
	ghi_chu text NULL,
	ma_trang_thai text NULL,
	CONSTRAINT kho_pkey PRIMARY KEY (ma_kho),
	CONSTRAINT kho_ma_kho_cha_fkey FOREIGN KEY (ma_kho_cha) REFERENCES mdm.kho(ma_kho),
	CONSTRAINT kho_ma_khu_vuc_fkey FOREIGN KEY (ma_khu_vuc) REFERENCES mdm.khu_vuc(ma_khu_vuc),
	CONSTRAINT kho_ma_trang_thai_fkey FOREIGN KEY (ma_trang_thai) REFERENCES mdm.trang_thai_kho(ma_trang_thai)
);


-- mdm.nhom_san_pham definition

-- Drop table

-- DROP TABLE mdm.nhom_san_pham;

CREATE TABLE mdm.nhom_san_pham (
	ma_nhom_san_pham text NOT NULL,
	ten_nhom_san_pham text NULL,
	ma_nhom_san_pham_cha text NULL,
	CONSTRAINT nhom_san_pham_pkey PRIMARY KEY (ma_nhom_san_pham),
	CONSTRAINT nhom_san_pham_ma_nhom_san_pham_cha_fkey FOREIGN KEY (ma_nhom_san_pham_cha) REFERENCES mdm.nhom_san_pham(ma_nhom_san_pham)
);


-- mdm.san_pham definition

-- Drop table

-- DROP TABLE mdm.san_pham;

CREATE TABLE mdm.san_pham (
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
	quy_cach_dong_goi float8 NULL,
	tong_trong_luong text NULL,
	ma_hinh_thuc_dong_goi text NULL,
	ma_nguon_goc_san_pham text NULL,
	barcode_san_pham float8 NULL,
	barcode_thung float8 NULL,
	ma_ncc_san_pham text NULL,
	nha_san_xuat text NULL,
	ma_mo_hinh_mua text NULL,
	ngon_ngu_san_pham text NULL,
	han_su_dung float8 NULL,
	CONSTRAINT san_pham_ma_dvc_fkey FOREIGN KEY (ma_dvc) REFERENCES mdm.don_vi_dg(ma_don_vi),
	CONSTRAINT san_pham_ma_dvl_fkey FOREIGN KEY (ma_dvl) REFERENCES mdm.don_vi_dg(ma_don_vi),
	CONSTRAINT san_pham_ma_dvt_fkey FOREIGN KEY (ma_dvt) REFERENCES mdm.don_vi_tinh(ma_dvt),
	CONSTRAINT san_pham_ma_hinh_thuc_dong_goi_fkey FOREIGN KEY (ma_hinh_thuc_dong_goi) REFERENCES mdm.ht_dong_goi(ma_ht_dong_goi),
	CONSTRAINT san_pham_ma_loai_hang_fkey FOREIGN KEY (ma_loai_hang) REFERENCES mdm.loai_hang(ma_loai_hang),
	CONSTRAINT san_pham_ma_mo_hinh_mua_fkey FOREIGN KEY (ma_mo_hinh_mua) REFERENCES mdm.mo_hinh_mua(ma_mo_hinh_mua),
	CONSTRAINT san_pham_ma_ncc_san_pham_fkey FOREIGN KEY (ma_ncc_san_pham) REFERENCES mdm.ncc_sp(ma_ncc),
	CONSTRAINT san_pham_ma_nguon_goc_san_pham_fkey FOREIGN KEY (ma_nguon_goc_san_pham) REFERENCES mdm.nguon_goc_sp(ma_nguon_goc),
	CONSTRAINT san_pham_ma_nhom_san_pham_fkey FOREIGN KEY (ma_nhom_san_pham) REFERENCES mdm.nhom_san_pham(ma_nhom_san_pham),
	CONSTRAINT san_pham_ma_packsize_fkey FOREIGN KEY (ma_packsize) REFERENCES mdm.packsize(ma_packsize),
	CONSTRAINT san_pham_ma_pl_nganh_hang_fkey FOREIGN KEY (ma_pl_nganh_hang) REFERENCES mdm.phan_loai_nganh_hang(ma_pl_nganh_hang),
	CONSTRAINT san_pham_ma_thuong_hieu_fkey FOREIGN KEY (ma_thuong_hieu) REFERENCES mdm.thuong_hieu(ma_thuong_hieu),
	CONSTRAINT san_pham_ma_trang_thai_fkey FOREIGN KEY (ma_trang_thai) REFERENCES mdm.trang_thai_sp(ma_trang_thai)
);