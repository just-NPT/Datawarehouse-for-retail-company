CREATE TABLE mdm.kho (
	ma_kho text NULL PRIMARY KEY,
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
	FOREIGN KEY (ma_kho_cha) REFERENCES mdm.kho(ma_kho),
	FOREIGN KEY (ma_trang_thai) REFERENCES mdm.trang_thai_kho(ma_trang_thai),
	FOREIGN KEY (ma_khu_vuc) REFERENCES mdm.khu_vuc(ma_khu_vuc)
);