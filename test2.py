import os
import re
from datetime import datetime

for table in ['D_KHO','D_LOAI_HANG','D_THUONG_HIEU','D_PHAN_LOAI_NGANH_HANG','D_NHOM_SAN_PHAM','D_PACKSIZE','D_DON_VI_TINH','D_DON_VI_DG','D_HT_DONG_GOI','D_NGUON_GOC_SP','D_MO_HINH_MUA','D_TRANG_THAI_SP','D_NCC_SP','D_SAN_PHAM','D_TRANG_THAI_KHO','D_KHU_VUC']:
  # table = 'D_DON_VI_DG'
  # Directory containing the files
  directory = f'./data/MDM/{table}/'

  # Define the date range
  start_date = datetime.strptime('20240317', '%Y%m%d')
  end_date = datetime.strptime('20240322', '%Y%m%d')

  # Regular expression to match the date in the filename
  date_pattern = re.compile(r'\d{8}')

  # Loop through all files in the directory
  for filename in os.listdir(directory):
      # print(filename)
      # Find the date in the filename
      match = date_pattern.search(filename)
      if match:
          file_date = datetime.strptime(match.group(), '%Y%m%d')
          # Check if the date is outside the specified range
          if not (start_date <= file_date <= end_date):
              # Construct the full file path
              file_path = os.path.join(directory, filename)
              # Delete the file
              os.remove(file_path)
              print(f'Deleted: {file_path}')