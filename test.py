import pandas as pd

df1 = pd.read_csv(f'./data/MDM/D_SAN_PHAM/D_SAN_PHAM_20240319.csv')
df2 = pd.read_csv(f'./data/MDM/D_SAN_PHAM/D_SAN_PHAM_20240318.csv')

f1 = open('./data/MDM/D_SAN_PHAM/D_SAN_PHAM_20240319.csv','rb').read()
f2 = open('./data/MDM/D_SAN_PHAM/D_SAN_PHAM_20240318.csv','rb').read()
print(f1==f2)
arr = ['1401542', '1401545', '2TC180', '2TC410', '58FR250', '58LO220', '58LO500', '5TC180', '80GLBB250', '96LO149', 'BT04', 'BTYEYV0002', 'BTYEYV0005', 'CNPM002', 'DAUAN.YA03', 'DTP1KG', 'FR220BK', 'FR250BK', 'FR250BLI', 'FR250HY', 'FR500BK', 'FR500BL', 'FR500HY', 'HCMFR149', 'LDFR220', 'LDFR250', 'LO149YB', 'LO220BL', 'LO220HY', 'M01', 'ON1FR100', 'ON1FR220', 'ON1FR250', 'ON1FR500', 'TNFR250', 'TNFR500', 'TNFR500H', 'TNLO149', 'TNLO250', 'TNLO500', 'YANTARHP3L']
print(len(arr))
df1.fillna('NULL',inplace=True)
df2.fillna('NULL',inplace=True)

def check_schema_equality(df1, df2):
    if df1.columns.tolist() == df2.columns.tolist():
        return True
    else:
        return False

schema_equal = check_schema_equality(df1, df2)
print(f"Schema Equality: {schema_equal}")
keys = {}
if schema_equal:
    def compare_values(df1, df2, key_column):
        for key, group1 in df1.groupby(key_column):
            group2 = df2[df2[key_column] == key]
            for col in df1.columns:
                if col != key_column:
                    if group1[col].iloc[0] != group2[col].iloc[0] :
                      print(group1[col].iloc[0])
                      print(group2[col].iloc[0])
                      print(key,col,'\n\n')
                      if key not in keys.keys():
                        keys[key] = [col] 
                      else:
                        keys[key].append(col)
                      break
        return True
    
    values_same = compare_values(df1, df2, 'MA_SAN_PHAM')
    print(f"Values with the dif key_column: {values_same}")
    print(keys.keys())

