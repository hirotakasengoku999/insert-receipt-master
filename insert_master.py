import sqlite3
import pandas as pd
import datetime
from pathlib import Path


def insert_master(df: pd.DataFrame, table_name: str) -> str:
    try:
        # SQLite3のDBに接続
        db_file = Path.cwd()/'datas'/'RECEDB'
        conn = sqlite3.connect(db_file)
        df.to_sql(table_name, conn, if_exists='replace', index=False)
        conn.commit()
        conn.close()
        return f'{table_name}にデータを登録しました'
    except Exception as e:
        return e

def read_master(in_dir: Path) -> None:
    use_cols_dict = {
        's': {'use_cols': [2, 4], 'col_names': ['MedicalCode', 'MedicalName'], 'table_name': 'M_MEDICAL_ACT'},
        'b': {'use_cols': [2, 5], 'col_names': ['DiseaseCode', 'DiseaseName'], 'table_name': 'M_DISEASE'},
        't': {'use_cols': [2, 4], 'col_names': ['EquipmentCode', 'EquipmentName'], 'table_name': 'M_EQUIPMENT'},
        'y': {'use_cols': [2, 4], 'col_names': ['MedicineCode', 'MedicineName'], 'table_name': 'M_MEDICINE'},
        'z': {'use_cols': [2, 6], 'col_names': ['ModifierCode', 'ModifierName'], 'table_name': 'M_MODIFIER'},
    }
    for file in in_dir.glob('**/*.csv'):
        # ファイル名の１文字目がuse_colsのキーに含まれているか
        if file.name[0] not in use_cols_dict.keys():
            continue
        print(f"{file.name}を読み込みます")
        use_cols = use_cols_dict[file.name[0]]['use_cols']
        df = pd.read_csv(file, engine='python', encoding='cp932', header=None, dtype='object', usecols=use_cols)
        df.columns = use_cols_dict[file.name[0]]['col_names']
        # 更新日付をYYYY-mm-dd HH:MM:SS形式で追加
        df['UpdatedTimeStamp'] = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        print(insert_master(df, use_cols_dict[file.name[0]]['table_name']))


def main():
    in_dir = Path.cwd()/'datas'
    read_master(in_dir)


if __name__ == '__main__':
    main()