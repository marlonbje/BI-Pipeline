from database import Database
import logging
from pathlib import Path
import numpy as np

class Storing:
    def __init__(self,tablename,db):
        folder = Path('data')
        if not folder.exists() or not folder.is_dir():
            folder.mkdir()
        
        self.db = Database('data/'+db)
        self.logger = logging.getLogger(self.__class__.__name__)
        self.tablename = tablename
        
    def _ddl(self,df):
        datatypes = {
            float: "FLOAT",
            np.float64: "FLOAT",
            np.float32: "FLOAT",
            int: "INTEGER",
            np.int64: "INTEGER",
            str: "TEXT",
        }
        
        cols = df.columns
        types = [type(df[col].iloc[0]) for col in cols]
        
        layout = f'CREATE TABLE IF NOT EXISTS {self.tablename}('
        
        if 'Datetime' not in cols:
                layout = layout + 'ID INTEGER PRIMARY KEY AUTOINCREMENT,'
                
        for col in zip(cols,types):
            dtype = datatypes[col[1]]
            if str(col[0].upper()) == 'DATETIME':
                layout = layout + f'{str(col[0])} {dtype} PRIMARY KEY,'
            else:
                layout = layout + f'{str(col[0])} {dtype},'
            
        layout = layout[:len(layout)-1] + ');'
        
        self.db.execute(layout)
        
        self.logger.info(f'Executed:\n{layout}')
        
        return len(cols)

    def _dml(self,df):
        df['Datetime'] = df['Datetime'].astype(str)
        collen = self._ddl(df)
        placeholder = "("
        for i in range(collen):
            placeholder = placeholder + '?,'
            
        placeholder = placeholder[:len(placeholder)-1] + ')'
        
        for j, data in df.iterrows():
            stmt = f'INSERT OR IGNORE INTO {self.tablename} VALUES {placeholder}'
            p = tuple(data.iloc[i] for i in range(len(data)))
            self.db.execute(stmt,p)
    
    def store(self,df):
        self._dml(df)
        self.db.close()