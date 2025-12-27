import sqlite3
import logging

class Database:
    def __init__(self,name):
        self.conn = sqlite3.connect(name)
        self.cur = self.conn.cursor()
        self.logger = logging.getLogger(self.__class__.__name__)
    
    def _commit(self):
        self.conn.commit()
        self.logger.info('Changes commited')
    
    def close(self):
        self.conn.close()
        self.logger.info('Database closed')
        
    def execute(self,statement,params=None):
        try:
            if params is None:
                self.cur.execute(statement)
            else:
                self.cur.execute(statement,params)
            self._commit()
        except Exception as e:
            self.logger.error(f'Invalid Statement:\n{statement}\n{params}')  