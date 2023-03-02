import pymysql
from config.config import Config
cfg_global = Config('global')

class SQL():
    def __init__(self) -> None:
        self.conn = self.connect()
        print(self.conn)

    def connect(self):
        mysql_config = cfg_global.config["MYSQL"]
        host = mysql_config['HOST']
        user = mysql_config['USER']
        passwd = mysql_config['PASSWD']
        port = mysql_config['PORT']
        db = mysql_config['DB']
        charset = mysql_config['CHARSET']
        
        return pymysql.connect(
            host=host, 
            user=user, 
            passwd=passwd,
            port=port,
            db=db,
            charset=charset
        )

    def cursor(self):
        return self.conn.cursor()

