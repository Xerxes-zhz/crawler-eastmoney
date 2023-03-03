import pymysql
from config.config import Config
cfg_global = Config('global')

def lock_sql(func):
    def warp(self, *args, **kwargs):
        self.lock.acquire()
        res=func(self, *args, **kwargs)
        self.lock.release()
        return res
    return warp

class SQL():
    def __init__(self,lock) -> None:
        self.conn = self.connect()
        self.lock = lock
        print('成功连接数据库')
    #建立数据库连接
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
    
    #返回当前连接的cursor
    def cursor(self):
        return self.conn.cursor()
    def _lock(self):
        pass
    
#将公用方法和特定操作的私有方法分离开
class FundSQL(SQL):
    @lock_sql
    def create_fund_table(self):
       
        sql_create_table = f"""CREATE TABLE IF NOT EXISTS fund (
            code CHAR(64),
            name CHAR(64) NOT NULL,
            PRIMARY KEY(code)
        )
        """
        cursor = self.cursor()
        cursor.execute(sql_create_table)
        self.conn.commit()
        cursor.close()

    @lock_sql
    def insert_fund(self, code, name):
        sql_insert_fund = f"""INSERT INTO fund (
                code,
                name
            ) VALUES(
                %s,
                %s
            )
            """
        values = (code, name)
        cursor = self.cursor()
        cursor.execute(sql_insert_fund, values)
        self.conn.commit()
        cursor.close()
    #如果不存在，创建表fund_{code}
    @lock_sql
    def create_fund_code_table(self, code):

        sql_create_table = f"""CREATE TABLE IF NOT EXISTS fund_{code} (
            code CHAR(64) NOT NULL,
            date CHAR(64) ,
            name CHAR(64) NOT NULL,
            net_worth_unit FLOAT NOT NULL,
            net_worth_sum FLOAT NOT NULL,
            PRIMARY KEY(date),
            FOREIGN KEY(code) REFERENCES fund(code)
        )
        """
        cursor = self.cursor()

        cursor.execute(sql_create_table)
        self.conn.commit()
        cursor.close()

    @lock_sql
    def insert_fund_net_worth(self, data_dict_list, code):
        sql_insert_net_worth = f"""INSERT INTO fund_{code} (
            code,
            date,
            name,
            net_worth_unit,
            net_worth_sum
        ) VALUES(
            %s,
            %s,
            %s,
            %s,
            %s
        )
        """
        values=[]
        for data_dict in data_dict_list:
            value=( 
                data_dict['fund_code'],
                data_dict['date'],
                data_dict['fund_name'],
                data_dict['net_worth_unit'],
                data_dict['net_worth_sum']
            )
            values.append(value)

        cursor = self.cursor()
        cursor.executemany(sql_insert_net_worth,values)
        self.conn.commit()
        cursor.close()
        pass
