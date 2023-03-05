import pymysql
from config.config import Config
from sqlalchemy import create_engine
cfg_global = Config('global')


# 锁装饰器
def lock_sql(func):
    def warp(self, *args, **kwargs):
        # 无锁直接执行，有锁加锁
        if self.lock is None:
            return func(self, *args, **kwargs)
        else:
            self.lock.acquire()
            res = func(self, *args, **kwargs)
            self.lock.release()
            return res
    return warp


class SQL():
    # 多线程时需要传入锁
    def __init__(self, lock=None) -> None:
        # 自动连接数据库
        self.conn = self.connect()
        self.lock = lock
        print('成功连接数据库')

    # 建立数据库连接
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

    # 关闭数据库连接
    def close(self):
        self.conn.close()

    # 返回当前连接的cursor
    def cursor(self):
        return self.conn.cursor()

    # 处理sql语句
    def execute_sql(self, sql, values=None):
        cursor = self.cursor()

        if values is None:
            cursor.execute(sql)
        else:
            cursor.execute(sql, values)

        self.conn.commit()
        cursor.close()

    # 批量处理sql语句
    def executemany_sql(self, sql, values):
        cursor = self.cursor()
        cursor.executemany(sql, values)
        self.conn.commit()
        cursor.close()

    # 返回一个数据库引擎
    def db_engine(self):
        mysql_config = cfg_global.config["MYSQL"]
        host = mysql_config['HOST']
        user = mysql_config['USER']
        passwd = mysql_config['PASSWD']
        port = mysql_config['PORT']
        db = mysql_config['DB']

        engine = create_engine(
            f'mysql+pymysql://{user}:{passwd}@{host}:{port}/{db}')
        return engine


# 将公用方法和特定操作的私有方法分离开
class FundSQL(SQL):
    #创建fund表
    @lock_sql
    def create_fund_table(self):
        sql_create_table = f"""CREATE TABLE IF NOT EXISTS fund (
            FundCode CHAR(64),
            FundName CHAR(64) NOT NULL,
            PRIMARY KEY(FundCode)
        )
        """

        self.execute_sql(sql_create_table)

    #将基金信息插入fund表
    @lock_sql
    def insert_fund(self, code, name):
        sql_insert_fund = f"""INSERT INTO fund (
                FundCode,
                FundName
            ) VALUES(
                %s,
                %s
            )
            """
        values = (code, name)

        self.execute_sql(sql_insert_fund, values)

    # 如果表不存在，创建表fund_{code}
    @lock_sql
    def create_fund_code_table(self, code):
        sql_create_table = f"""CREATE TABLE IF NOT EXISTS fund_{code} (
            FundCode CHAR(64) NOT NULL,
            FundName CHAR(64) NOT NULL,
            TradingDay CHAR(64) ,
            NetWorth FLOAT NOT NULL,
            PRIMARY KEY(TradingDay),
            FOREIGN KEY(FundCode) REFERENCES fund(FundCode)
        )
        """

        self.execute_sql(sql_create_table)

    #将字典数据插入数据库
    @lock_sql
    def insert_fund_net_worth(self, data_dict_list, code):
        sql_insert_net_worth = f"""INSERT INTO fund_{code} (
            FundCode,
            FundName,
            TradingDay,
            NetWorth
        ) VALUES(
            %s,
            %s,
            %s,
            %s
        )
        """
        values = []
        for data_dict in data_dict_list:
            value = (
                data_dict['fund_code'],
                data_dict['fund_name'],
                data_dict['date'],
                data_dict['net_worth_sum']
            )
            values.append(value)

        self.executemany_sql(sql_insert_net_worth, values)
