
from spider import get_fund_list, get_net_worth
from mysql import FundSQL
from threading import Thread, Event, Lock
import queue
import tools
import csv
from config.config import Config

cfg_global = Config('global')
MODE = cfg_global.config['MODE']
DOWNLOAD_DIR = '../download/'


def thread_data_insert_sql(dict_queue: queue, sql: FundSQL, stop_event: Event):
    while True:
        # 获取队列数据
        (data_dict_list, fund_code) = dict_queue.get()
        # 将队列里的净值数据加入数据库
        sql.insert_fund_net_worth(data_dict_list, fund_code)
        # 终止事件触发后，存完所有数据后终止线程
        if stop_event.is_set() and dict_queue.qsize() == 0:
            break


def thread_data_insert_csv(dict_queue: queue, stop_event: Event):
    while True:
        # 获取队列数据

        (data_dict_list, fund_code) = dict_queue.get()
        # 将队列里的净值数据加入数据库
        if fund_code is None:
            path = DOWNLOAD_DIR + f'fund.csv'
            row_list = data_dict_list
        else:
            path = DOWNLOAD_DIR + f'fund_{fund_code}.csv'
            row_list = []
            for data_dict in data_dict_list:
                row_list.append({
                        'FundCode':data_dict['fund_code'],
                        'FundName':data_dict['fund_name'],
                        'TradingDay':data_dict['date'],
                        'NetWorth':data_dict['net_worth_sum']
                    }
                )

        with open(path, 'w',encoding='utf-8') as f:
            columns = list(row_list[0].keys())
            csv_writer = csv.DictWriter(f, fieldnames=columns,)
            csv_writer.writeheader()
            csv_writer.writerows(row_list)
        # 终止事件触发后，存完所有数据后终止线程
        if stop_event.is_set() and dict_queue.qsize() == 0:
            break


def sql_main():
    # 初始化
    sql_lock = Lock()
    sql = FundSQL(sql_lock)
    dict_queue = queue.Queue()
    stop_event = Event()

    # 开启一个线程异步将获取的数据添加到数据库
    t = Thread(target=thread_data_insert_sql,
               args=(dict_queue, sql, stop_event))
    t.start()

    # 获取基金列表
    fund_list = get_fund_list()
    print(f'成功获取基金列表：{len(fund_list)}条')
    # 创建基金产品表
    sql.create_fund_table()
    for fund in fund_list:
        fund_code = fund[0]
        fund_name = fund[1]
        # 创建每个基金产品对应的表
        sql.create_fund_code_table(code=fund_code)
        # 将建表后的产品加入基金产品表
        sql.insert_fund(code=fund_code, name=fund_name)
        # 获取净值列表
        dict_list = get_net_worth(fund_code, fund_name)
        # 将净值列表加入处理队列
        dict_queue.put((dict_list, fund_code))

    # 终止事件触发，不会有新的数据传入数据库存入线程
    stop_event.set()
    t.join()
    print('所有数据获取成功并存入数据库')
    sql.close()


def local_main():
    # 初始化
    tools.check_dir(DOWNLOAD_DIR)
    dict_queue = queue.Queue()
    stop_event = Event()

    # 开启一个线程异步将获取的数据添加到本地
    t = Thread(target=thread_data_insert_csv, args=(dict_queue, stop_event))
    t.start()

    # 获取基金列表
    fund_list = get_fund_list()
    print(f'成功获取基金列表：{len(fund_list)}条')
    fund_dict_list = []
    for fund in fund_list:
        fund_code = fund[0]
        fund_name = fund[1]
        fund_dict_list.append({
            'FundCode': fund_code,
            'FundName': fund_name
        })
        # 获取净值列表
        dict_list = get_net_worth(fund_code, fund_name)
        # 将净值列表加入处理队列
        dict_queue.put((dict_list, fund_code))
    dict_queue.put((fund_dict_list, None))
    # 终止事件触发，不会有新的数据传入数据库存入线程
    stop_event.set()
    t.join()
    print('所有数据获取成功并存入本地download文件夹')


if __name__ == '__main__':
    if MODE == 'mysql':
        sql_main()
    if MODE == 'local':
        local_main()
