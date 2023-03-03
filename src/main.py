
from spider import get_fund_list, get_net_worth
from mysql import FundSQL
from threading import Thread, Event,Lock
import queue
import time
def thread_data_insert(dict_queue: queue, sql: FundSQL, stop_event: Event):
    while True:
        (data_dict_list, fund_code)= dict_queue.get()
        sql.insert_fund_net_worth(data_dict_list, fund_code)
        #终止事件触发后，存完所有数据后终止线程
        if stop_event.is_set() and dict_queue.qsize()==0:
            break 

def main():
    fund_list = get_fund_list()

    print(f'成功获取基金列表：{len(fund_list)}条')

    sql_lock = Lock()
    sql = FundSQL(sql_lock)
    dict_queue = queue.Queue()
    stop_event = Event()

    # 开启一个线程异步将获取的数据添加到数据库
    t = Thread(target=thread_data_insert,args=(dict_queue, sql, stop_event))
    t.start()

    sql.create_fund_table() 
    for fund in  fund_list:
        fund_code = fund[0]
        fund_name = fund[1]

        sql.create_fund_code_table(code=fund_code)
        sql.insert_fund(code=fund_code, name=fund_name)

        dict_list = get_net_worth(fund_code, fund_name)

        dict_queue.put((dict_list, fund_code))


    #终止事件触发，不会有新的数据传入数据库存入线程
    stop_event.set()
    t.join()
    print('所有数据获取成功并存入数据库')
        

if __name__ == '__main__':
    main()