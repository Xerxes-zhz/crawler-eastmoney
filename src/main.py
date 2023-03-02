
from spider import get_fund_list, get_net_worth
from mysql import SQL
def main():
    fund_list = get_fund_list()

    print(f'成功获取基金列表：{len(fund_list)}条')

    sql = SQL()
    data_list={}
    for fund in  fund_list:
        fund_code = fund[0]
        fund_name = fund[1]
        #create table fund_code

        dict_list = get_net_worth(fund_code, fund_name)
        
        print(dict_list)
        

if __name__ == '__main__':
    main()