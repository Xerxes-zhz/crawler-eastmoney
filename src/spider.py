import requests
import time
import sys
from random import random
import json
from config.config import Config

cfg_global = Config('global')


class Http():
    def get(url, params, headers, max_retry=0):
        try:
            response = requests.get(url=url, params=params, headers=headers)
        except:
            if max_retry < 3:
                print('get请求失败，5秒后重连')
                time.sleep(5)
                return Http.get(url, params, headers, max_retry+1)
            else:
                print('get请求重试3次后失败，停止重试，终止程序')
                sys.exit()
        return response


def get_fund_list() -> list:
    fund_list = []
    # 开放式基金共16021条 取3页300条作为作业
    max_pages = cfg_global.config['FUND_MAX_PAGES']
    for page in range(1, max_pages + 1):
        url = cfg_global.config['URL_API_FUND_LIST']  # 开放型基金列表url
        params = {
            't': 1,
            'lx': 1,
            'letter': '',
            'gsid': '',
            'text': '',
            'sort': 'zdf,desc',
            'page': f'{page},100',  # 页码，每页数量
            'dt': str(time.time()*1000),  # 13位时间戳
            'atfc': '',
            'onlySale': 0
        }
        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/110.0.0.0 Safari/537.36',
        }
        response = Http.get(url, params, headers)
        data_start = response.text.find('datas:[') + 6
        data_end = response.text.find('],count') + 1
        list = json.loads(response.text[data_start:data_end])
        for fund in list:
            fund_list.append((fund[0],fund[1]))
    return fund_list


def get_net_worth(fund_code, fund_name) -> list:
    def _mock_jQuery_callback():
        return f"jQuery183{str(random()).replace('.', '')}_{int(time.time() * 1000)}"

    def _get_api_json(page_index, page_size):
        url = cfg_global.config['URL_API_FUND_NET_WORTH']  # 基金净值

        headers = {
            'Cookie': 'qgqp_b_id=e5502b5f12094ccfbb9fab58333cec40; st_si=49210073734851; st_asi=delete; EMFUND1=null; EMFUND2=null; EMFUND3=null; EMFUND4=null; EMFUND5=null; EMFUND6=null; EMFUND7=null; EMFUND8=null; EMFUND0=null; EMFUND9=03-02 22:03:58@#$%u62DB%u5546%u4E2D%u8BC1%u767D%u9152%u6307%u6570%28LOF%29A@%23%24161725; st_pvi=03963704789141; st_sp=2021-11-10%2022%3A51%3A42; st_inirUrl=https%3A%2F%2Fwww.baidu.com%2Flink; st_sn=15; st_psi=20230302233227615-112200305283-5529533825',
            'Host': 'api.fund.eastmoney.com',
            'Referer': 'http://fundf10.eastmoney.com/',
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/110.0.0.0 Safari/537.36',
        }

        params = {
            'callback': _mock_jQuery_callback(),  # 回调函数
            'fundCode': fund_code,  # 基金代码
            'pageIndex': page_index,  # 分页
            'pageSize': page_size,  # 页面数据量
            'startDate': '',  # 开始日期
            'endDate': '',  # 结束日期
            '_': int(time.time()*1000),  # 时间戳（当前时间戳
        }

        response = Http.get(url, params, headers)

        text = response.text
        data_start = text.find('(')+1
        data_end = text.find(')')
        _json = json.loads(text[data_start:data_end])

        return _json
    
    # 仅获取一条数据来获取总条数
    data_json = _get_api_json(1, 1)
    total_count = data_json['TotalCount']

    # 通过总条数获取全部数据
    data_json = _get_api_json(1,total_count)
    data_dict_list = data_json['Data']['LSJZList']

    format_dict_list = []
    for data_dict in data_dict_list:
        # 跳过非交易日
        if data_dict['JZZZL'] =='':
            continue
        format_dict = {
            'fund_code':fund_code,
            'fund_name':fund_name,
            'date': data_dict['FSRQ'],
            'net_worth_unit': data_dict['DWJZ'],
            'net_worth_sum': data_dict['LJJZ'],
        }
        format_dict_list.append(format_dict)

    return format_dict_list

