import tools
import pandas as pd
import numpy as np
from mysql import FundSQL
from config.config import Config

cfg_global = Config('global')
MODE = cfg_global.config['MODE']
RESULT_DIR = '../result/'
DATA_DIR = '../data/'
DOWNLOAD_DIR = '../download/'


def local_main():
    dict_df_fund_code = {}
    fund_list = []
    
    #从本地获取基金数据
    df_fund = pd.read_csv(DOWNLOAD_DIR+'fund.csv',converters={'FundCode':str})
    # 全量数据导出
    df_fund.to_csv(DATA_DIR+'fund.csv', index=False)
    
    fund_list = df_fund['FundCode']
    for code in fund_list:
        df = pd.read_csv(DOWNLOAD_DIR+f'fund_{code}.csv',converters={'FundCode':str})
        # 全量数据导出
        df.to_csv(DATA_DIR+f'fund_{code}.csv')
        dict_df_fund_code[code] = df

    return dict_df_fund_code, df_fund


def sql_main():
    # 初始化
    sql = FundSQL()
    sql_engine = sql.db_engine()
    sql_con = sql_engine.connect()
    dict_df_fund_code = {}

    #从数据库获取基金数据
    df_fund = pd.read_sql_table(table_name='fund', con=sql_con)
    # 全量数据导出
    df_fund.to_csv(DATA_DIR+'fund.csv', index=False)

    fund_list = df_fund['FundCode']
    for code in fund_list:
        df = pd.read_sql_table(table_name=f'fund_{code}', con=sql_con)
        # 全量数据导出
        df.to_csv(DATA_DIR+f'fund_{code}.csv')
        dict_df_fund_code[code] = df

    sql.close()
    return dict_df_fund_code, df_fund

# 获取字典作为容器的产品数据表，和仅有code和name的总表


def process_main(dict_df, df_fund):
    #子函数中使用的全局变量：dict_df, df_fund, year_set, year_list
    # 年化收益率
    def _avg_return_rate(df):
        ser_net_worth = df['NetWorth']
        len = ser_net_worth.count()
        return (ser_net_worth.iat[len-1]/ser_net_worth.iat[0]-1)/len*252
    # 夏普比例

    def _sharpe(df):
        # 年 无风险利率
        un_risk_rate_year = cfg_global.config['UN_RISK_RATE_YEAR']
        # 年化波动率
        std_return_rate_year = df['ReturnRate'].std()*np.sqrt(252)
        # 年化收益率
        return_rate_year = _avg_return_rate(df)

        return (return_rate_year - un_risk_rate_year) / (std_return_rate_year)

    # 收益
    def _return(df):
        return round(df['Return'].sum(), 3)

    # 年化收益
    def _avg_return(df):
        return round(df['Return'].mean()*252, 3)

    # 回撤
    def _drawdown(df):
        arr = df['NetWorth'].to_numpy()
        i = 0
        _max = -1
        _max_drawdown = -1
        if arr[0] == arr.max():
            return (arr[0]-arr.min())/arr[0]
        while True:
            # 后续无最大值跳出
            if _max >= arr[i:].max() or arr[-1] == arr[i:].max():
                break
            # 假如当前是最大值，找到后续最大的
            while arr[i] >= _max:
                _max = arr[i]
                i += 1
            # 算出当前区间回撤
            _range_drawdown = (_max-arr[i:].min())/_max
            # 如果是最大回撤更新最大回撤
            if _range_drawdown >= _max_drawdown:
                _max_drawdown = _range_drawdown
            # 后续无最大值跳出
            if _max >= arr[i:].max() or arr[-1] == arr[i:].max():
                break
            # 找到后续第一个最大值
            while arr[i] < _max:
                i += 1
        return _max_drawdown

    # 基金数据表单表初始化
    def _df_fund_code_init(code):
        df = dict_df[code]
        # 增加 收益字段
        df['Return'] = df['NetWorth'] - df['NetWorth'].shift(1)
        df.at[0, 'Return'] = 0
        # 增加收益率字段
        df['ReturnRate'] = df['Return'] / df['NetWorth'].shift(1)
        df.at[0, 'ReturnRate'] = 0
        # 增加 年月字段
        df['Year'] = df['TradingDay'].str.split('-', expand=True)[0]
        df['Month'] = df['Year']+ '-' + df['TradingDay'].str.split('-', expand=True)[1]
        year_list = df['Year'].unique()
        year_set.update(year_list)

    # 基金总表初始化
    def _df_fund_init(df):
        df['return'] = None
        df['avg_return'] = None
        df['sharpe'] = None
        df['drawdown'] = None
        year_para_list = ['return', 'sharpe', 'drawdown']
        for para in year_para_list:
            for year in year_list:
                df[f'{year}_{para}'] = None

    # 月数据处理
    def _month_process(month_group):
        year_list = []
        month_list = []
        month_return_list = []
        for name, group in month_group:
            y, m = name.split('-')
            year_list.append(y)
            month_list.append(m)
            month_return_list.append(round(group['Return'].sum(), 3))

        data = {
            'year': year_list,
            'month': month_list,
            'fund_name': [df_fund[df_fund['FundCode'] == code]['FundName'].iat[0]] * len(month_list),
            'return': month_return_list
        }
        return pd.DataFrame(data)

    # 年数据处理
    def _year_process(year_group):
        for name, group in year_group:
            year = name
            df = group
            df_fund.loc[
                df_fund['FundCode'] == code, [
                    f'{year}_return',
                    f'{year}_sharpe',
                    f'{year}_drawdown'
                ]
            ] = [
                _return(df),
                _sharpe(df),
                _drawdown(df)
            ]

    # 初始化
    fund_list = df_fund['FundCode']
    year_set = set()
    for code in fund_list:
        # 给数据表增加字段
        _df_fund_code_init(code)
    year_list = list(year_set)
    year_list.sort(reverse=True)
    # 给指标总表增加字段
    _df_fund_init(df_fund)

    print('数据初始化完成')

    # 数据处理开始
    dict_month_df = {}
    for code in fund_list:
        df = dict_df[code]

        # 处理月数据
        month_group = df.groupby('Month')
        dict_month_df[code] = _month_process(month_group)

        # 处理年数据
        year_group = df.groupby('Year')
        _year_process(year_group)

        # 总表全量数据处理
        df_fund.loc[
            df_fund['FundCode'] == code, [
                'return',
                'avg_return',
                'sharpe',
                'drawdown'
            ]
        ] = [
            _return(df),
            _avg_return(df),
            _sharpe(df),
            _drawdown(df)
        ]
    print('数据处理完成')

    df_all_month = pd.DataFrame()
    for key in dict_month_df:
        df_all_month = pd.concat([df_all_month, dict_month_df[key]])
        dict_month_df[key].to_csv(RESULT_DIR + 'month/' + key + '.csv', index=False)

    df_all_month.to_csv(RESULT_DIR+'fund_all_month.csv', index=False)
    df_fund.rename(columns={
        'FundCode': 'fund_code',
        'FundName': 'fund_name'
    }, inplace=True)
    df_fund.to_csv(RESULT_DIR+'fund.csv', index=False)

    print('数据输出完成')



if __name__ == '__main__':
    tools.check_dir(RESULT_DIR)
    tools.check_dir(DATA_DIR)
    tools.check_dir(RESULT_DIR+'month/')

    # 将数据处理成一样的模式
    if MODE == 'mysql':
        dict_df, df_fund = sql_main()
    if MODE == 'local':
        dict_df, df_fund = local_main()
    # 数据处理主函数
    process_main(dict_df, df_fund)
