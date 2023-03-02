解题过程
===
1. 浏览基金页面找到净值数据页面http://fundf10.eastmoney.com/jjjz_290008.html
2. F12检查获取数据的请求头，确定净值获取的api：`http://api.fund.eastmoney.com/f10/lsjz?callback=jQuery18307682204501251404_1677750744040&fundCode=290008&pageIndex=5&pageSize=20&startDate=&endDate=&_=1677750759141`
3. 分析api结构
   ```
    url=http://api.fund.eastmoney.com/f10/lsjz
   ```
4. 