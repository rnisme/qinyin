# -*- coding: utf-8 -*-
"""
Created on Mon Jul  8 15:45:54 2019

@author: xiaoniu
"""


import sys
sys.path.append('../qy_con')
import qy_con

def main():
    sql = '''
    select cu.uid,cu.nick_name,cu.create_time as register_date -- 注册时间
    ,cu.app_channel_name -- 注册渠道
    ,min(re.finish_date) as first_invest_time -- 首充时间
    ,sum(amount) as amount -- 累计充值金额
    ,now() etl_time -- 成为大R的时间
    from (select * from customer where uid in (  
    select distinct uid
    from(
    -- 单天充值超过2000
    select c.uid,date(r.finish_date),sum(amount) invest_num
    from (select * from recharge where type = 1 and state = 2) r
    left join customer c on c.customer_id = r.customer_id
    group by c.uid,date(r.finish_date)
    having invest_num >2000
    ) a
    )) cu
    left join recharge re on cu.customer_id = re.customer_id
    group by cu.uid
    '''
    qy_bigr = qy_con.read_con1(sql)
    qy_con.to_conbi(qy_bigr,'qy_bigr',if_exists='replace')
    
if __name__ == '__main__':
    try:
        main()
        print('qy_bigr 执行成功')
    except Exception as e:
        print('qy_bigr 报错：',e)
    finally:
        pass