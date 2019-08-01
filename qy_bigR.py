# -*- coding: utf-8 -*-
"""
Created on Mon Jul  8 15:53:22 2019

@author: xiaoniu
"""

import numpy as np
import sys
sys.path.append('../qy_con')
import qy_con
def main():
    sql_uid = '''
    select distinct uid
    from (
    -- 24小时内充值满2000
    select c.uid,sum(amount) invest_num
    from (select * from recharge where type = 1 and state = 2 and finish_date >= date_add(now(),interval -1 day)) r
    left join customer c on c.customer_id = r.customer_id
    group by c.uid
    having invest_num >2000
    union all 
    -- 过去 7*24小时内充值超过5000
    select c.uid,sum(amount) invest_num
    from (select * from recharge where type = 1 and state = 2 and finish_date >= date_add(now(),interval -7 day)) r
    left join customer c on c.customer_id = r.customer_id
    group by c.uid
    having invest_num >5000 ) a
    '''
    new_uid = qy_con.read_con1(sql_uid)
    old_uid = qy_con.read_con3('select uid from qy_bigr')
    full_uid = new_uid.append(old_uid).drop_duplicates(keep='first')
    add_uid = full_uid.append(old_uid).drop_duplicates(keep=False)
    add_uid = np.array(add_uid).flatten()
    if len(add_uid) >= 1:
        sql = '''
        select cu.uid,cu.nick_name,cu.create_time as register_date -- 注册时间
        ,cu.app_channel_name -- 注册渠道
        ,min(re.finish_date) as first_invest_time -- 首充时间
        ,sum(amount) as amount -- 累计充值金额
        ,now() etl_time -- 成为大R的时间
        from (select * from customer where uid in (%s)) cu
        left join recharge re on cu.customer_id = re.customer_id
        group by cu.uid
        '''% (",".join(map(str,add_uid)))
        qy_bigr = qy_con.read_con1(sql)
        qy_con.to_conbi(qy_bigr,'qy_bigr',if_exists='append')
    
if __name__ == '__main__':
    try:
        main()
        print('qy_bigr 执行成功')
    except Exception as e:
        print('qy_bigr 报错：',e)
    finally:
        pass