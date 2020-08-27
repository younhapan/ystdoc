逾期相关数据:

应还日期
放款单数（即对应 应还订单数）
首逾单数
未还单数

首日
2日
3日
7日
15日
30日


SELECT t1.repay_time auto_repay_time,
       t1.borrow_id,
       t1.state,
       t2.repay_time finish_repay_time,
       timestampdiff(second, t1.repay_time, CASE WHEN t2.repay_time IS NULL THEN now() ELSE t2.repay_time END) time_diff
FROM cl_borrow_repay t1
LEFT JOIN cl_borrow_repay_log t2 ON t1.borrow_id = t2.borrow_id
WHERE date(t1.repay_time) BETWEEN '2020-07-28' AND '2020-07-29'



SELECT t1.repay_time auto_repay_time, t1.borrow_id, t2.repay_time finish_repay_time,
       timestampdiff(second, t1.repay_time, CASE WHEN t2.repay_time IS NULL THEN now() ELSE t2.repay_time END) time_diff
FROM cl_borrow_repay t1
LEFT JOIN cl_borrow_repay_log t2 ON t1.borrow_id = t2.borrow_id
WHERE t1.repay_time >= '%s' AND t1.repay_time < '%s'









申请单数：
漏斗中的申请订单数>天级监控申请订单数 —— 天级监控中未计算type=nan的订单

机审通过订单数：
漏斗!=天级监控
①basic_matrix中suggestion=1，cl_borrow_progress中state=21(机审不通过)
②cl_borrow_progress中state=22(机审通过待人审)，basic_matrix中type=nan




borrow_id: 4870, 4907, mysql里记的状态是机审不通过（state=21），但是风控给的决策结果suggestion=1。
最近每天都有一两单的结果是这样对不上的，辛苦看一下~



