1、金币体系可能的问题
已发现首充问题


周卡，能否同一时间多次购买
能否重复领取


1.1 是否能在同一时间多次购买 ———— 验证用户连续两次购买周卡之间的时间间隔，是否 >= 7 天

WITH wkc AS
    (SELECT t.logdate logdate,
           t.user_id user_id,
           t.price price,
           row_number() over(partition by t.user_id order by t.`time`) purchase_rn
    FROM sharemax_dw.transaction t
    WHERE t.logdate BETWEEN '2020-03-16' AND '2020-04-16'
      AND t.type = 'buy_coin_card' -- 用户购买周卡行为
      AND t.status = 1
      AND t.purchase_type IS NULL)

SELECT w1.logdate logdate,
       w1.user_id user_id,
       w1.price price,
       w1.purchase_rn purchase_rn,
       w2.logdate logdate2,
       w2.price price2,
       w2.purchase_rn purchase_rn2,
       datediff(w2.logdate, w1.logdate) date_diff
FROM wkc w1
LEFT JOIN wkc w2
ON w1.user_id = w2.user_id AND w1.purchase_rn + 1 = w2.purchase_rn
WHERE datediff(w2.logdate, w1.logdate) >= 7


周卡的时效是按照当地时间计算 time_zone_in_capital

WITH tmp AS
    (SELECT to_date(from_utc_timestamp(from_unixtime(cast(substr(unix_timestamp(t.`time`),1,10) AS int)),
                  CASE WHEN co.iso2 IS NULL THEN 'Asia/Kolkata'
                       ELSE co.time_zone_in_capital
                  END)) capital_date,
           t.*
    FROM sharemax_dw.transaction t
    JOIN user_center.funshare_user_accounts u ON t.user_id = u.user_id
    LEFT JOIN sharemax_dw.country_info co ON u.country = co.iso2
    WHERE t.logdate BETWEEN date_sub('2020-03-16',1) AND '2020-04-16'
      AND to_date(from_utc_timestamp(from_unixtime(cast(substr(unix_timestamp(t.`time`),1,10) AS int)),
                  CASE WHEN co.iso2 IS NULL THEN 'Asia/Kolkata'
                       ELSE co.time_zone_in_capital
                  END)) BETWEEN '2020-03-16' AND '2020-04-16'
      AND t.type = 'buy_coin_card' -- 用户购买周卡行为
      AND t.status = 1
      AND t.purchase_type IS NULL)

SELECT wkc.capital_date capital_date,
        wkc.user_id user_id,
        wkc.price price,
        datediff(wkc.capital_date, wkc.lag_date) date_diff
FROM
    (SELECT t.capital_date,
            t.user_id,
            t.price,
            lag(t.capital_date, 1) over(partition by t.user_id order by t.`time`) lag_date
    FROM tmp t) wkc
WHERE datediff(wkc.capital_date, wkc.lag_date) < 7








-- SELECT wkc.capital_date capital_date,
--        wkc.user_id user_id,
--        wkc.price price,
--        datediff(wkc.logdate, wkc.lag_date) date_diff
-- FROM
--     (SELECT to_date(from_utc_timestamp(from_unixtime(cast(substr(unix_timestamp(t.`time`),1,10) AS int)),
--                   CASE WHEN co.iso2 IS NULL THEN 'Asia/Kolkata'
--                        ELSE co.time_zone_in_capital
--                   END)) capital_date,
--            t.user_id user_id,
--            t.price price,
--            lag(t.logdate, 1) over(partition by t.user_id order by t.`time`) lag_date
--     FROM sharemax_dw.transaction t
--     JOIN user_center.funshare_user_accounts u ON t.user_id = u.user_id
--     LEFT JOIN sharemax_dw.country_info co ON u.country = co.iso2
--     WHERE t.logdate BETWEEN date_sub('2020-03-16',1) AND '2020-04-16'
--       AND to_date(from_utc_timestamp(from_unixtime(cast(substr(unix_timestamp(t.`time`),1,10) AS int)),
--                   CASE WHEN co.iso2 IS NULL THEN 'Asia/Kolkata'
--                        ELSE co.time_zone_in_capital
--                   END)) BETWEEN '2020-03-16' AND '2020-04-16'
--       AND t.type = 'buy_coin_card' -- 用户购买周卡行为
--       AND t.status = 1
--       AND t.purchase_type IS NULL) wkc
-- WHERE datediff(wkc.logdate, wkc.lag_date) < 7



1.2 周卡能否在同一天重复领取金币

领取金币按照当地时间
WITH tmp AS
    (SELECT to_date(from_utc_timestamp(from_unixtime(cast(substr(unix_timestamp(t.`time`),1,10) AS int)),
                  CASE WHEN co.iso2 IS NULL THEN 'Asia/Kolkata'
                       ELSE co.time_zone_in_capital
                  END)) capital_date,
           t.*
    FROM sharemax_dw.transaction t
    JOIN user_center.funshare_user_accounts u ON t.user_id = u.user_id
    LEFT JOIN sharemax_dw.country_info co ON u.country = co.iso2
    WHERE t.logdate BETWEEN date_sub('2020-03-16',1) AND '2020-04-16'
      AND to_date(from_utc_timestamp(from_unixtime(cast(substr(unix_timestamp(t.`time`),1,10) AS int)),
                  CASE WHEN co.iso2 IS NULL THEN 'Asia/Kolkata'
                       ELSE co.time_zone_in_capital
                  END)) BETWEEN '2020-03-16' AND '2020-04-16'
      AND t.type = 'daily_card_coin' -- 用户每日使用周卡领取金币
      AND t.status = 1
      AND t.purchase_type IS NULL)

SELECT dcc.capital_date capital_date,
       dcc.user_id user_id,
       dcc.coin coin,
       -- dcc.`time` `time`,
       datediff(dcc.capital_date, dcc.lag_date) date_diff
FROM
    (SELECT t.capital_date capital_date,
           t.user_id user_id,
           t.coin coin,
           -- t.`time` `time`,
           lag(t.capital_date, 1) over(partition by t.user_id order by t.`time`) lag_date
    FROM tmp t) dcc
WHERE datediff(dcc.capital_date, dcc.lag_date) = 0 -- 确认是否存在有用户在同一天领取金币次数>1




SELECT t.*,
        to_date(from_utc_timestamp(from_unixtime(cast(substr(unix_timestamp(t.`time`),1,10) AS int)),
                  CASE WHEN co.iso2 IS NULL THEN 'Asia/Kolkata'
                       ELSE co.time_zone_in_capital
                  END)) capital_date
    FROM sharemax_dw.transaction t
    JOIN user_center.funshare_user_accounts u ON t.user_id = u.user_id
    LEFT JOIN sharemax_dw.country_info co ON u.country = co.iso2
    WHERE t.logdate BETWEEN '2020-03-15' AND '2020-03-16'
      AND t.type = 'daily_card_coin' -- 用户每日使用周卡领取金币
      AND t.status = 1
      AND t.purchase_type IS NULL
      AND t.user_id = 25601725



2、用户充值相关
用户充值种类，渠道分布
用户充值金额分布

2.1 用户充值渠道分布
SELECT t.gateway gateway,
        t.payment payment,
        COUNT(DISTINCT t.user_id) user_cnt,
        SUM(CASE WHEN t.cash_type = 'INR' THEN t.price/85 ELSE t.price END) recharge_price,
        COUNT(1) recharge_cnt
FROM sharemax_dw.transaction t
WHERE t.logdate BETWEEN date_sub('2020-03-16',1) AND '2020-04-16'
  AND t.type IN('recharge', 'buy_coin_card')
  AND t.status = 1
  AND t.purchase_type IS NULL
GROUP BY t.gateway,
         t.payment


2.2 用户充值种类分布：周卡、首充活动、一般充值

SELECT CASE WHEN t.sku_iap_id = 'f_c_1000' THEN 'discount_activity'
            WHEN t.sku_iap_id = 'card_7' THEN 'card_7'
            ELSE 'normal_recharge'
       END AS recharge_type,
       COUNT(DISTINCT t.user_id) user_cnt,
       COUNT(1) recharge_cnt,
       SUM(CASE WHEN t.cash_type = 'INR' THEN t.price/85 ELSE t.price END) recharge_price
FROM sharemax_dw.transaction t
WHERE t.logdate BETWEEN '2020-03-16' AND '2020-04-16'
  AND t.type IN('recharge', 'buy_coin_card')
  AND t.status = 1
  AND t.purchase_type IS NULL
GROUP BY CASE WHEN t.sku_iap_id = 'f_c_1000' THEN 'discount_activity'
            WHEN t.sku_iap_id = 'card_7' THEN 'card_7'
            ELSE 'normal_recharge' END

2.3 用户充值金额分布

用户充值累计金额

SELECT max(r.recharge_cnt) max_recharge_cnt,
       min(r.recharge_cnt) min_recharge_cnt,
       avg(r.recharge_cnt) avg_recharge_cnt,
       percentile_approx(r.recharge_cnt, 0.5) median_recharge_cnt,
       max(r.recharge_price) max_recharge_price,
       min(r.recharge_price) min_recharge_price,
       avg(r.recharge_price) avg_recharge_price,
       percentile_approx(r.recharge_price, 0.5) median_recharge_price,
       COUNT(DISTINCT r.user_id) recharge_user_cnt,
       SUM(r.recharge_cnt) recharge_cnt,
       SUM(r.recharge_price) recharge_price
FROM
    (SELECT t.user_id user_id,
           COUNT(1) recharge_cnt,
           SUM(CASE WHEN t.cash_type = 'INR' THEN t.price/85 ELSE t.price END) recharge_price
    FROM sharemax_dw.transaction t
    WHERE t.logdate BETWEEN '2020-03-16' AND '2020-04-16'
      AND t.type IN('recharge', 'buy_coin_card')
      AND t.status = 1
      AND t.purchase_type IS NULL
    GROUP BY t.user_id) r


用户充值单次金额

SELECT CASE WHEN t.cash_type = 'INR' THEN t.price/85 ELSE t.price END AS sku_price,
       COUNT(DISTINCT t.user_id) user_cnt,
       COUNT(1) recharge_cnt
FROM sharemax_dw.transaction t
WHERE t.logdate BETWEEN '2020-03-16' AND '2020-04-16'
  AND t.type IN('recharge', 'buy_coin_card')
  AND t.status = 1
  AND t.purchase_type IS NULL
GROUP BY CASE WHEN t.cash_type = 'INR' THEN t.price/85 ELSE t.price END







