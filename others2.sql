分时段的卸载是否全部都在上涨
卸载前最后一个事件

表情包campaign ID：开始日期0723
'Hindi_Whole*State-Install-Aru*Batao*Text-0722-DVIP01-AA'

正常campaign ID：
'Tamil_Whole*State-Install-Love*Videos-0228-PSP01-AA',
'Hindi_Whole*State-Install-STATES*MODIFIED*Solo*Lady*Love*Text-1206-PSP02-AA',
'Telugu_Whole*State-Install-Love*Female*Background-0218-DVIP03-AA',

聊天室campaign ID：
'Hindi_Whole*State-Install-Chatroom*Image-0802-PSP03-AA'



不同语言下【首次新增】自然流量的分时段0日卸载：

WITH new_tmp AS
(SELECT t.logdate logdate,
		t.device.advertising_id aid,
		t.traffic_source traffic_source,
		t.app_info app_info
FROM sharemax_dw.funshare_firebase_first_open t LATERAL VIEW explode(t.event_params) t AS event_param
WHERE logdate BETWEEN '2019-08-05' AND '2019-08-06'
  AND t.event_param.KEY='previous_first_open_count'
  AND t.event_param.value.int_value=0
)

SELECT new_device.logdate logdate,
		user_author.lang lang,
		remove_0.remove_hour remove_hour,
		COUNT(DISTINCT remove_0.aid) remove_0_cnt
FROM
	(SELECT min(to_date(from_unixtime(unix_timestamp(t.created_at)+19800))) AS user_create_time,
			t.account_id account_id,
			t1.current_lang lang
	FROM dm.user_authorizations AS t JOIN user_center.funshare_user_accounts AS t1 ON t.user_id = t1.user_id
	AND t1.user_type IN ('client')
	WHERE t.app = 'funshare'
	GROUP BY t.account_id,
			t1.current_lang) user_author
	JOIN
	(SELECT DISTINCT t.logdate,
					t.aid aid
	FROM new_tmp t
	) new_device --首次新增设备
	ON user_author.account_id=new_device.aid
	LEFT JOIN
	(SELECT DISTINCT t.logdate,
					t.aid aid
	FROM new_tmp t
	WHERE t.traffic_source.name IN('Tamil_Whole*State-Install-Love*Videos-0228-PSP01-AA',
	  								'Hindi_Whole*State-Install-STATES*MODIFIED*Solo*Lady*Love*Text-1206-PSP02-AA',
	  								'Telugu_Whole*State-Install-Love*Female*Background-0218-DVIP03-AA',
	  								'Hindi_Whole*State-Install-Aru*Batao*Text-0722-DVIP01-AA',
	  								'Telugu_Whole*State-Install-Love*Female*Background-0218-DVIP03-AA',
	  								'Hindi_Whole*State-Install-Chatroom*Image-0802-PSP03-AA')
	) new_ad_device --首次广告新增设备
	ON new_device.aid=new_ad_device.aid AND new_device.logdate=new_ad_device.logdate
	LEFT JOIN
	(SELECT t.logdate logdate,
		t.device.advertising_id aid,
		hour(from_unixtime(substr(event_timestamp,1,10)+19800)) remove_hour
	FROM sharemax_dw.funshare_firebase_app_remove t
	WHERE t.logdate BETWEEN '2019-08-05' AND '2019-08-06') remove_0
	ON new_device.logdate=remove_0.logdate AND new_device.aid=remove_0.aid
WHERE new_ad_device.aid IS NULL
GROUP BY new_device.logdate,
		user_author.lang,
		remove_0.remove_hour


去除掉某些事件卸载前最后事件是click_home_page的用户
click_home_page事件前最后一个事件 分布

SELECT event_name,
       count(DISTINCT user_aid) user_cnt
FROM
  (SELECT device.advertising_id AS user_aid,
          event_name,
          ROW_NUMBER() OVER (PARTITION BY device.advertising_id
                             ORDER BY event_timestamp DESC) AS rn
   FROM `funshare-8f5de.analytics_176305069.events_20190701`
   WHERE device.advertising_id IN
       (SELECT DISTINCT user_aid
        FROM
          (SELECT device.advertising_id AS user_aid,
                  event_name,
                  ROW_NUMBER() OVER (PARTITION BY device.advertising_id
                                     ORDER BY event_timestamp DESC) AS row_num
           FROM `funshare-8f5de.analytics_176305069.events_20190701`
           WHERE device.advertising_id IN
               (SELECT device.advertising_id AS user_aid
                FROM `funshare-8f5de.analytics_176305069.events_20190701`
                WHERE event_name = "app_remove"
                  AND FORMAT_TIMESTAMP("%Y%m%d",TIMESTAMP_MICROS(user_first_touch_timestamp),'+05:30') = event_date
                GROUP BY user_aid)
             AND event_name NOT IN ("app_remove",
                                    "user_engagement",
                                    "screen_view",
                                    "video_play_time",
                                    "session_start",
                                    'fresh_request',
                                    'fresh_response',
                                    'impression',
                                    'category_response',
                                    'firebase_campaign'))
        WHERE row_num = 1
          AND event_name='click_home_page' )
     AND event_name NOT IN ("app_remove",
                            "user_engagement",
                            "screen_view",
                            "video_play_time",
                            "session_start",
                            'fresh_request',
                            'fresh_response',
                            'impression',
                            'category_response',
                            'firebase_campaign',
                            'play_video',
                            'play_detail',
                            'video_loading',
                            'click_play_video',
                            'share_return',
                            'favourite',
                            'download_video',
                            'click_like',
                            'click_comment',
                            'follow',
                            'category_request'))
WHERE rn=2
GROUP BY event_name




issue#4856
nearby下selfie被分发的feed_兼职（分语言分日期）
SELECT selfie_feed.india_date india_date,
        selfie_feed.lang lang,
        COUNT(DISTINCT selfie_feed.id) new_feed_cnt,
        COUNT(DISTINCT imp.feed_seq_id) imp_feed_cnt
FROM
  (SELECT f.*
  FROM sharemax_dw.feed f
  WHERE logdate BETWEEN date_sub('2019-08-06',1) AND '2019-08-06'
    AND f.india_date='2019-08-06'
    AND f.tag_id IN ('5ae43c4b99f58a169c752e1f',
                  '5bed6c17dca0fd000cb98ba2',
                  '5b31f8316e72673fa5cc85e0',
                  '5b91122e6e726710ccc3cebf',
                  '5b6c10fa6e726716c19c2c5a')
    AND f.user_id IN (22132685,22026591,20423005,22132377,21968697,21901519,22098657,13388852,
                      16475973,16179809,13473964,15260148,15856891,15265578,15292164,15261635,
                      15309050,15309200,15856831,15856671,15856649,15856138,15322155,15664625,
                      13865889,15317954,14136876,15617024,15207110,15623306,15270850,15261036,
                      14080421,15441643,15272837,16131737,15410928,15352854,17054755,15430916,
                      15479975,15427589,13160598,14241602,15293635,15404133,15294888,14200233,
                      15338868,16436433,15404379,15431142,15091587,15239082,14870061,14993700,
                      14979991,15025815,14878712,15207208,14977041,15698540,15196763,13668157,
                      14623212,10110378,15450137,15450783,15450859,15451086,15453050,15482195,
                      15488697,15489051,15489156,15493636,15496480,15496961,15500727,15500856,
                      15501506,15502033,15502105,15502173,15502312,15502883,15503022,15503320,
                      15503352,15507269,15507469,15508713,15509692,15509809,15546820,16070261,
                      22132685,20423005,22132377,19608311,21968697,21901519,22098657,21438608,
                      15096385,22542566,22545276,22342316,22796728,22797939,22799002,15481643)
  ) selfie_feed
  LEFT JOIN
  (SELECT t.*
  FROM sharemax_dw.refresh t LATERAL VIEW explode(t.feed_ids) t AS feed_seq_id
  WHERE logdate BETWEEN date_sub('2019-08-06',1) AND '2019-08-06'
    AND to_date(from_unixtime(unix_timestamp(t.`time`)+19800))='2019-08-06'
    AND t.refresh_id IS NOT NULL
    AND t.source='nearby') imp
  ON selfie_feed.seq_id=imp.feed_seq_id
GROUP BY selfie_feed.india_date,
          selfie_feed.lang




issue#4854
【需求】创建关于音频的功能留存报表，便于追踪音频功能效果。
功能留存定义：当天有音频播放功能的用户种在第X天仍继续播放的用户行为数（X=1，3，7，15）
报表维度：分语言，分新老用户，分日期，以上维度都添加all选项


SELECT '2019-08-01' india_date,
        d0.lang lang,
        d0.user_type user_type,
        COUNT(DISTINCT d0.user_id) user_cnt,
        COUNT(DISTINCT d1.user_id) re_1_cnt,
        COUNT(DISTINCT d3.user_id) re_3_cnt,
        COUNT(DISTINCT d7.user_id) re_7_cnt
FROM
    (SELECT DISTINCT t.data['user_id'] user_id,
            u.current_lang lang,
            CASE WHEN to_date(from_unixtime(unix_timestamp(u.created_at)+19800))='2019-08-01' THEN 'new' ELSE 'old' END AS user_type
    FROM sharemax_dw.action_log t
    JOIN user_center.funshare_user_accounts u
    ON t.data['user_id']=u.user_id AND u.current_lang IN('hi','ta','te')
    WHERE t.logdate BETWEEN date_sub('2019-08-01',1) AND '2019-08-01'
      AND to_date(from_unixtime(unix_timestamp(regexp_replace(substr(t.`timestamp`,1,19),'T',' ')) + 19800))='2019-08-01'
      AND t.data['type']='play_audio') d0
    LEFT JOIN
    (SELECT DISTINCT t.data['user_id'] user_id
    FROM sharemax_dw.action_log t
    WHERE t.logdate BETWEEN date_add('2019-08-01',0) AND date_add('2019-08-01',1)
      AND to_date(from_unixtime(unix_timestamp(regexp_replace(substr(t.`timestamp`,1,19),'T',' ')) + 19800))=date_add('2019-08-01',1)
      AND t.data['type']='play_audio') d1
    ON d0.user_id=d1.user_id
    LEFT JOIN
    (SELECT DISTINCT t.data['user_id'] user_id
    FROM sharemax_dw.action_log t
    WHERE t.logdate BETWEEN date_add('2019-08-01',2) AND date_add('2019-08-01',3)
      AND to_date(from_unixtime(unix_timestamp(regexp_replace(substr(t.`timestamp`,1,19),'T',' ')) + 19800))=date_add('2019-08-01',3)
      AND t.data['type']='play_audio') d3
    ON d0.user_id=d3.user_id
    LEFT JOIN
    (SELECT DISTINCT t.data['user_id'] user_id
    FROM sharemax_dw.action_log t
    WHERE t.logdate BETWEEN date_add('2019-08-01',6) AND date_add('2019-08-01',7)
      AND to_date(from_unixtime(unix_timestamp(regexp_replace(substr(t.`timestamp`,1,19),'T',' ')) + 19800))=date_add('2019-08-01',7)
      AND t.data['type']='play_audio') d7
    ON d0.user_id=d7.user_id
GROUP BY d0.lang,
          d0.user_type



issue#4858
需要9月5到9月8日的
在togo聊天室每天送出礼物总价值大于100金币的用户
用户id，用户名，联系方式以及房间的displayid(如果没有的话标null)

SELECT gf.india_date,
		gf.user_id user_id,
        u.name user_name,
        u.phone phone,
        gf.reward_gift_cnt reward_gift_cnt,
        gf.reward_gift_coin reward_gift_coin,
        ci.display_id display_id
FROM
  (SELECT to_date(from_unixtime(unix_timestamp(t.`time`)+19800)) india_date,
  		  t.rewarder_id user_id,--送礼id
          SUM(t.gift_count*size(t.receiver)) reward_gift_cnt,
          SUM(t.gift_cost) reward_gift_coin
    FROM sharemax_dw.reward_log t
    WHERE t.logdate BETWEEN date_sub('2019-09-05',1) AND '2019-09-08'
      AND to_date(from_unixtime(unix_timestamp(t.`time`)+19800)) BETWEEN '2019-09-05' AND '2019-09-08'
      AND t.reward_info_id<>0
    GROUP BY to_date(from_unixtime(unix_timestamp(t.`time`)+19800)),
    		t.rewarder_id) gf
  JOIN user_center.funshare_user_accounts u ON gf.user_id=u.user_id
  LEFT JOIN sharemax_dw.chatroom_info ci ON u.user_id=ci.user_id
WHERE gf.reward_gift_coin>=100




issue#4888
8月1日到8月15日全部充值用户的信息
用户id，用户名，已充值金币数，联系方式

SELECT t.user_id user_id,
      u.name user_name,
      u.phone user_phone,
      SUM(t.coin) recharge_coin
  FROM sharemax_dw.transaction t JOIN user_center.funshare_user_accounts u ON t.user_id=u.user_id
  WHERE t.logdate BETWEEN date_sub('2019-08-01',1) AND '2019-08-15'
    AND to_date(from_unixtime(unix_timestamp(t.`time`)+19800)) BETWEEN '2019-08-01' AND '2019-08-15'
    AND t.type='recharge'
    AND t.status=1
  GROUP BY t.user_id,
            u.name,
            u.phone


审核下线feed的在线情况

SELECT avg(unix_timestamp(f.updated_at)-unix_timestamp(f.created_at))/3600 avg_time,
        max(unix_timestamp(f.updated_at)-unix_timestamp(f.created_at))/3600 max_time,
        min(unix_timestamp(f.updated_at)-unix_timestamp(f.created_at))/3600 min_time,
        percentile_approx((unix_timestamp(f.updated_at)-unix_timestamp(f.created_at)),0.5) median_time
FROM sharemax_dw.feed f JOIN tmp.feed_offline t ON f.seq_id=t.feed_id
WHERE f.logdate BETWEEN '2019-07-27' AND '2019-08-12'
AND f.india_date BETWEEN '2019-07-28' AND '2019-08-12'

审核下线feed的在线时长情况
SELECT CASE WHEN (unix_timestamp(f.updated_at)-unix_timestamp(f.created_at))/3600 <=1 THEN 1
            WHEN (unix_timestamp(f.updated_at)-unix_timestamp(f.created_at))/3600 <=2 THEN 2
            WHEN (unix_timestamp(f.updated_at)-unix_timestamp(f.created_at))/3600 <=3 THEN 3
            WHEN (unix_timestamp(f.updated_at)-unix_timestamp(f.created_at))/3600 <=4 THEN 4
            WHEN (unix_timestamp(f.updated_at)-unix_timestamp(f.created_at))/3600 <=5 THEN 5
            WHEN (unix_timestamp(f.updated_at)-unix_timestamp(f.created_at))/3600 <=6 THEN 6
            WHEN (unix_timestamp(f.updated_at)-unix_timestamp(f.created_at))/3600 <=7 THEN 7
            WHEN (unix_timestamp(f.updated_at)-unix_timestamp(f.created_at))/3600 <=8 THEN 8
            ELSE '8+' END AS online_time,
        COUNT(DISTINCT f.seq_id) feed_cnt
FROM sharemax_dw.feed f JOIN tmp.feed_offline t ON f.seq_id=t.feed_id
WHERE f.logdate BETWEEN '2019-07-27' AND '2019-08-12'
AND f.india_date BETWEEN '2019-07-28' AND '2019-08-12'
GROUP BY CASE WHEN (unix_timestamp(f.updated_at)-unix_timestamp(f.created_at))/3600 <=1 THEN 1
            WHEN (unix_timestamp(f.updated_at)-unix_timestamp(f.created_at))/3600 <=2 THEN 2
            WHEN (unix_timestamp(f.updated_at)-unix_timestamp(f.created_at))/3600 <=3 THEN 3
            WHEN (unix_timestamp(f.updated_at)-unix_timestamp(f.created_at))/3600 <=4 THEN 4
            WHEN (unix_timestamp(f.updated_at)-unix_timestamp(f.created_at))/3600 <=5 THEN 5
            WHEN (unix_timestamp(f.updated_at)-unix_timestamp(f.created_at))/3600 <=6 THEN 6
            WHEN (unix_timestamp(f.updated_at)-unix_timestamp(f.created_at))/3600 <=7 THEN 7
            WHEN (unix_timestamp(f.updated_at)-unix_timestamp(f.created_at))/3600 <=8 THEN 8
            ELSE '8+' END




审核下线帖子的展现、分享、下载、点赞、评论数据分布情况

SELECT t.feed_seq_id,
        COUNT(DISTINCT t.refresh_refresh_id) refresh_cnt,
        COUNT(DISTINCT CASE WHEN t.user_action_action_type='share' THEN t.user_action_action_id END) share_cnt,
        COUNT(DISTINCT CASE WHEN t.user_action_action_type IN('fav','save') THEN t.user_action_action_id END) fav_cnt,
        COUNT(DISTINCT CASE WHEN t.user_action_action_type ='like' THEN t.user_action_action_id END) like_cnt,
        COUNT(DISTINCT CASE WHEN t.user_action_action_type ='comment' THEN t.user_action_action_id END) comment_cnt
FROM dm.sharemax_feed_action_new t JOIN tmp.feed_offline tf ON t.feed_seq_id=tf.feed_id
WHERE t.logdate BETWEEN '2019-07-27' AND '2019-08-12'
AND t.feed_india_date BETWEEN '2019-07-28' AND '2019-08-12'
AND t.refresh_refresh_id IS NOT NULL
GROUP BY t.feed_seq_id



当天新feed中：被下线feed的表现 vs 当天正常展现的帖子 下载数据分布情况

SELECT b.is_online is_online,
      ceil(b.fav_rate*1000) fav_interval,
      COUNT(DISTINCT b.feed_seq_id) feed_cnt
FROM
  (SELECT  t.feed_india_date india_date,
          t.feed_seq_id feed_seq_id,
          CASE WHEN tf.feed_id IS NULL THEN 'online' ELSE 'offline' END is_online,
          COUNT(DISTINCT concat(t.feed_seq_id,t.refresh_refresh_id)) refresh_cnt,
          COUNT(DISTINCT CASE WHEN t.user_action_action_type IN('fav','save') THEN t.user_action_action_id END) fav_cnt,
          COUNT(DISTINCT CASE WHEN t.user_action_action_type IN('fav','save') THEN t.user_action_action_id END)/COUNT(DISTINCT concat(t.feed_seq_id,t.refresh_refresh_id)) fav_rate
  FROM dm.sharemax_feed_action_new t LEFT JOIN tmp.feed_offline tf ON t.feed_seq_id=tf.feed_id
  WHERE t.logdate BETWEEN date_sub('2019-07-28',1) AND '2019-08-12'
  AND t.feed_india_date BETWEEN '2019-07-28' AND '2019-08-12'
  AND t.refresh_refresh_id IS NOT NULL
  AND t.feed_india_date=to_date(from_unixtime(unix_timestamp(t.refresh_time)+19800))
  GROUP BY t.feed_india_date,
            t.feed_seq_id,
            CASE WHEN tf.feed_id IS NULL THEN 'online' ELSE 'offline' END) b
GROUP BY b.is_online,
          ceil(b.fav_rate*1000)



SELECT  t.feed_india_date india_date,
        t.feed_seq_id feed_seq_id,
        COUNT(DISTINCT concat(t.feed_seq_id,t.refresh_refresh_id)) refresh_cnt,
        COUNT(DISTINCT CASE WHEN t.user_action_action_type IN('fav','save') THEN t.user_action_action_id END) fav_cnt,
        COUNT(DISTINCT CASE WHEN t.user_action_action_type IN('fav','save') THEN t.user_action_action_id END)/COUNT(DISTINCT concat(t.feed_seq_id,t.refresh_refresh_id)) fav_rate
FROM dm.sharemax_feed_action_new t LEFT JOIN tmp.feed_offline tf ON t.feed_seq_id=tf.feed_id
WHERE t.logdate BETWEEN date_sub('2019-07-28',1) AND '2019-08-12'
AND t.feed_india_date BETWEEN '2019-07-28' AND '2019-08-12'
AND t.refresh_refresh_id IS NOT NULL
AND t.feed_india_date=to_date(from_unixtime(unix_timestamp(t.refresh_time)+19800))
AND tf.feed_id IS NULL
GROUP BY t.feed_india_date,
          t.feed_seq_id,
          CASE WHEN tf.feed_id IS NULL THEN 'online' ELSE 'offline' END
ORDER BY COUNT(DISTINCT CASE WHEN t.user_action_action_type IN('fav','save') THEN t.user_action_action_id END)/COUNT(DISTINCT concat(t.feed_seq_id,t.refresh_refresh_id)) DESC
LIMIT 1000




当天新feed中：被下线feed的表现 vs 当天正常展现的帖子 点赞数据分布情况

SELECT b.is_online is_online,
      ceil(b.like_rate*1000) like_interval,
      COUNT(DISTINCT b.feed_seq_id) feed_cnt
FROM
  (SELECT  t.feed_india_date india_date,
          t.feed_seq_id feed_seq_id,
          CASE WHEN tf.feed_id IS NULL THEN 'online' ELSE 'offline' END is_online,
          COUNT(DISTINCT concat(t.feed_seq_id,t.refresh_refresh_id)) refresh_cnt,
          COUNT(DISTINCT CASE WHEN t.user_action_action_type ='like' THEN t.user_action_action_id END) like_cnt,
          COUNT(DISTINCT CASE WHEN t.user_action_action_type ='like' THEN t.user_action_action_id END)/COUNT(DISTINCT concat(t.feed_seq_id,t.refresh_refresh_id)) like_rate
  FROM dm.sharemax_feed_action_new t LEFT JOIN tmp.feed_offline tf ON t.feed_seq_id=tf.feed_id
  WHERE t.logdate BETWEEN date_sub('2019-07-28',1) AND '2019-08-12'
  AND t.feed_india_date BETWEEN '2019-07-28' AND '2019-08-12'
  AND t.refresh_refresh_id IS NOT NULL
  AND t.feed_india_date=to_date(from_unixtime(unix_timestamp(t.refresh_time)+19800))
  GROUP BY t.feed_india_date,
            t.feed_seq_id,
            CASE WHEN tf.feed_id IS NULL THEN 'online' ELSE 'offline' END) b
GROUP BY b.is_online,
          ceil(b.like_rate*1000)

online和offline各取1000
SELECT b.india_date,
        b.feed_seq_id,
        b.is_online,
        b.refresh_cnt,
        b.like_cnt,
        b.fav_cnt,
        b.share_cnt,
        b.like_rate,
        b.fav_rate,
        b.share_rate
FROM
  (SELECT  t.feed_india_date india_date,
      t.feed_seq_id feed_seq_id,
      CASE WHEN tf.feed_id IS NULL THEN 'online' ELSE 'offline' END is_online,
      row_number() over(PARTITION BY CASE WHEN tf.feed_id IS NULL THEN 'online' ELSE 'offline' END ORDER BY t.feed_seq_id) rn,
      COUNT(DISTINCT concat(t.feed_seq_id,t.refresh_refresh_id)) refresh_cnt,
      COUNT(DISTINCT CASE WHEN t.user_action_action_type ='like' THEN t.user_action_action_id END) like_cnt,
      COUNT(DISTINCT CASE WHEN t.user_action_action_type IN('fav','save') THEN t.user_action_action_id END) fav_cnt,
      COUNT(DISTINCT CASE WHEN t.user_action_action_type ='share' THEN t.user_action_action_id END) share_cnt,
      COUNT(DISTINCT CASE WHEN t.user_action_action_type ='like' THEN t.user_action_action_id END)/COUNT(DISTINCT concat(t.feed_seq_id,t.refresh_refresh_id)) like_rate, --点赞率
      COUNT(DISTINCT CASE WHEN t.user_action_action_type IN('fav','save') THEN t.user_action_action_id END)/COUNT(DISTINCT concat(t.feed_seq_id,t.refresh_refresh_id)) fav_rate, --分享率
      COUNT(DISTINCT CASE WHEN t.user_action_action_type ='share' THEN t.user_action_action_id END)/COUNT(DISTINCT concat(t.feed_seq_id,t.refresh_refresh_id)) share_rate
  FROM dm.sharemax_feed_action_new t LEFT JOIN tmp.feed_offline tf ON t.feed_seq_id=tf.feed_id
  WHERE t.logdate BETWEEN date_sub('2019-07-28',1) AND '2019-08-12'
  AND t.feed_india_date BETWEEN '2019-07-28' AND '2019-08-12'
  AND t.refresh_refresh_id IS NOT NULL
  AND t.feed_india_date=to_date(from_unixtime(unix_timestamp(t.refresh_time)+19800))
  GROUP BY t.feed_india_date,
            t.feed_seq_id,
            CASE WHEN tf.feed_id IS NULL THEN 'online' ELSE 'offline' END) b
WHERE b.rn<=1000


SELECT CASE WHEN b.fav_rate>=0.02 THEN 'high' ELSE 'low' END AS fav_rate_type,
      COUNT(DISTINCT b.feed_seq_id) feed_cnt,
      SUM(b.refresh_cnt) total_refresh_cnt,
      SUM(b.fav_cnt) total_fav_cnt
FROM
  (SELECT  t.feed_india_date india_date,
          t.feed_seq_id feed_seq_id,
          COUNT(DISTINCT concat(t.feed_seq_id,t.refresh_refresh_id)) refresh_cnt,
          COUNT(DISTINCT CASE WHEN t.user_action_action_type IN('fav','save') THEN t.user_action_action_id END) fav_cnt,
          COUNT(DISTINCT CASE WHEN t.user_action_action_type IN('fav','save') THEN t.user_action_action_id END)/COUNT(DISTINCT concat(t.feed_seq_id,t.refresh_refresh_id)) fav_rate
  FROM dm.sharemax_feed_action_new t JOIN tmp.feed_offline tf ON t.feed_seq_id=tf.feed_id
  WHERE t.logdate BETWEEN date_sub('2019-07-28',1) AND '2019-08-12'
  AND t.feed_india_date BETWEEN '2019-07-28' AND '2019-08-12'
  AND t.refresh_refresh_id IS NOT NULL
  AND t.feed_india_date=to_date(from_unixtime(unix_timestamp(t.refresh_time)+19800))
  GROUP BY t.feed_india_date,
            t.feed_seq_id,
            CASE WHEN tf.feed_id IS NULL THEN 'online' ELSE 'offline' END) b
GROUP BY CASE WHEN b.fav_rate>=0.02 THEN 'high' ELSE 'low' END

当天新feed中：被下线feed的表现 vs 当天正常展现的帖子在当天的表现

SELECT  t.feed_india_date india_date,
        CASE WHEN tf.feed_id IS NULL THEN 'online' ELSE 'offline' END is_online,
        COUNT(DISTINCT concat(t.feed_seq_id,t.refresh_refresh_id)) refresh_cnt,
        COUNT(DISTINCT CASE WHEN t.user_action_action_type='share' THEN t.user_action_action_id END) share_cnt,
        COUNT(DISTINCT CASE WHEN t.user_action_action_type IN('fav','save') THEN t.user_action_action_id END) fav_cnt,
        COUNT(DISTINCT CASE WHEN t.user_action_action_type ='like' THEN t.user_action_action_id END) like_cnt,
        COUNT(DISTINCT CASE WHEN t.user_action_action_type ='comment' THEN t.user_action_action_id END) comment_cnt
FROM dm.sharemax_feed_action_new t LEFT JOIN tmp.feed_offline tf ON t.feed_seq_id=tf.feed_id
WHERE t.logdate BETWEEN date_sub('2019-07-28',1) AND '2019-08-12'
AND t.feed_india_date BETWEEN '2019-07-28' AND '2019-08-12'
AND t.refresh_refresh_id IS NOT NULL
AND t.feed_india_date=to_date(from_unixtime(unix_timestamp(t.refresh_time)+19800))
GROUP BY t.feed_india_date,
          CASE WHEN tf.feed_id IS NULL THEN 'online' ELSE 'offline' END


聊天室掷骰子事件
WITH tmp AS
(
SELECT t.*,
    to_date(from_unixtime(unix_timestamp(regexp_replace(substr(t.`timestamp`,1,19),'T',' ')) + 19800)) india_date
FROM sharemax_dw.action_log t
WHERE t.logdate BETWEEN date_sub('2019-08-12',1) AND '2019-08-12'
  AND to_date(from_unixtime(unix_timestamp(regexp_replace(substr(t.`timestamp`,1,19),'T',' ')) + 19800)) BETWEEN '2019-08-12' AND '2019-08-12'
  AND t.data['type'] IN('send_lucky_number')
)

SELECT t.india_date india_date,
    CASE WHEN to_date(from_unixtime(unix_timestamp(u.created_at)+19800))=t.india_date THEN 'new' ELSE 'old' END AS user_type,
    COUNT(DISTINCT CASE WHEN t.data['type']='send_lucky_number' THEN t.data['user_id'] END) send_lucky_number_user_cnt,
    COUNT(CASE WHEN t.data['type']='send_lucky_number' THEN 1 END) send_lucky_number_cnt,
    COUNT(CASE WHEN t.data['type']='send_lucky_number' AND t.data['is_success']=1 THEN 1 END) send_success_cnt,
    COUNT(CASE WHEN t.data['type']='send_lucky_number' AND t.data['is_success']=0 THEN 1 END) send_fail_cnt
FROM tmp t
JOIN user_center.funshare_user_accounts u
ON t.data['user_id']=u.user_id
GROUP BY t.india_date,
    CASE WHEN to_date(from_unixtime(unix_timestamp(u.created_at)+19800))=t.india_date THEN 'new' ELSE 'old' END



issue#4872
查看这两个用户的20190812进聊天室的行为，用户id 为 30043216，19673301
在每个小时进去过的room id，room name，当时的host id，及他在此房间停留的时长，
他在此聊天室举报的用户user id

togo：

SELECT getin.india_date,
       getin.time_hour,
       getin.time_minute,
       getin.user_id,
       getin.room_id,
       getin.display_id,
       getin.room_name,
       getin.host_id,
       dur.duration,
       report.reported_users
FROM
  (SELECT DISTINCT
  		 to_date(from_unixtime(unix_timestamp(t.`time`)+19800)) india_date,
         hour(from_unixtime(unix_timestamp(t.`time`)+19800)) time_hour,
         minute(from_unixtime(unix_timestamp(t.`time`)+19800)) time_minute,
          t.sender user_id,
          t.room_id room_id,
          ci.display_id display_id,
          ci.name room_name,
          t.hostid host_id
  FROM sharemax_dw.chatroom_user_action t JOIN sharemax_dw.chatroom_info ci ON t.room_id=ci.id
  WHERE t.logdate BETWEEN date_sub('2019-08-12',1) AND '2019-08-12'
    AND to_date(from_unixtime(unix_timestamp(t.`time`)+19800))='2019-08-12'
    AND t.method='new_user'
    AND t.sender IN(30043216,19673301)) getin
  LEFT JOIN
  (SELECT to_date(from_unixtime(unix_timestamp(t.`time`)+19800)) india_date,
         t.uid user_id,
         t.rid room_id,
         SUM(t.closed_at-t.created_at)/60 duration--单位：分钟
  FROM sharemax_dw.chatroom_connection t
  WHERE t.logdate BETWEEN date_sub('2019-08-12',1) AND '2019-08-12'
    AND to_date(from_unixtime(unix_timestamp(t.`time`)+19800))='2019-08-12'
    AND t.uid IN(30043216,19673301)
  GROUP BY to_date(from_unixtime(unix_timestamp(t.`time`)+19800)),
           t.uid,
           t.rid) dur
  ON getin.india_date=dur.india_date AND getin.user_id=dur.user_id AND getin.room_id=dur.room_id
  LEFT JOIN
  (SELECT to_date(from_unixtime(unix_timestamp(t.created_at)+19800)) india_date,
          t.reported_by user_id,
          t.room_id room_id,
          collect_set(t.user_id) reported_users --collect_list()
  FROM sharemax_dw.chatroom_user_report t
  WHERE to_date(from_unixtime(unix_timestamp(t.created_at)+19800))='2019-08-12'
    AND t.reported_by IN(30043216,19673301)
    AND t.app='funshare'
  GROUP BY to_date(from_unixtime(unix_timestamp(t.created_at)+19800)),
            t.reported_by,
            t.room_id) report
  ON getin.india_date=report.india_date AND getin.user_id=report.user_id AND getin.room_id=report.room_id

wangcai：

SELECT getin.middle_east_date middle_east_date,
       getin.time_hour time_hour,
       getin.time_minute time_minute,
       getin.user_id user_id,
       getin.room_id room_id,
       getin.display_id display_id,
       getin.room_name room_name,
       getin.host_id host_id,
       dur.duration duration,
       report.reported_users reported_users
FROM
  (SELECT DISTINCT
  		  t.logdate middle_east_date,
          hour(from_unixtime(unix_timestamp(t.`time`)+10800)) time_hour,
          minute(from_unixtime(unix_timestamp(t.`time`)+10800)) time_minute,
          t.sender user_id,
          t.room_id room_id,
          ci.display_id display_id,
          ci.name room_name,
          t.hostid host_id
  FROM wangcai_dw.wangcai_chatroom_user_action t JOIN wangcai_dw.wangcai_chatroom_info ci ON t.room_id=ci.id
  WHERE t.logdate BETWEEN '2019-10-10' AND '2019-10-10'
    AND t.method='new_user'
    AND t.sender IN(23406914)) getin
LEFT JOIN
  (SELECT t.logdate middle_east_date,
         t.uid user_id,
         t.rid room_id,
         SUM(t.closed_at-t.created_at)/60 duration--单位：分钟
  FROM wangcai_dw.wangcai_chatroom_connection t
  WHERE t.logdate BETWEEN '2019-10-10' AND '2019-10-10'
    AND t.uid IN(23406914)
  GROUP BY t.logdate,
           t.uid,
           t.rid) dur
ON getin.middle_east_date=dur.middle_east_date AND getin.user_id=dur.user_id AND getin.room_id=dur.room_id
LEFT JOIN
  (SELECT to_date(from_unixtime(unix_timestamp(t.created_at)+10800)) middle_east_date,
          t.reported_by user_id,
          t.room_id room_id,
          collect_set(t.user_id) reported_users
  FROM chatroom.chatroom_user_report t
  WHERE to_date(from_unixtime(unix_timestamp(t.created_at)+10800))='2019-10-10'
    AND t.reported_by IN(23406914)
    AND t.app='wangcai'
  GROUP BY to_date(from_unixtime(unix_timestamp(t.created_at)+10800)),
            t.reported_by,
            t.room_id) report
ON getin.middle_east_date=report.middle_east_date AND getin.user_id=report.user_id AND getin.room_id=report.room_id


issue#4877
用户姓名和头像url
SELECT user_id,
      name,
      get_json_object(avatar,"$.origin") profile_url
FROM user_center.funshare_user_accounts
WHERE user_id IN(19786226,19750639,21643591,22810302,19748451,23126731)




SELECT
  CASE WHEN to_date(from_unixtime(unix_timestamp(user_accounts_created_at)+19800)) = '{dat}' THEN 'new' ELSE 'old' END user_type,
  coalesce(refresh_lang,user_action_lang,'no_lang') lang,
  refresh_source channel,
  count(DISTINCT CASE WHEN to_date(from_unixtime(unix_timestamp(refresh_time)+19800)) = '{dat}' THEN refresh_refresh_id END) refresh_cnt,
  count(DISTINCT CASE WHEN to_date(from_unixtime(unix_timestamp(refresh_time)+19800)) = '{dat}' THEN concat(user_action_refresh_feed_id,refresh_refresh_id) END) refresh_feed_cnt,
  count(DISTINCT CASE WHEN to_date(from_unixtime(unix_timestamp(refresh_time)+19800)) = '{dat}' THEN user_accounts_user_id END) refresh_user_cnt
  FROM dm.sharemax_user_action_account_new
  left join sharemax_dw.feed
    on feed.seq_id = sharemax_user_action_account_new.user_action_refresh_feed_id
  WHERE user_accounts_account_type NOT IN ('fake', 'staff')
    AND sharemax_user_action_account_new.logdate BETWEEN date_add('{dat}',-1) AND '{dat}'
    AND refresh_refresh_id IS NOT NULL
  GROUP BY
  coalesce(refresh_lang,user_action_lang,'no_lang'),
  CASE WHEN to_date(from_unixtime(unix_timestamp(user_accounts_created_at)+19800)) = '{dat}' THEN 'new' ELSE 'old' END,
  refresh_source



SELECT to_date(from_unixtime(unix_timestamp(regexp_replace(substr(t.`timestamp`,1,19),'T',' ')) + 19800)) india_date,
     CASE WHEN to_date(from_unixtime(unix_timestamp(u.created_at)+19800))=to_date(from_unixtime(unix_timestamp(regexp_replace(substr(t.`timestamp`,1,19),'T',' ')) + 19800)) THEN 'new' ELSE 'old' END AS user_type,
     CASE WHEN t.data['pos'] IS NULL THEN 'no_pos' ELSE t.data['pos'] END AS getin_pos,
     COUNT(DISTINCT t.data['user_id']) getin_user_cnt,
     COUNT(1) getin_cnt
FROM sharemax_dw.action_log t JOIN user_center.funshare_user_accounts u ON t.data['user_id']=u.user_id
WHERE t.logdate BETWEEN date_sub('2019-08-05',1) AND '2019-08-15'
  AND to_date(from_unixtime(unix_timestamp(regexp_replace(substr(t.`timestamp`,1,19),'T',' ')) + 19800)) BETWEEN '2019-08-05' AND '2019-08-15'
  AND t.data['type'] IN('getin_chatroom_success')
  AND (t.data['pos'] IS NULL OR t.data['pos']='hot_room')
GROUP BY to_date(from_unixtime(unix_timestamp(regexp_replace(substr(t.`timestamp`,1,19),'T',' ')) + 19800)),
        CASE WHEN to_date(from_unixtime(unix_timestamp(u.created_at)+19800))=to_date(from_unixtime(unix_timestamp(regexp_replace(substr(t.`timestamp`,1,19),'T',' ')) + 19800)) THEN 'new' ELSE 'old' END,
        CASE WHEN t.data['pos'] IS NULL THEN 'no_pos' ELSE t.data['pos'] END



SELECT to_date(from_unixtime(unix_timestamp(regexp_replace(substr(t.`timestamp`,1,19),'T',' ')) + 19800)) india_date,
     CASE WHEN to_date(from_unixtime(unix_timestamp(u.created_at)+19800))=to_date(from_unixtime(unix_timestamp(regexp_replace(substr(t.`timestamp`,1,19),'T',' ')) + 19800)) THEN 'new' ELSE 'old' END AS user_type,
     COUNT(DISTINCT t.data['user_id']) getin_user_cnt,
     COUNT(1) getin_cnt
FROM sharemax_dw.action_log t JOIN user_center.funshare_user_accounts u ON t.data['user_id']=u.user_id
WHERE t.logdate BETWEEN date_sub('2019-08-05',1) AND '2019-08-15'
  AND to_date(from_unixtime(unix_timestamp(regexp_replace(substr(t.`timestamp`,1,19),'T',' ')) + 19800)) BETWEEN '2019-08-05' AND '2019-08-15'
  AND t.data['type'] IN('getin_chatroom_success')
  AND t.app_v='1.4.2'
GROUP BY to_date(from_unixtime(unix_timestamp(regexp_replace(substr(t.`timestamp`,1,19),'T',' ')) + 19800)),
        CASE WHEN to_date(from_unixtime(unix_timestamp(u.created_at)+19800))=to_date(from_unixtime(unix_timestamp(regexp_replace(substr(t.`timestamp`,1,19),'T',' ')) + 19800)) THEN 'new' ELSE 'old' END




issue#4897
希望能够通过房间的display id
跑出来 该房间的 房主id，房间名，封面图链接，房间长id

SELECT display_id,
        id room_id,
        user_id owner_id,
        name room_name,
        url
FROM sharemax_dw.chatroom_info
WHERE display_id IN (971828)


issue#4899
如题,
拆新老用户, 1.4.2 版本进入房间人数/次数,
以及其中通过点击trending推荐房间进入的人数/次数


SELECT to_date(from_unixtime(unix_timestamp(regexp_replace(substr(t.`timestamp`,1,19),'T',' ')) + 19800)) india_date,
     CASE WHEN to_date(from_unixtime(unix_timestamp(u.created_at)+19800))=to_date(from_unixtime(unix_timestamp(regexp_replace(substr(t.`timestamp`,1,19),'T',' ')) + 19800)) THEN 'new' ELSE 'old' END AS user_type,
     COUNT(DISTINCT t.data['user_id']) getin_user_cnt,
     COUNT(1) getin_cnt,
     COUNT(CASE WHEN t.data['pos']='hot_room' THEN t.data['user_id'] END) trending_getin_user_cnt,
     COUNT(CASE WHEN t.data['pos']='hot_room' THEN 1 END) trending_getin_cnt
FROM sharemax_dw.action_log t JOIN user_center.funshare_user_accounts u ON t.data['user_id']=u.user_id
WHERE t.logdate BETWEEN date_sub('2019-08-15',1) AND '2019-08-15'
  AND to_date(from_unixtime(unix_timestamp(regexp_replace(substr(t.`timestamp`,1,19),'T',' ')) + 19800)) BETWEEN '2019-08-15' AND '2019-08-15'
  AND t.data['type'] IN('getin_chatroom_success')
  AND t.app_v='1.4.2'
GROUP BY to_date(from_unixtime(unix_timestamp(regexp_replace(substr(t.`timestamp`,1,19),'T',' ')) + 19800)),
        CASE WHEN to_date(from_unixtime(unix_timestamp(u.created_at)+19800))=to_date(from_unixtime(unix_timestamp(regexp_replace(substr(t.`timestamp`,1,19),'T',' ')) + 19800)) THEN 'new' ELSE 'old' END





SELECT a.india_date india_date,
       a.lang lang,
       a.tag_id tag_id,
       t.title tag_title,
       t.en_title tag_title_en,
       a.impression_cnt impression_cnt,
       a.feed_cnt feed_cnt,
       a.share_cnt share_cnt,
       a.like_cnt like_cnt,
       a.fav_cnt fav_cnt,
       a.comment_cnt comment_cnt,
       b.play_audio_cnt play_audio_cnt
from
(SELECT to_date(from_unixtime(unix_timestamp(t.refresh_time)+19800)) india_date,
      t.feed_lang lang,
      t.feed_tag_id tag_id,
      count(distinct concat(t.user_action_refresh_feed_id,t.refresh_refresh_id)) impression_cnt,
      count(distinct feed_seq_id) feed_cnt,
      count(distinct CASE WHEN t.user_action_action_type='share' THEN t.user_action_action_id ELSE NULL END) share_cnt,
      count(distinct CASE WHEN t.user_action_action_type='like' THEN t.user_action_action_id ELSE NULL END) like_cnt,
      count(distinct CASE WHEN t.user_action_action_type IN('fav','save') THEN t.user_action_action_id ELSE NULL END) fav_cnt,
      count(distinct CASE WHEN t.user_action_action_type='comment' THEN t.user_action_action_id ELSE NULL END) comment_cnt
FROM dm.sharemax_feed_action_new t
WHERE t.logdate BETWEEN date_sub('2019-08-10',1) AND '2019-08-16'
  and to_date(from_unixtime(unix_timestamp(t.refresh_time)+19800)) BETWEEN '2019-08-10' AND '2019-08-16'
  and t.feed_india_date >='2019-06-01'
  and t.feed_feed_type='audio'
  and t.refresh_refresh_id IS NOT NULL
GROUP BY to_date(from_unixtime(unix_timestamp(t.refresh_time)+19800)),
         t.feed_lang,
         t.feed_tag_id)a
JOIN
(SELECT to_date(from_unixtime(unix_timestamp(regexp_replace(substr(f4.`timestamp`,1,19),'T',' ')) + 19800)) india_date,
       f.tag_id,
       count(f4.data['feed_id']) play_audio_cnt
  from sharemax_dw.feed f
   JOIN sharemax_dw.action_log f4
  ON f4.logdate BETWEEN date_sub('2019-08-10',1) AND '2019-08-16'
  and to_date(from_unixtime(unix_timestamp(regexp_replace(substr(f4.`timestamp`,1,19),'T',' ')) + 19800)) BETWEEN '2019-08-10' AND '2019-08-16'
  AND f4.data['type']='play_audio'
  AND f.id=f4.data['feed_id']
where f.logdate>='2019-06-01'
  AND f.feed_type='audio'
GROUP by to_date(from_unixtime(unix_timestamp(regexp_replace(substr(f4.`timestamp`,1,19),'T',' ')) + 19800)),
         f.tag_id)b
on a.india_date = b.india_date
and a.tag_id = b.tag_id
left JOIN sharemax_dw.tag t
on a.tag_id = t.id



WITH tmp AS
(
SELECT t.*,
    to_date(from_unixtime(unix_timestamp(regexp_replace(substr(t.`timestamp`,1,19),'T',' ')) + 19800)) india_date
FROM sharemax_dw.action_log t
WHERE t.logdate BETWEEN date_sub('2019-08-16',1) AND '2019-08-16'
  AND to_date(from_unixtime(unix_timestamp(regexp_replace(substr(t.`timestamp`,1,19),'T',' ')) + 19800)) BETWEEN '2019-08-16' AND '2019-08-16'
  AND t.data['type'] IN('click_chatroom_tab','click_chatroom','click_trending_recommend_chatroom','push_click','getin_chatroom_success')
)

SELECT t.india_date india_date,
    CASE WHEN to_date(from_unixtime(unix_timestamp(u.created_at)+19800))=t.india_date THEN 'new' ELSE 'old' END AS uer_type,
    COUNT(DISTINCT CASE WHEN t.data['type']='click_chatroom_tab' THEN t.data['user_id'] END) click_tab_user_cnt,
    COUNT(CASE WHEN t.data['type']='click_chatroom_tab' THEN 1 END) click_chatroom_tab,
    COUNT(DISTINCT CASE WHEN t.data['type']='click_chatroom' THEN t.data['user_id'] END) click_chatroom_user_cnt,
    COUNT(CASE WHEN t.data['type']='click_chatroom' THEN 1 END) click_chatroom,
    COUNT(DISTINCT CASE WHEN t.data['type']='click_trending_recommend_chatroom' THEN t.data['user_id'] END) click_trending_room_user_cnt,
      COUNT(CASE WHEN t.data['type']='click_trending_recommend_chatroom' THEN 1 END) click_trending_room_cnt,
    COUNT(DISTINCT CASE WHEN t.data['type']='getin_chatroom_success' THEN t.data['user_id'] END) getin_user_cnt,
    COUNT(CASE WHEN t.data['type']='getin_chatroom_success' THEN 1 END) getin_chatroom_success
FROM tmp t
JOIN user_center.funshare_user_accounts u
ON t.data['user_id']=u.user_id
GROUP BY t.india_date,
    CASE WHEN to_date(from_unixtime(unix_timestamp(u.created_at)+19800))=t.india_date THEN 'new' ELSE 'old' END


issue#4862
需求补充：
统计维度和统计对象不变，补充以下统计字段：
添加字段：feed点击播放量，点击播放feed数

select a.logdate logdate,
       f.lang lang,
       case when f.audio_bg_img_url <>'' then 1 else 0 end is_img,
       count(DISTINCT CASE WHEN a.event_name='impression' THEN f.id END) impression_feed_cnt,
       count(DISTINCT CASE WHEN a.event_name='play_audio' THEN f.id END) play_feed_cnt,
       count(case when a.event_name='impression' then 1 end) impression_cnt,
       count(case when a.event_name='play_audio' then 1 end) play_cnt,
       count(DISTINCT case when a.event_name='play_audio' and a.action='click' then f.id end) click_play_feed_cnt,
       count(CASE WHEN a.event_name='play_audio' AND a.action='click' THEN 1 END) click_play_cnt
 from sharemax_dw.feed f
 join
 (SELECT a.logdate,
       a.event_name,
       a.feed_id,
       a.event_param.value.string_value action
 FROM
   (SELECT a.logdate,
          a.event_name,
          a.event_param.value.string_value feed_id,
          a.event_params
   from sharemax_dw.funshare_firebase a LATERAL VIEW explode(a.event_params) a as event_param
   where a.logdate between '2019-08-07' and '2019-08-16'
     and a.event_name in('impression','play_audio')
     and a.event_param.key='feed_id') a LATERAL VIEW explode(a.event_params) a as event_param
 WHERE a.event_param.key='action') a
on f.id = a.feed_id
where f.logdate > '2019-08-06'
  and f.feed_type='audio'
group by a.logdate,
         f.lang,
         case when f.audio_bg_img_url <>'' then 1 else 0 end


issue#4921
需要数据：
8.12-8.18，yoyo selfie challenge tag每日的刷新量，被刷新到的feed的分享量，下载量，点赞量，评论量
8.3-8.9，selfie tag每日的刷新量，被刷新到的feed的分享量，下载量，点赞量，评论量
tag id:
yoyo selfie challenge:
5d4000cdb5bdde000bba0289,5d40016711c9b7000bfafc29,5d415b3c11c9b7000bfafc42
selfie:
5ae43c4b99f58a169c752e1f,5b31f8316e72673fa5cc85e0,5b6c10fa6e726716c19c2c5a

SELECT to_date(from_unixtime(unix_timestamp(t.refresh_time)+19800)) india_date,
        t.feed_tag_id tag_id,
        COUNT(DISTINCT t.refresh_refresh_id) refresh_cnt,
        COUNT(DISTINCT t.feed_id) refresh_feed_cnt,
        COUNT(DISTINCT CASE WHEN t.user_action_action_type='share' THEN t.user_action_action_id END) share_cnt,
        COUNT(DISTINCT CASE WHEN t.user_action_action_type IN('fav','save') THEN t.user_action_action_id END) fav_cnt,
        COUNT(DISTINCT CASE WHEN t.user_action_action_type='like' THEN t.user_action_action_id END) like_cnt,
        COUNT(DISTINCT CASE WHEN t.user_action_action_type='comment' THEN t.user_action_action_id END) comment_cnt
FROM dm.sharemax_feed_action_new t
WHERE t.logdate BETWEEN date_sub('2019-08-03',1) AND '2019-08-09'
  AND to_date(from_unixtime(unix_timestamp(t.refresh_time)+19800)) BETWEEN '2019-08-03' AND '2019-08-09'
  AND t.refresh_refresh_id IS NOT NULL
  AND t.feed_tag_id IN('5ae43c4b99f58a169c752e1f','5b31f8316e72673fa5cc85e0','5b6c10fa6e726716c19c2c5a')
GROUP BY to_date(from_unixtime(unix_timestamp(t.refresh_time)+19800)),
          t.feed_tag_id

SELECT to_date(from_unixtime(unix_timestamp(t.refresh_time)+19800)) india_date,
        t.feed_tag_id tag_id,
        COUNT(DISTINCT t.refresh_refresh_id) refresh_cnt,
        COUNT(DISTINCT t.feed_id) refresh_feed_cnt,
        COUNT(DISTINCT CASE WHEN t.user_action_action_type='share' THEN t.user_action_action_id END) share_cnt,
        COUNT(DISTINCT CASE WHEN t.user_action_action_type IN('fav','save') THEN t.user_action_action_id END) fav_cnt,
        COUNT(DISTINCT CASE WHEN t.user_action_action_type='like' THEN t.user_action_action_id END) like_cnt,
        COUNT(DISTINCT CASE WHEN t.user_action_action_type='comment' THEN t.user_action_action_id END) comment_cnt
FROM dm.sharemax_feed_action_new t
WHERE t.logdate BETWEEN date_sub('2019-08-12',1) AND '2019-08-18'
  AND to_date(from_unixtime(unix_timestamp(t.refresh_time)+19800)) BETWEEN '2019-08-12' AND '2019-08-18'
  AND t.refresh_refresh_id IS NOT NULL
  AND t.feed_tag_id IN('5d4000cdb5bdde000bba0289','5d40016711c9b7000bfafc29','5d415b3c11c9b7000bfafc42')
GROUP BY to_date(from_unixtime(unix_timestamp(t.refresh_time)+19800)),
          t.feed_tag_id

issue#4922
最近7天充值成功的用户, 每笔充值选择的金额明细
需要的字段有:
充值日期
用户id
用户名
充值coin数
充值金额(卢比)

SELECT to_date(from_unixtime(unix_timestamp(t.`time`)+19800)) india_date,
       t.user_id user_id,
       u.name name,
       t.coin recharge_coin,
       t.price recharge_price
FROM sharemax_dw.transaction t JOIN user_center.funshare_user_accounts u ON t.user_id=u.user_id
WHERE t.logdate BETWEEN date_sub('2019-08-20',7) AND '2019-08-20'
  AND to_date(from_unixtime(unix_timestamp(t.`time`)+19800)) BETWEEN date_sub('2019-08-20',6) AND '2019-08-20'
  AND t.type='recharge'
  AND t.status=1

issue#4871
分新老用户
从不同入口进房间的用户数
日期：最近7天

从trending hot_room进房间的用户数：hot_room
从chatroom推送进房间的用户数————无
从房间分享链接进房间的用户数————无
从房间列表页进房间的用户数(点击party之后, 所有列表页, all-popular, all-new, all-rankings(无), related-recently, related-following)
从房间id搜索进入房间的用户数 search

全部进房间的用户数(应该是以上各项去重?)

%E0%A4%A8%E0%A4%AF%E0%A4%BE
नया
new
6023

%E0%A4%AB%E0%A5%89%E0%A4%B2%E0%A5%8B%E0%A4%87%E0%A4%82%E0%A4%97
फॉलोइंग
following
2094

%E0%A4%B2%E0%A5%8B%E0%A4%95%E0%A4%AA%E0%A5%8D%E0%A4%B0%E0%A4%BF%E0%A4%AF
लोकप्रिय
popular
36893

%E0%A4%B9%E0%A4%BE%E0%A4%B2+%E0%A4%B9%E0%A5%80+%E0%A4%AE%E0%A5%87%E0%A4%82
हाल ही में
recently
9481

SELECT getin.india_date india_date,
       getin.user_type user_type,
       COALESCE(getin.getin_pos,'all') getin_pos,
       getin.getin_user_cnt,
       dau.dau_cnt
FROM
  (SELECT to_date(from_unixtime(unix_timestamp(regexp_replace(substr(t.`timestamp`,1,19),'T',' ')) + 19800)) india_date,
       CASE WHEN to_date(from_unixtime(unix_timestamp(u.created_at)+19800))=to_date(from_unixtime(unix_timestamp(regexp_replace(substr(t.`timestamp`,1,19),'T',' ')) + 19800)) THEN 'new' ELSE 'old' END AS user_type,
       CASE WHEN t.data['pos']='%E0%A4%A8%E0%A4%AF%E0%A4%BE' THEN 'new'
            WHEN t.data['pos']='%E0%A4%AB%E0%A5%89%E0%A4%B2%E0%A5%8B%E0%A4%87%E0%A4%82%E0%A4%97' THEN 'following'
            WHEN t.data['pos']='%E0%A4%B2%E0%A5%8B%E0%A4%95%E0%A4%AA%E0%A5%8D%E0%A4%B0%E0%A4%BF%E0%A4%AF' THEN 'popular'
            WHEN t.data['pos']='%E0%A4%B9%E0%A4%BE%E0%A4%B2+%E0%A4%B9%E0%A5%80+%E0%A4%AE%E0%A5%87%E0%A4%82' THEN 'recently'
            ELSE t.data['pos'] END AS getin_pos,
      COUNT(DISTINCT t.data['user_id']) getin_user_cnt
  FROM sharemax_dw.action_log t JOIN user_center.funshare_user_accounts u ON t.data['user_id']=u.user_id
  WHERE t.logdate BETWEEN date_sub('2019-08-21',7) AND '2019-08-21'
    AND to_date(from_unixtime(unix_timestamp(regexp_replace(substr(t.`timestamp`,1,19),'T',' ')) + 19800)) BETWEEN date_sub('2019-08-21',6) AND '2019-08-21'
    AND t.data['type']='getin_chatroom_success'
    AND t.data['pos']<>''
    AND t.app_v='1.4.2'
  GROUP BY to_date(from_unixtime(unix_timestamp(regexp_replace(substr(t.`timestamp`,1,19),'T',' ')) + 19800)),
            CASE WHEN to_date(from_unixtime(unix_timestamp(u.created_at)+19800))=to_date(from_unixtime(unix_timestamp(regexp_replace(substr(t.`timestamp`,1,19),'T',' ')) + 19800)) THEN 'new' ELSE 'old' END,
            CASE WHEN t.data['pos']='%E0%A4%A8%E0%A4%AF%E0%A4%BE' THEN 'new'
                 WHEN t.data['pos']='%E0%A4%AB%E0%A5%89%E0%A4%B2%E0%A5%8B%E0%A4%87%E0%A4%82%E0%A4%97' THEN 'following'
                 WHEN t.data['pos']='%E0%A4%B2%E0%A5%8B%E0%A4%95%E0%A4%AA%E0%A5%8D%E0%A4%B0%E0%A4%BF%E0%A4%AF' THEN 'popular'
                 WHEN t.data['pos']='%E0%A4%B9%E0%A4%BE%E0%A4%B2+%E0%A4%B9%E0%A5%80+%E0%A4%AE%E0%A5%87%E0%A4%82' THEN 'recently'
                 ELSE t.data['pos'] END
  GROUPING SETS(
    (to_date(from_unixtime(unix_timestamp(regexp_replace(substr(t.`timestamp`,1,19),'T',' ')) + 19800)),
    CASE WHEN to_date(from_unixtime(unix_timestamp(u.created_at)+19800))=to_date(from_unixtime(unix_timestamp(regexp_replace(substr(t.`timestamp`,1,19),'T',' ')) + 19800)) THEN 'new' ELSE 'old' END),
    (to_date(from_unixtime(unix_timestamp(regexp_replace(substr(t.`timestamp`,1,19),'T',' ')) + 19800)),
    CASE WHEN to_date(from_unixtime(unix_timestamp(u.created_at)+19800))=to_date(from_unixtime(unix_timestamp(regexp_replace(substr(t.`timestamp`,1,19),'T',' ')) + 19800)) THEN 'new' ELSE 'old' END,
    CASE WHEN t.data['pos']='%E0%A4%A8%E0%A4%AF%E0%A4%BE' THEN 'new'
         WHEN t.data['pos']='%E0%A4%AB%E0%A5%89%E0%A4%B2%E0%A5%8B%E0%A4%87%E0%A4%82%E0%A4%97' THEN 'following'
         WHEN t.data['pos']='%E0%A4%B2%E0%A5%8B%E0%A4%95%E0%A4%AA%E0%A5%8D%E0%A4%B0%E0%A4%BF%E0%A4%AF' THEN 'popular'
         WHEN t.data['pos']='%E0%A4%B9%E0%A4%BE%E0%A4%B2+%E0%A4%B9%E0%A5%80+%E0%A4%AE%E0%A5%87%E0%A4%82' THEN 'recently'
         ELSE t.data['pos'] END))) getin
  JOIN
  (SELECT to_date(from_unixtime(unix_timestamp(t.`time`)+19800)) india_date,
          CASE WHEN to_date(from_unixtime(unix_timestamp(u.created_at)+19800))=to_date(from_unixtime(unix_timestamp(t.`time`)+19800)) THEN 'new' ELSE 'old' END AS user_type,
          COUNT(DISTINCT t.uid) dau_cnt
  FROM sharemax_dw.refresh t JOIN user_center.funshare_user_accounts u ON t.uid=u.user_id
  WHERE t.logdate BETWEEN date_sub('2019-08-21',7) AND '2019-08-21'
    AND to_date(from_unixtime(unix_timestamp(t.`time`)+19800)) BETWEEN date_sub('2019-08-21',6) AND '2019-08-21'
    AND t.refresh_id IS NOT NULL
    AND t.app_version='1.4.2'
  GROUP BY to_date(from_unixtime(unix_timestamp(t.`time`)+19800)),
        CASE WHEN to_date(from_unixtime(unix_timestamp(u.created_at)+19800))=to_date(from_unixtime(unix_timestamp(t.`time`)+19800)) THEN 'new' ELSE 'old' END) dau
  ON getin.india_date=dau.india_date AND getin.user_type=dau.user_type
WHERE length(getin.getin_pos)<10



WITH tmp AS
(SELECT to_date(from_unixtime(unix_timestamp(regexp_replace(substr(t.`timestamp`,1,19),'T',' ')) + 19800)) india_date,
       CASE WHEN to_date(from_unixtime(unix_timestamp(u.created_at)+19800))=to_date(from_unixtime(unix_timestamp(regexp_replace(substr(t.`timestamp`,1,19),'T',' ')) + 19800)) THEN 'new' ELSE 'old' END AS user_type,
       CASE WHEN t.data['pos']='%E0%A4%A8%E0%A4%AF%E0%A4%BE' THEN 'new'
            WHEN t.data['pos']='%E0%A4%AB%E0%A5%89%E0%A4%B2%E0%A5%8B%E0%A4%87%E0%A4%82%E0%A4%97' THEN 'following'
            WHEN t.data['pos']='%E0%A4%B2%E0%A5%8B%E0%A4%95%E0%A4%AA%E0%A5%8D%E0%A4%B0%E0%A4%BF%E0%A4%AF' THEN 'popular'
            WHEN t.data['pos']='%E0%A4%B9%E0%A4%BE%E0%A4%B2+%E0%A4%B9%E0%A5%80+%E0%A4%AE%E0%A5%87%E0%A4%82' THEN 'recently'
            ELSE t.data['pos'] END AS getin_pos,
       t.data['user_id'] user_id
FROM sharemax_dw.action_log t JOIN user_center.funshare_user_accounts u ON t.data['user_id']=u.user_id
WHERE t.logdate BETWEEN date_sub('2019-08-21',7) AND '2019-08-21'
  AND to_date(from_unixtime(unix_timestamp(regexp_replace(substr(t.`timestamp`,1,19),'T',' ')) + 19800)) BETWEEN date_sub('2019-08-21',6) AND '2019-08-21'
  AND t.data['type']='getin_chatroom_success'
  AND t.data['pos']<>''
  AND t.app_v='1.4.2')

SELECT getin.india_date india_date,
       getin.user_type user_type,
       COALESCE(getin.getin_pos,'all') getin_pos,
       getin.getin_user_cnt,
       dau.dau_cnt
FROM
  (SELECT t.india_date india_date,
       t.user_type user_type,
       t.getin_pos getin_pos,
       COUNT(DISTINCT t.user_id) getin_user_cnt
  FROM tmp t
  GROUP BY t.india_date,
            t.user_type,
            t.getin_pos
  GROUPING SETS((t.india_date,t.user_type),
                (t.india_date,t.user_type,t.getin_pos))) getin
  JOIN
  (SELECT to_date(from_unixtime(unix_timestamp(t.`time`)+19800)) india_date,
          CASE WHEN to_date(from_unixtime(unix_timestamp(u.created_at)+19800))=to_date(from_unixtime(unix_timestamp(t.`time`)+19800)) THEN 'new' ELSE 'old' END AS user_type,
          COUNT(DISTINCT t.uid) dau_cnt
  FROM sharemax_dw.refresh t JOIN user_center.funshare_user_accounts u ON t.uid=u.user_id
  WHERE t.logdate BETWEEN date_sub('2019-08-21',7) AND '2019-08-21'
    AND to_date(from_unixtime(unix_timestamp(t.`time`)+19800)) BETWEEN date_sub('2019-08-21',6) AND '2019-08-21'
    AND t.refresh_id IS NOT NULL
    AND t.app_version='1.4.2'
  GROUP BY to_date(from_unixtime(unix_timestamp(t.`time`)+19800)),
        CASE WHEN to_date(from_unixtime(unix_timestamp(u.created_at)+19800))=to_date(from_unixtime(unix_timestamp(t.`time`)+19800)) THEN 'new' ELSE 'old' END) dau
  ON getin.india_date=dau.india_date AND getin.user_type=dau.user_type
WHERE length(getin.getin_pos)<10


wangcai:
WITH tmp AS
(SELECT t.logdate middle_east_date,
       CASE WHEN to_date(from_unixtime(unix_timestamp(u.created_at)+10800))=t.logdate THEN 'new' ELSE 'old' END AS user_type,
       t.event_params['pos'] getin_pos,
       t.user_id user_id
FROM wangcai_dw.wangcai_app_action_log_from_firebase t JOIN user_center.wangcai_user_accounts u ON t.user_id=u.user_id
WHERE t.logdate BETWEEN date_sub('2019-09-16',2) AND '2019-09-16'
  AND t.event_name='getin_chatroom_success'
  AND t.event_params['pos']<>'')

SELECT getin.middle_east_date middle_east_date,
       getin.user_type user_type,
       COALESCE(getin.getin_pos,'all') getin_pos,
       getin.getin_user_cnt getin_user_cnt
FROM
  (SELECT t.middle_east_date middle_east_date,
       t.user_type user_type,
       t.getin_pos getin_pos,
       COUNT(DISTINCT t.user_id) getin_user_cnt
  FROM tmp t
  GROUP BY t.middle_east_date,
            t.user_type,
            t.getin_pos
  GROUPING SETS((t.middle_east_date,t.user_type),
                (t.middle_east_date,t.user_type,t.getin_pos))) getin


issue#4929
聊天室各个tab最近7天的点击用户渗透

SELECT sub_tab.india_date india_date,
       sub_tab.user_type user_type,
       COALESCE(sub_tab.tab_name,'all') tab_name,
       sub_tab.click_user_cnt
FROM
  (SELECT to_date(from_unixtime(unix_timestamp(regexp_replace(substr(t.`timestamp`,1,19),'T',' ')) + 19800)) india_date,
         CASE WHEN to_date(from_unixtime(unix_timestamp(u.created_at)+19800))=to_date(from_unixtime(unix_timestamp(regexp_replace(substr(t.`timestamp`,1,19),'T',' ')) + 19800)) THEN 'new' ELSE 'old' END AS user_type,
         CASE WHEN t.data['name']='%E0%A4%A8%E0%A4%AF%E0%A4%BE' THEN 'new'
              WHEN t.data['name']='%E0%A4%AB%E0%A5%89%E0%A4%B2%E0%A5%8B%E0%A4%87%E0%A4%82%E0%A4%97' THEN 'following'
              WHEN t.data['name']='%E0%A4%B2%E0%A5%8B%E0%A4%95%E0%A4%AA%E0%A5%8D%E0%A4%B0%E0%A4%BF%E0%A4%AF' THEN 'popular'
              WHEN t.data['name']='%E0%A4%B9%E0%A4%BE%E0%A4%B2+%E0%A4%B9%E0%A5%80+%E0%A4%AE%E0%A5%87%E0%A4%82' THEN 'recently'
              ELSE t.data['name'] END AS tab_name,
        COUNT(DISTINCT t.data['user_id']) click_user_cnt
  FROM sharemax_dw.action_log t JOIN user_center.funshare_user_accounts u ON t.data['user_id']=u.user_id
  WHERE t.logdate BETWEEN date_sub('2019-08-21',1) AND '2019-08-21'
    AND to_date(from_unixtime(unix_timestamp(regexp_replace(substr(t.`timestamp`,1,19),'T',' ')) + 19800)) BETWEEN date_sub('2019-08-21',0) AND '2019-08-21'
    AND t.data['type']='click_chatroom_sub_tab'
    AND t.data['name']<>''
    AND t.app_v='1.4.2'
  GROUP BY to_date(from_unixtime(unix_timestamp(regexp_replace(substr(t.`timestamp`,1,19),'T',' ')) + 19800)),
            CASE WHEN to_date(from_unixtime(unix_timestamp(u.created_at)+19800))=to_date(from_unixtime(unix_timestamp(regexp_replace(substr(t.`timestamp`,1,19),'T',' ')) + 19800)) THEN 'new' ELSE 'old' END,
            CASE WHEN t.data['name']='%E0%A4%A8%E0%A4%AF%E0%A4%BE' THEN 'new'
                 WHEN t.data['name']='%E0%A4%AB%E0%A5%89%E0%A4%B2%E0%A5%8B%E0%A4%87%E0%A4%82%E0%A4%97' THEN 'following'
                 WHEN t.data['name']='%E0%A4%B2%E0%A5%8B%E0%A4%95%E0%A4%AA%E0%A5%8D%E0%A4%B0%E0%A4%BF%E0%A4%AF' THEN 'popular'
                 WHEN t.data['name']='%E0%A4%B9%E0%A4%BE%E0%A4%B2+%E0%A4%B9%E0%A5%80+%E0%A4%AE%E0%A5%87%E0%A4%82' THEN 'recently'
                 ELSE t.data['name'] END
  GROUPING SETS(
      (to_date(from_unixtime(unix_timestamp(regexp_replace(substr(t.`timestamp`,1,19),'T',' ')) + 19800)),
      CASE WHEN to_date(from_unixtime(unix_timestamp(u.created_at)+19800))=to_date(from_unixtime(unix_timestamp(regexp_replace(substr(t.`timestamp`,1,19),'T',' ')) + 19800)) THEN 'new' ELSE 'old' END),
      (to_date(from_unixtime(unix_timestamp(regexp_replace(substr(t.`timestamp`,1,19),'T',' ')) + 19800)),
      CASE WHEN to_date(from_unixtime(unix_timestamp(u.created_at)+19800))=to_date(from_unixtime(unix_timestamp(regexp_replace(substr(t.`timestamp`,1,19),'T',' ')) + 19800)) THEN 'new' ELSE 'old' END,
      CASE WHEN t.data['name']='%E0%A4%A8%E0%A4%AF%E0%A4%BE' THEN 'new'
           WHEN t.data['name']='%E0%A4%AB%E0%A5%89%E0%A4%B2%E0%A5%8B%E0%A4%87%E0%A4%82%E0%A4%97' THEN 'following'
           WHEN t.data['name']='%E0%A4%B2%E0%A5%8B%E0%A4%95%E0%A4%AA%E0%A5%8D%E0%A4%B0%E0%A4%BF%E0%A4%AF' THEN 'popular'
           WHEN t.data['name']='%E0%A4%B9%E0%A4%BE%E0%A4%B2+%E0%A4%B9%E0%A5%80+%E0%A4%AE%E0%A5%87%E0%A4%82' THEN 'recently'
           ELSE t.data['name'] END))) sub_tab
WHERE length(sub_tab.tab_name)<10


WITH tmp AS
(SELECT to_date(from_unixtime(unix_timestamp(regexp_replace(substr(t.`timestamp`,1,19),'T',' ')) + 19800)) india_date,
         CASE WHEN to_date(from_unixtime(unix_timestamp(u.created_at)+19800))=to_date(from_unixtime(unix_timestamp(regexp_replace(substr(t.`timestamp`,1,19),'T',' ')) + 19800)) THEN 'new' ELSE 'old' END AS user_type,
         CASE WHEN t.data['name']='%E0%A4%A8%E0%A4%AF%E0%A4%BE' THEN 'new'
              WHEN t.data['name']='%E0%A4%AB%E0%A5%89%E0%A4%B2%E0%A5%8B%E0%A4%87%E0%A4%82%E0%A4%97' THEN 'following'
              WHEN t.data['name']='%E0%A4%B2%E0%A5%8B%E0%A4%95%E0%A4%AA%E0%A5%8D%E0%A4%B0%E0%A4%BF%E0%A4%AF' THEN 'popular'
              WHEN t.data['name']='%E0%A4%B9%E0%A4%BE%E0%A4%B2+%E0%A4%B9%E0%A5%80+%E0%A4%AE%E0%A5%87%E0%A4%82' THEN 'recently'
              ELSE t.data['name'] END AS tab_name,
      t.data['user_id'] user_id
FROM sharemax_dw.action_log t JOIN user_center.funshare_user_accounts u ON t.data['user_id']=u.user_id
WHERE t.logdate BETWEEN date_sub('2019-08-22',7) AND '2019-08-22'
  AND to_date(from_unixtime(unix_timestamp(regexp_replace(substr(t.`timestamp`,1,19),'T',' ')) + 19800)) BETWEEN date_sub('2019-08-22',6) AND '2019-08-22'
  AND t.data['type']='click_chatroom_sub_tab'
  AND t.data['name']<>''
  AND t.app_v>='1.4.2')

SELECT sub_tab.india_date india_date,
       sub_tab.user_type user_type,
       COALESCE(sub_tab.tab_name,'total') tab_name,
       sub_tab.click_user_cnt
FROM
  (SELECT t.india_date india_date,
         t.user_type user_type,
         t.tab_name tab_name,
         COUNT(DISTINCT t.user_id) click_user_cnt
  FROM tmp t
  GROUP BY t.india_date,
           t.user_type,
           t.tab_name
  GROUPING SETS((t.india_date,t.user_type),
                (t.india_date,t.user_type,t.tab_name))) sub_tab
WHERE length(sub_tab.tab_name)<10



wangcai:
WITH tmp AS
(SELECT t.logdate middle_east_date,
         CASE WHEN to_date(from_unixtime(unix_timestamp(u.created_at)+10800))=t.logdate THEN 'new' ELSE 'old' END AS user_type,
         t.event_params['name'] tab_name,
         t.user_id user_id
FROM wangcai_dw.wangcai_app_action_log_from_firebase t JOIN user_center.wangcai_user_accounts u ON t.user_id=u.user_id
WHERE t.logdate BETWEEN date_sub('2019-09-16',1) AND '2019-09-16'
  AND t.event_name='click_chatroom_sub_tab'
  AND t.event_params['name']<>'')

SELECT sub_tab.middle_east_date middle_east_date,
       sub_tab.user_type user_type,
       COALESCE(sub_tab.tab_name,'total') tab_name,
       sub_tab.click_user_cnt
FROM
  (SELECT t.middle_east_date middle_east_date,
         t.user_type user_type,
         t.tab_name tab_name,
         COUNT(DISTINCT t.user_id) click_user_cnt
  FROM tmp t
  GROUP BY t.middle_east_date,
           t.user_type,
           t.tab_name
  GROUPING SETS((t.middle_east_date,t.user_type),
                (t.middle_east_date,t.user_type,t.tab_name))) sub_tab



issue#4930
Hindi下audio feed comment+like数量前2000名的feed数据，需要字段如下：：
语言，feed id ,发feed的用户性别

SELECT f.lang lang,
        t.feed_seq_id feed_id,
        u.user_id user_id,
        u.gender gender,
        (get_json_object(t.stat_count_mapping,'$.comment_count')+get_json_object(t.stat_count_mapping,'$.like_count')) comment_like_cnt
FROM sharemax.feed_statistics t
JOIN sharemax_dw.feed f ON t.feed_seq_id=f.seq_id AND f.feed_type='audio' AND f.lang='hi' AND f.logdate>='2019-05-25'
JOIN user_center.funshare_user_accounts u ON f.user_id=u.user_id
ORDER BY comment_like_cnt DESC
LIMIT 2000




SELECT to_date(from_unixtime(unix_timestamp(t.`time`)+19800)) india_date,
    t.rewarder_id rewarder_id,
    t.receiver_id receiver_id,
    SUM(t.gift_cost/size(t.receiver)) reward_coin_cnt
FROM iap.reward_log t LATERAL VIEW explode(t.receiver) t AS receiver_id
WHERE t.logdate BETWEEN date_sub('2019-08-01',1) AND '2019-08-31'
  AND to_date(from_unixtime(unix_timestamp(t.`time`)+19800)) BETWEEN '2019-08-01' AND '2019-08-31'
  AND t.reward_info_id<>0
  AND t.app='funshare'
GROUP BY to_date(from_unixtime(unix_timestamp(t.`time`)+19800)),
    t.rewarder_id,
    t.receiver_id


SELECT r.receiver_id receiver_id,
    SUM(t.gift_sum_cost/t.receiver_count) reward_coin_cnt
FROM iap.reward_info t JOIN iap.reward_relation r ON t.id=r.reward_info_id
WHERE to_date(from_unixtime(unix_timestamp(t.reward_at)+19800)) BETWEEN '2019-07-01' AND '2019-07-31'
  AND t.app='funshare'
  AND r.receiver_id IN(19750543,
                    21643591,
                    19825902,
                    19748451,
                    19756793,
                    20643364,
                    19750639,
                    20740358,
                    20644832,
                    19799828,
                    19808787,
                    19750316,
                    19814737,
                    19568658,
                    21568005,
                    21507737,
                    19786226,
                    21577333,
                    20307713,
                    21497670,
                    21599096,
                    19637066,
                    22710540,
                    18789776,
                    19938622,
                    18764531,
                    19041543,
                    18139064,
                    15272837,
                    19873358,
                    21907616,
                    18829990,
                    15260148,
                    18328491,
                    15856891,
                    17818479,
                    15261635,
                    15322155,
                    22528546,
                    22537341,
                    19809559,
                    22779794,
                    22312020)
GROUP BY r.receiver_id


chatroom网络图数据：

【1】TOGO：
SELECT '0101-0107' india_date,
        re.rewarder_id rewarder_id,
        u.name rewarder_name,
        CASE WHEN u.gender='boy' THEN 'M'
        	 WHEN u.gender='girl' THEN 'F'
        ELSE 'N' END AS rewarder_gender,
        re.receiver_id receiver_id,
        u2.name receiver_name,
        CASE WHEN u2.gender='boy' THEN 'M'
        	 WHEN u2.gender='girl' THEN 'F'
        ELSE 'N' END AS receiver_gender,
        re.reward_coin_cnt reward_coin_cnt
FROM
  (SELECT t.rewarder_id rewarder_id,
      t.receiver_id receiver_id,
      SUM(t.gift_cost/size(t.receiver)) reward_coin_cnt
  FROM iap.reward_log t LATERAL VIEW explode(t.receiver) t AS receiver_id
  WHERE t.logdate BETWEEN date_sub('2020-01-01',1) AND '2020-01-07'
    AND to_date(from_unixtime(unix_timestamp(t.`time`)+19800)) BETWEEN date_sub('2020-01-01',1) AND '2020-01-07'
    AND t.reward_info_id<>0
    AND t.transaction_id<>0
    AND t.app='funshare'
  GROUP BY t.rewarder_id,
      t.receiver_id) re
  LEFT JOIN user_center.funshare_user_accounts u ON re.rewarder_id=u.user_id
  LEFT JOIN user_center.funshare_user_accounts u2 ON re.receiver_id=u2.user_id
WHERE re.reward_coin_cnt>=1000











【2】wangcai：+user_id所属国家信息 +性别 +rewarder\receiver区分不同颜色
0916-0922
1003-1009

WITH tmp AS
(SELECT c.user_id user_id,
		c.country country
FROM
	(SELECT t.*,
			row_number() OVER(PARTITION BY t.user_id ORDER BY t.event_timestamp DESC) rn
	FROM wangcai_dw.wangcai_app_action_log_from_firebase t
	WHERE t.logdate BETWEEN '2020-01-01' AND '2020-01-07') c
WHERE c.rn=1)

SELECT '0101-0107' middle_east_date,
        re.rewarder_id rewarder_id,
        u.name rewarder_name,
        CASE WHEN u.gender='boy' THEN 'M'
        	WHEN u.gender='girl' THEN 'F'
        	ELSE 'N' END AS rewarder_gender,
        t1.country rewarder_country,
        re.receiver_id receiver_id,
        u2.name receiver_name,
        CASE WHEN u2.gender='boy' THEN 'M'
        	WHEN u2.gender='girl' THEN 'F'
        	ELSE 'N' END AS receiver_gender,
        t2.country receiver_country,
        re.reward_coin_cnt reward_coin_cnt
FROM
  (SELECT t.rewarder_id rewarder_id,
      t.receiver_id receiver_id,
      SUM(t.gift_cost/size(t.receiver)) reward_coin_cnt
  FROM iap.reward_log t LATERAL VIEW explode(t.receiver) t AS receiver_id
  WHERE t.logdate BETWEEN date_sub('2020-01-01',1) AND '2020-01-07'
    AND to_date(from_unixtime(unix_timestamp(t.`time`)+10800)) BETWEEN '2020-01-01' AND '2020-01-07'
    AND t.reward_info_id<>0
    AND t.transaction_id<>0
    AND t.app='wangcai'
  GROUP BY t.rewarder_id,
      t.receiver_id) re
  LEFT JOIN user_center.wangcai_user_accounts u ON re.rewarder_id=u.user_id
  LEFT JOIN user_center.wangcai_user_accounts u2 ON re.receiver_id=u2.user_id
  LEFT JOIN tmp t1 ON re.rewarder_id=t1.user_id
  LEFT JOIN tmp t2 ON re.receiver_id=t2.user_id
WHERE re.reward_coin_cnt>=1000

issue#4938
最近3天, 每天产生的每笔送礼行为的相关数据, 需要的字段有
日期Date
小时Time(精确到自然小时)
送礼用户id
送礼用户名
收礼用户id
收礼用户名
礼物金币价值

togo
SELECT re.india_date,
        re.date_hour,
        re.transaction_id,
        re.reward_type,
        re.rewarder_id,
        u.name rewarder_name,
        re.receiver_id,
        u2.name receiver_name,
        re.reward_coin_cnt
FROM
  (SELECT to_date(from_unixtime(unix_timestamp(t.`time`)+19800)) india_date,
        hour(from_unixtime(unix_timestamp(t.`time`)+19800)) date_hour,
        t.rewarder_id rewarder_id,
        t.receiver_id receiver_id,
        t.transaction_id transaction_id,
        CASE WHEN size(t.receiver)=1 THEN 'single' ELSE 'group' END AS reward_type,
        t.gift_cost/size(t.receiver) reward_coin_cnt
  FROM sharemax_dw.reward_log t LATERAL VIEW explode(t.receiver) t AS receiver_id
  WHERE t.logdate BETWEEN date_sub('2019-08-26',3) AND '2019-08-26'
    AND to_date(from_unixtime(unix_timestamp(t.`time`)+19800)) BETWEEN date_sub('2019-08-26',2) AND '2019-08-26'
    AND t.reward_info_id<>0) re
  LEFT JOIN user_center.funshare_user_accounts u ON re.rewarder_id=u.user_id
  LEFT JOIN user_center.funshare_user_accounts u2 ON re.receiver_id=u2.user_id

wangcai
SELECT re.middle_east_date,
        re.date_hour,
        re.transaction_id,
        re.reward_type,
        re.rewarder_id,
        u.name rewarder_name,
        re.receiver_id,
        u2.name receiver_name,
        re.reward_coin_cnt
FROM
  (SELECT to_date(from_unixtime(unix_timestamp(t.`time`)+10800)) middle_east_date,
        hour(from_unixtime(unix_timestamp(t.`time`)+10800)) date_hour,
        t.rewarder_id rewarder_id,
        t.receiver_id receiver_id,
        t.transaction_id transaction_id,
        CASE WHEN size(t.receiver)=1 THEN 'single' ELSE 'group' END AS reward_type,
        t.gift_cost/size(t.receiver) reward_coin_cnt
  FROM iap.reward_log t LATERAL VIEW explode(t.receiver) t AS receiver_id
  WHERE t.logdate BETWEEN date_sub('2019-08-26',3) AND '2019-08-26'
    AND to_date(from_unixtime(unix_timestamp(t.`time`)+10800)) BETWEEN date_sub('2019-08-26',2) AND '2019-08-26'
    AND t.reward_info_id<>0
    AND t.app='wangcai') re
  LEFT JOIN user_center.wangcai_user_accounts u ON re.rewarder_id=u.user_id
  LEFT JOIN user_center.wangcai_user_accounts u2 ON re.receiver_id=u2.user_id


SELECT to_date(from_unixtime(unix_timestamp(regexp_replace(substr(t.`timestamp`,1,19),'T',' ')) + 19800)) india_date,
     COUNT(DISTINCT t.data['user_id']) upgrade_user_cnt,
     COUNT(1) upgrade_cnt
FROM sharemax_dw.action_log t
WHERE t.logdate BETWEEN date_sub('2019-08-27',7) AND '2019-08-27'
  AND to_date(from_unixtime(unix_timestamp(regexp_replace(substr(t.`timestamp`,1,19),'T',' ')) + 19800)) BETWEEN date_sub('2019-08-27',6) AND '2019-08-27'
  AND t.data['type'] IN('cr_upgrade')
GROUP BY to_date(from_unixtime(unix_timestamp(regexp_replace(substr(t.`timestamp`,1,19),'T',' ')) + 19800))



SELECT to_date(from_unixtime(substr(t.event_timestamp,1,10)+10800)) middle_east_date,
		t.user_id,
		u.name user_name,
		COUNT(1) upgrade_cnt
FROM wangcai_dw.wangcai_app_action_log_from_firebase t JOIN user_center.wangcai_user_accounts u ON t.user_id=u.user_id
WHERE t.logdate BETWEEN '2019-09-01' AND '2019-09-10'
  AND t.event_name='cr_upgrade'
GROUP BY to_date(from_unixtime(substr(t.event_timestamp,1,10)+10800)),
		t.user_id,
		u.name




chatroom_ban_mute 事件中user_id是房主的user_id

闭麦：chatroom_mute


开麦：chatroom_open_speaker
下麦：chatroom_leave_queue
离开房间 leave_chatroom


WITH tmp AS
(
  SELECT t.india_date,
         t.room_id,
         t.user_id,
         t.event_name,
         t.time,
         row_number() over(PARTITION BY t.room_id,t.user_id ORDER by t.timestamp) rn
  from
  (SELECT to_date(from_unixtime(unix_timestamp(regexp_replace(substr(t.`timestamp`,1,19),'T',' ')) + 19800)) india_date,
          t.data['room_id'] room_id,
          t.data['user_id'] user_id,
          t.data['type'] event_name,
          t.timestamp
    FROM sharemax.action_log c
   WHERE t.logdate BETWEEN date_sub('2019-08-29',1) AND '2019-08-29'
     AND to_date(from_unixtime(unix_timestamp(regexp_replace(substr(t.`timestamp`,1,19),'T',' ')) + 19800))='2019-08-29'
     AND t.data['type'] IN('chatroom_mute')
     AND t.data['user']='host'
   UNION ALL
   SELECT to_date(from_unixtime(unix_timestamp(regexp_replace(substr(t.`timestamp`,1,19),'T',' ')) + 19800)) india_date,
          t.data['room_id'] room_id,
          t.data['user_id'] user_id,
          t.data['type'] event_name,
          t.timestamp
    FROM sharemax.action_log c
   WHERE t.logdate BETWEEN date_sub('2019-08-29',1) AND '2019-08-29'
     AND to_date(from_unixtime(unix_timestamp(regexp_replace(substr(t.`timestamp`,1,19),'T',' ')) + 19800))='2019-08-29'
     AND t.data['type'] IN('chatroom_open_speaker','chatroom_leave_queue','leave_chatroom')) t
)


SELECT t1.india_date,
       t1.room_id,
       t1.user_id,
       SUM(unix_timestamp(regexp_replace(substr(t2.`timestamp`,1,19),'T',' '))-unix_timestamp(regexp_replace(substr(t1.`timestamp`,1,19),'T',' ')))/60 mute_time --minutes
FROM tmp t1
LEFT JOIN tmp t2
ON t1.user_id=t2.user_id AND t1.room_id=t2.user_id
AND t2.event_name IN('chatroom_open_speaker','chatroom_leave_queue','leave_chatroom')
WHERE t1.event_name IN('chatroom_mute')
  AND t2.rn-t1.rn=1
GROUP BY t1.india_date,
          t1.room_id,
          t1.user_id


issue 麦上静音时长
开始：
mic_turn_off
结束：
mic_turn_on
host_leave
abnormal_exit
normal_exit
mic_give_up
mic_kick
user_remove


WITH tmp AS
(SELECT t.india_date,
		t.user_id,
		t.method,
		t.room_id,
		t.time,
		row_number() OVER(PARTITION BY t.user_id,t.room_id ORDER BY t.time) rn
FROM
	(SELECT to_date(from_unixtime(unix_timestamp(t.`time`)+19800)) india_date,
			abs(t.receiver) user_id,
			t.method method,
			t.room_id room_id,
			t.time
	FROM sharemax_dw.chatroom_user_action t
	WHERE logdate BETWEEN date_sub('2019-08-28',1) AND '2019-08-28'
	  AND to_date(from_unixtime(unix_timestamp(t.`time`)+19800))='2019-08-28'
	  AND t.method IN('mic_turn_off','mic_turn_on','mic_kick','user_remove')
	UNION ALL
	SELECT to_date(from_unixtime(unix_timestamp(t.`time`)+19800)) india_date,
			abs(t.sender) user_id,
			t.method method,
			t.room_id room_id,
			t.time
	FROM sharemax_dw.chatroom_user_action t
	WHERE logdate BETWEEN date_sub('2019-08-28',1) AND '2019-08-28'
	  AND to_date(from_unixtime(unix_timestamp(t.`time`)+19800))='2019-08-28'
	  AND t.method IN('host_leave','abnormal_exit','normal_exit','mic_give_up')) t)

SELECT t1.india_date,
		t1.user_id,
		t1.room_id,
		SUM(unix_timestamp(t2.`time`)-unix_timestamp(t1.`time`))/60 mute_time --minute
FROM tmp t1
LEFT JOIN tmp t2
ON t1.user_id=t2.user_id AND t1.room_id=t2.room_id
AND t2.method IN('mic_turn_on','host_leave','abnormal_exit','normal_exit','mic_give_up','mic_kick','user_remove')
WHERE t1.method IN('mic_turn_off')
  AND t2.rn-t1.rn=1
  AND unix_timestamp(t2.`time`)-unix_timestamp(t1.`time`)<=1800
GROUP BY t1.india_date,
		t1.user_id,
		t1.room_id

SELECT t.india_date,
		t.user_id,
		t.method,
		t.room_id,
		t.time,
		row_number() OVER(PARTITION BY t.user_id,t.room_id ORDER BY t.time) rn
FROM
	(SELECT to_date(from_unixtime(unix_timestamp(t.`time`)+19800)) india_date,
			abs(t.receiver) user_id,
			t.method method,
			t.room_id room_id,
			t.time
	FROM sharemax_dw.chatroom_user_action t
	WHERE logdate BETWEEN date_sub('2019-08-28',1) AND '2019-08-28'
	  AND to_date(from_unixtime(unix_timestamp(t.`time`)+19800))='2019-08-28'
	  AND t.method IN('mic_turn_off','mic_turn_on','mic_kick','user_remove')
	UNION ALL
	SELECT to_date(from_unixtime(unix_timestamp(t.`time`)+19800)) india_date,
			abs(t.sender) user_id,
			t.method method,
			t.room_id room_id,
			t.time
	FROM sharemax_dw.chatroom_user_action t
	WHERE logdate BETWEEN date_sub('2019-08-28',1) AND '2019-08-28'
	  AND to_date(from_unixtime(unix_timestamp(t.`time`)+19800))='2019-08-28'
	  AND t.method IN('host_leave','abnormal_exit','normal_exit','mic_give_up','new_user')) t
WHERE t.user_id=22462505

WITH tmp AS
(SELECT t.india_date,
		t.user_id,
		t.method,
		t.room_id,
		t.time,
		row_number() OVER(PARTITION BY t.user_id,t.room_id ORDER BY t.time) rn
FROM
	(SELECT to_date(from_unixtime(unix_timestamp(t.`time`)+19800)) india_date,
			abs(t.receiver) user_id,
			t.method method,
			t.room_id room_id,
			t.time
	FROM sharemax_dw.chatroom_user_action t
	WHERE logdate BETWEEN date_sub('2019-08-28',1) AND '2019-08-28'
	  AND to_date(from_unixtime(unix_timestamp(t.`time`)+19800))='2019-08-28'
	  AND t.method IN('mic_turn_off','mic_turn_on','mic_kick','user_remove')
	UNION ALL
	SELECT to_date(from_unixtime(unix_timestamp(t.`time`)+19800)) india_date,
			abs(t.sender) user_id,
			t.method method,
			t.room_id room_id,
			t.time
	FROM sharemax_dw.chatroom_user_action t
	WHERE logdate BETWEEN date_sub('2019-08-28',1) AND '2019-08-28'
	  AND to_date(from_unixtime(unix_timestamp(t.`time`)+19800))='2019-08-28'
	  AND t.method IN('host_leave','abnormal_exit','normal_exit','mic_give_up')) t)

SELECT t1.*,
		t2.*,
		(unix_timestamp(t2.`time`)-unix_timestamp(t1.`time`))/60 time_between
FROM tmp t1
LEFT JOIN tmp t2
ON t1.user_id=t2.user_id AND t1.room_id=t2.room_id
AND t2.method IN('mic_turn_on','host_leave','abnormal_exit','normal_exit','mic_give_up','mic_kick','user_remove')
WHERE t1.method IN('mic_turn_off')
  AND t2.rn-t1.rn=1
GROUP BY t1.india_date,
		t1.user_id,
		t1.room_id
WHERE t1.user_id=22462505


wangcai

WITH tmp AS
(SELECT t.middle_east_date,
		t.user_id,
		t.method,
		t.room_id,
		t.time,
		row_number() OVER(PARTITION BY t.user_id,t.room_id ORDER BY t.time) rn
FROM
	(SELECT t.logdate middle_east_date,
			abs(t.receiver) user_id,
			t.method method,
			t.room_id room_id,
			t.time
	FROM wangcai_dw.wangcai_chatroom_user_action t
	WHERE t.logdate BETWEEN '2019-08-28' AND '2019-08-28'
	  AND t.method IN('mic_turn_off','mic_turn_on','mic_kick','user_remove')
	UNION ALL
	SELECT t.logdate middle_east_date,
			abs(t.sender) user_id,
			t.method method,
			t.room_id room_id,
			t.time
	FROM wangcai_dw.wangcai_chatroom_user_action t
	WHERE t.logdate BETWEEN '2019-08-28' AND '2019-08-28'
	  AND t.method IN('host_leave','abnormal_exit','normal_exit','mic_give_up')) t)

SELECT t1.middle_east_date,
		t1.user_id,
		t1.room_id,
		SUM(unix_timestamp(t2.`time`)-unix_timestamp(t1.`time`))/60 mute_time --minute
FROM tmp t1
LEFT JOIN tmp t2
ON t1.user_id=t2.user_id AND t1.room_id=t2.room_id
AND t2.method IN('mic_turn_on','host_leave','abnormal_exit','normal_exit','mic_give_up','mic_kick','user_remove')
WHERE t1.method IN('mic_turn_off')
  AND t2.rn-t1.rn=1
  AND unix_timestamp(t2.`time`)-unix_timestamp(t1.`time`)<=1800
GROUP BY t1.middle_east_date,
		t1.user_id,
		t1.room_id


issue#4955
+新增feed数量

SELECT t.india_date,
		t.tag_id,
		COUNT(DISTINCT t.id) new_feed_cnt
FROM sharemax_dw.feed t
WHERE t.logdate BETWEEN date_sub('2019-08-29',1) AND '2019-08-30'
  AND t.india_date BETWEEN '2019-08-29' AND '2019-08-30'
  AND t.tag_id IN('5d663245b5bdde000bba056f','5d66326b11c9b7000bfafee5',
				'5d6632b011c9b7000bfafee6','5ae43c4b99f58a169c752e1f',
				'5b31f8316e72673fa5cc85e0','5b6c10fa6e726716c19c2c5a')
GROUP BY t.india_date,t.tag_id

SELECT f1.india_date india_date,
		f1.feed_seq_id feed_seq_id,
		COUNT(DISTINCT concat(f1.refresh_id,f1.feed_seq_id)) refresh_cnt,
		COUNT(DISTINCT f1.uid) refresh_user_cnt,
		COUNT(DISTINCT CASE WHEN f3.action_type='like' THEN f3.action_id END) like_cnt,
		COUNT(DISTINCT CASE WHEN f3.action_type='comment' THEN f3.action_id END) comment_cnt,
		COUNT(DISTINCT CASE WHEN f3.action_type='share' THEN f3.action_id END) share_cnt,
		COUNT(DISTINCT CASE WHEN f3.action_type IN('fav','save') THEN f3.action_id END) fav_cnt
FROM
	(SELECT to_date(from_unixtime(unix_timestamp(t.`time`)+19800)) india_date,
			t.refresh_id refresh_id,
			t.uid uid,
			t.feed_seq_id feed_seq_id
	FROM sharemax_dw.refresh t LATERAL VIEW explode(t.feed_ids) t AS feed_seq_id
	WHERE t.logdate BETWEEN date_sub('2019-08-29',1) AND '2019-08-29'
	  AND to_date(from_unixtime(unix_timestamp(t.`time`)+19800)) BETWEEN '2019-08-29' AND '2019-08-29'
	  AND t.source='nearby'
	  AND t.feed_seq_id IN(37315272,37318254,37318985)
	  AND t.refresh_id IS NOT NULL
	  ) f1
	LEFT JOIN
	(SELECT * FROM sharemax_dw.user_action t
	WHERE t.logdate BETWEEN date_sub('2019-08-29',1) AND '2019-08-29'
	  AND to_date(from_unixtime(unix_timestamp(t.time)+19800)) BETWEEN '2019-08-29' AND '2019-08-29') f3
	ON f1.refresh_id=f3.refresh_id AND f1.feed_seq_id=f3.feed_seq_id
GROUP BY f1.india_date,
		f1.feed_seq_id


issue#4954

语音匹配matrix
需要字段：

日期
lang
female/male，all

进入match tab的人数 ——click_voice_match
点击录音按钮的次数，人数, ——click_my_voice_btn
进入卡片录制页面次数，人数 ——voice_match_start_record
从录同款进入卡片录制页面次数，人数, ——voice_match_start_record，data['from']='card'
从Retake voice进入卡片录制页面次数，人数——voice_match_start_record，data['from']='retake'
send次数，人数，——voice_match_record_complete，data['result']='success'


WITH tmp AS
(SELECT t.data['type'] event_name,
		t.data['user_id'] user_id,
		t.data['from'] pos,
		t.data['result'] result
FROM sharemax_dw.action_log t
WHERE t.logdate BETWEEN date_sub('{dat}',1) AND '{dat}'
  AND to_date(from_unixtime(unix_timestamp(regexp_replace(substr(t.`timestamp`,1,19),'T',' ')) + 19800))='{dat}'
  AND t.data['type'] IN ('click_voice_match','click_my_voice_btn','voice_match_start_record','voice_match_record_complete'))

SELECT '{dat}' india_date,
		u.current_lang lang,
		COALESCE(CASE WHEN u.gender IN('boy','M') THEN 'male'
				WHEN u.gender IN('girl','F') THEN 'female'
				ELSE 'no_gender'
				END, 'all') AS gender,
		COUNT(DISTINCT CASE WHEN t.event_name='click_voice_match' THEN t.user_id END) click_tab_user_cnt,
		COUNT(DISTINCT CASE WHEN t.event_name='click_my_voice_btn' THEN t.user_id END) click_my_voice_user_cnt,
		COUNT(CASE WHEN t.event_name='click_my_voice_btn' THEN 1 END) click_my_voice_cnt,
		COUNT(DISTINCT CASE WHEN t.event_name='voice_match_start_record' THEN t.user_id END) start_record_user_cnt,
		COUNT(CASE WHEN t.event_name='voice_match_start_record' THEN 1 END) start_record_cnt,
		COUNT(DISTINCT CASE WHEN t.event_name='voice_match_start_record' AND t.pos='card' THEN t.user_id END) AS same_record_user_cnt,
		COUNT(CASE WHEN t.event_name='voice_match_start_record' AND t.pos='card' THEN 1 END) AS same_record_cnt,
		COUNT(DISTINCT CASE WHEN t.event_name='voice_match_start_record' AND t.pos='retake' THEN t.user_id END) AS retake_record_user_cnt,
		COUNT(CASE WHEN t.event_name='voice_match_start_record' AND t.pos='retake' THEN 1 END) AS retake_record_cnt,
		COUNT(DISTINCT CASE WHEN t.event_name='voice_match_record_complete' AND t.result='success' THEN t.user_id END) send_user_cnt,
		COUNT(CASE WHEN t.event_name='voice_match_record_complete' AND t.result='success' THEN 1 END) send_cnt
FROM tmp t JOIN user_center.funshare_user_accounts u ON t.user_id=u.user_id
GROUP BY u.current_lang,
		CASE WHEN u.gender IN('boy','M') THEN 'male'
			 WHEN u.gender IN('girl','F') THEN 'female'
			 ELSE 'no_gender'
		END
GROUPING SETS((u.current_lang,CASE WHEN u.gender IN('boy','M') THEN 'male' WHEN u.gender IN('girl','F') THEN 'female' ELSE 'no_gender' END),
			  (u.current_lang))



SELECT a.feed_india_date,
       a.lang lang,
       a.tag_id tag_id,
       b.title tag_title,
       b.en_title tag_title_eng,
       c.id category_id,
       c.title category_title,
       c.en_title category_title_eng,
       a.feed_cnt feed_cnt,
       a.refresh_feed_cnt refresh_feed_cnt,
       a.share_cnt share_cnt,
       a.like_cnt like_cnt,
       a.fav_cnt fav_cnt,
       a.comment_cnt comment_cnt
from
(SELECT t.feed_india_date,
        t.feed_lang lang,
        t.feed_tag_id tag_id,
        count(distinct t.feed_seq_id) feed_cnt,
        count(distinct concat(t.user_action_refresh_feed_id,t.refresh_refresh_id)) refresh_feed_cnt,
        count(distinct CASE WHEN t.user_action_action_type='share' THEN t.user_action_action_id ELSE NULL END) share_cnt,
        count(distinct CASE WHEN t.user_action_action_type='like' THEN t.user_action_action_id ELSE NULL END) like_cnt,
        count(distinct CASE WHEN t.user_action_action_type='fav' THEN t.user_action_action_id ELSE NULL END) fav_cnt,
        count(distinct CASE WHEN t.user_action_action_type='comment' THEN t.user_action_action_id ELSE NULL END) comment_cnt
FROM dm.sharemax_feed_action_new t
WHERE t.logdate between '2019-08-28' and '2019-08-30'
  and t.feed_india_date between '2019-08-29' and '2019-08-30'
  and t.refresh_refresh_id is not NULL
GROUP BY t.feed_india_date,
         t.feed_lang,
         t.feed_tag_id)a
JOIN sharemax_dw.tag b
  ON a.tag_id=b.id and b.id in ('5d663245b5bdde000bba056f',
                                '5d66326b11c9b7000bfafee5',
                                '5d6632b011c9b7000bfafee6')
JOIN sharemax_dw.category c
  ON b.category_id = c.id


增加收礼物、送礼物的贡献值 gift contribution


SELECT re.india_date,
        re.rewarder_id,
        u.name rewarder_name,
        re.receiver_id,
        u2.name receiver_name,
        re.reward_coin_cnt,
        re.reward_contribution_cnt
FROM
  (SELECT to_date(from_unixtime(unix_timestamp(t.`time`)+19800)) india_date,
      t.rewarder_id rewarder_id,
      t.receiver_id receiver_id,
      SUM(t.gift_cost/size(t.receiver)) reward_coin_cnt,
      SUM(t.gift_contribution/size(t.receiver)) reward_contribution_cnt
  FROM iap.reward_log t LATERAL VIEW explode(t.receiver) t AS receiver_id
  WHERE t.logdate BETWEEN date_sub('2019-08-31',1) AND '2019-08-31'
    AND to_date(from_unixtime(unix_timestamp(t.`time`)+19800)) BETWEEN '2019-08-31' AND '2019-08-31'
    AND hour(from_unixtime(unix_timestamp(t.`time`)+19800)) in (18,19)
    AND t.room_id in ('5d4190fa5524eb000139deab')
    AND t.reward_info_id<>0
    AND t.app='funshare'
  GROUP BY to_date(from_unixtime(unix_timestamp(t.`time`)+19800)),
      t.rewarder_id,
      t.receiver_id) re
  LEFT JOIN user_center.funshare_user_accounts u ON re.rewarder_id=u.user_id
  LEFT JOIN user_center.funshare_user_accounts u2 ON re.receiver_id=u2.user_id


issue#4963
现在需要跑一下玩过语音匹配，即有过以下行为post，like，dislike
且不是机器人
且没有自己的房间（没有display id)
且没有在chatroom里被运营ban掉
的用户id
限定：粉丝数大于100

SELECT DISTINCT t.user_id,
		fans.follower_cnt fans_cnt
FROM sharemax_dw.match_user_action t
JOIN user_center.funshare_user_accounts u ON t.user_id=u.user_id AND u.user_type='client'
LEFT JOIN sharemax_dw.chatroom_info c ON t.user_id=c.user_id
LEFT JOIN sharemax.banned_group_user b ON t.user_id=b.user_id
LEFT JOIN
	(SELECT uf.follow_user_id user_id,
			COUNT(DISTINCT uf.user_id) follower_cnt
	FROM sharemax_dw.user_follows uf
	GROUP BY uf.follow_user_id) fans
ON t.user_id=fans.user_id
WHERE t.logdate>='2019-08-29'
  AND t.action IN('post','like','dislike')
  AND c.user_id IS NULL
  AND b.user_id IS NULL
  AND fans.follower_cnt>100



SELECT f.gender,
		f.is_logined,
		f.has_card,
		COUNT(DISTINCT f.user_id) user_cnt,
		SUM(f.dislike_cnt) ttl_dislike,
		SUM(f.dislike_cnt)/COUNT(DISTINCT f.user_id) dislike_per_user
FROM
	(SELECT t.data['user_id'] user_id,
			CASE WHEN u.gender IN('boy','M') THEN 'male'
				 WHEN u.gender IN('girl','F') THEN 'female'
				 ELSE 'no_gender'
			END AS gender,
			MAX(t.data['is_logined']) is_logined,
			MAX(t.data['has_card']) has_card,
			COUNT(1) dislike_cnt
	FROM sharemax_dw.action_log t JOIN user_center.funshare_user_accounts u ON t.data['user_id']=u.user_id
	WHERE t.logdate BETWEEN date_sub('2019-09-01',1) AND '2019-09-01'
	  AND to_date(from_unixtime(unix_timestamp(regexp_replace(substr(t.`timestamp`,1,19),'T',' ')) + 19800))='2019-09-01'
	  AND t.data['type']='voice_match_dislike'
	GROUP BY t.data['user_id'],
			CASE WHEN u.gender IN('boy','M') THEN 'male'
				 WHEN u.gender IN('girl','F') THEN 'female'
				 ELSE 'no_gender'
			END) f
GROUP BY f.gender,f.is_logined,f.has_card


当天已登录用户前5次滑动卡片的like和dislike数据

SELECT f.gender,
		COUNT(DISTINCT f.user_id) logined_user_cnt,
		SUM(top_5.like_cnt) ttl_like_cnt,
		SUM(top_5.dislike_cnt) ttl_dislike_cnt
FROM
	(SELECT t.data['user_id'] user_id,
			CASE WHEN u.gender IN('boy','M') THEN 'male'
				 WHEN u.gender IN('girl','F') THEN 'female'
				 ELSE 'no_gender'
			END AS gender,
			MAX(t.data['is_logined']) is_logined
	FROM sharemax_dw.action_log t JOIN user_center.funshare_user_accounts u ON t.data['user_id']=u.user_id
	WHERE t.logdate BETWEEN date_sub('2019-09-01',1) AND '2019-09-01'
	  AND to_date(from_unixtime(unix_timestamp(regexp_replace(substr(t.`timestamp`,1,19),'T',' ')) + 19800))='2019-09-01'
	  AND t.data['type'] IN('voice_match_dislike','voice_match_like')
	GROUP BY t.data['user_id'],
			CASE WHEN u.gender IN('boy','M') THEN 'male'
				 WHEN u.gender IN('girl','F') THEN 'female'
				 ELSE 'no_gender'
			END) f
	LEFT JOIN
	(SELECT f2.user_id,
			COUNT(CASE WHEN f2.event_type='voice_match_like' THEN 1 END) AS like_cnt,
			COUNT(CASE WHEN f2.event_type='voice_match_dislike' THEN 1 END) AS dislike_cnt
	FROM
		(SELECT t.data['user_id'] user_id,
				t.data['type'] event_type,
				row_number() OVER(PARTITION BY t.data['user_id'] ORDER BY t.`timestamp`) rn
		FROM sharemax_dw.action_log t
		WHERE t.logdate BETWEEN date_sub('2019-09-01',1) AND '2019-09-01'
		  AND to_date(from_unixtime(unix_timestamp(regexp_replace(substr(t.`timestamp`,1,19),'T',' ')) + 19800))='2019-09-01'
		  AND t.data['type'] IN('voice_match_dislike','voice_match_like')) f2
	WHERE f2.rn<=5
	GROUP BY f2.user_id) top_5
	ON f.user_id=top_5.user_id
WHERE f.is_logined=1
GROUP BY f.gender

wangcai ID：23499676，辛苦帮看下这个ID对应设备的所有ID，以及各个ID目前的状态（是否被封，被封了多久等）
SELECT f.advertising_id,
		f2.user_id
FROM
	(SELECT t.user_id,
			t.advertising_id
	FROM wangcai_dm.wangcai_user_device t
	WHERE t.user_id=23499676) f
	LEFT JOIN wangcai_dm.wangcai_user_device f2 ON f.advertising_id=f2.advertising_id

issue#4969
8月30日到现在

每天收集到free gift的unique用户数；free_gift_get
每天使用歌曲播放功能的unique用户数；
≥1.4.3 的dau


当天开播host数
当天开播host 在 1.4.3 版本及以上的人数
当天开播使用播放音乐功能的host数

togo:
SELECT act.india_date india_date,
		act.free_gift_get_user_cnt,
		act.free_gift_get_cnt,
		act.host_play_music_user_cnt,
		act.host_play_music_cnt,
		dau.dau_cnt
FROM
	(SELECT to_date(from_unixtime(unix_timestamp(regexp_replace(substr(t.`timestamp`,1,19),'T',' ')) + 19800)) india_date,
			COUNT(DISTINCT CASE WHEN t.data['type']='free_gift_get' THEN t.data['user_id'] END) free_gift_get_user_cnt,
			COUNT(CASE WHEN t.data['type']='free_gift_get' THEN 1 END) free_gift_get_cnt,
			COUNT(DISTINCT CASE WHEN t.data['type']='host_play_music' THEN t.data['user_id'] END) host_play_music_user_cnt,
			COUNT(CASE WHEN t.data['type']='host_play_music' THEN 1 END) host_play_music_cnt
	FROM sharemax_dw.action_log t
	WHERE t.logdate BETWEEN date_sub('2019-09-06',1) AND '2019-09-08'
	  AND to_date(from_unixtime(unix_timestamp(regexp_replace(substr(t.`timestamp`,1,19),'T',' ')) + 19800)) BETWEEN '2019-09-06' AND '2019-09-08'
	  AND t.data['type'] IN('host_play_music','free_gift_get')
	GROUP BY to_date(from_unixtime(unix_timestamp(regexp_replace(substr(t.`timestamp`,1,19),'T',' ')) + 19800))) act
	LEFT JOIN
	(SELECT to_date(from_unixtime(unix_timestamp(t.`time`)+19800)) india_date,
	        COUNT(DISTINCT t.uid) dau_cnt
	FROM sharemax_dw.refresh t
	WHERE t.logdate BETWEEN date_sub('2019-09-06',1) AND '2019-09-08'
	  AND to_date(from_unixtime(unix_timestamp(t.`time`)+19800)) BETWEEN '2019-09-06' AND '2019-09-08'
	  AND t.refresh_id IS NOT NULL
	  AND t.app_version>='1.4.3'
	GROUP BY to_date(from_unixtime(unix_timestamp(t.`time`)+19800))) dau
	ON act.india_date=dau.india_date


SELECT h.india_date,
		COUNT(DISTINCT h.host_id) host_cnt,
		COUNT(DISTINCT CASE WHEN dau.app_version>='1.4.3' THEN h.host_id END) v143_host_cnt,
		COUNT(DISTINCT m.host_id) host_play_music_user_cnt
FROM
	(SELECT DISTINCT to_date(from_unixtime(unix_timestamp(t.`time`)+19800)) india_date,
			t.user_id host_id
	FROM sharemax_dw.chatroom_online_duration t
	WHERE t.logdate BETWEEN date_sub('2019-09-06',1) AND '2019-09-08'
	  AND t.role='host'
	  AND to_date(from_unixtime(unix_timestamp(t.`time`)+19800)) BETWEEN '2019-09-06' AND '2019-09-08') h
	LEFT JOIN
	(SELECT DISTINCT to_date(from_unixtime(unix_timestamp(regexp_replace(substr(t.`timestamp`,1,19),'T',' ')) + 19800)) india_date,
			t.data['user_id'] host_id
	FROM sharemax_dw.action_log t
	WHERE t.logdate BETWEEN date_sub('2019-09-06',1) AND '2019-09-08'
	  AND to_date(from_unixtime(unix_timestamp(regexp_replace(substr(t.`timestamp`,1,19),'T',' ')) + 19800)) BETWEEN '2019-09-06' AND '2019-09-08'
	  AND t.data['type'] IN('host_play_music')) m
	ON h.india_date=m.india_date AND h.host_id=m.host_id
	LEFT JOIN
	(SELECT to_date(from_unixtime(unix_timestamp(t.`time`)+19800)) india_date,
	        t.uid user_id,
	        MAX(t.app_version) app_version
	FROM sharemax_dw.refresh t
	WHERE t.logdate BETWEEN date_sub('2019-09-06',1) AND '2019-09-08'
	  AND to_date(from_unixtime(unix_timestamp(t.`time`)+19800)) BETWEEN '2019-09-06' AND '2019-09-08'
	  AND t.refresh_id IS NOT NULL
	GROUP BY to_date(from_unixtime(unix_timestamp(t.`time`)+19800)),
			t.uid) dau
	ON h.india_date=dau.india_date AND h.host_id=dau.user_id
GROUP BY h.india_date


wangcai:
SELECT t.logdate middle_east_date,
		COUNT(DISTINCT CASE WHEN t.event_name='free_gift_get' THEN t.user_id END) free_gift_get_user_cnt,
		COUNT(DISTINCT CASE WHEN t.event_name='host_play_music' THEN t.user_id END) host_play_music_user_cnt
FROM wangcai_dw.wangcai_app_action_log_from_firebase t
WHERE t.logdate BETWEEN '2019-08-30' AND '2019-09-02'
  AND t.event_name IN('host_play_music','free_gift_get')
GROUP BY t.logdate


SELECT h.middle_east_date,
		COUNT(DISTINCT h.host_id) host_cnt,
		COUNT(DISTINCT CASE WHEN dau.app_version>='1.0.4' THEN h.host_id END) v104_host_cnt,
		COUNT(DISTINCT m.host_id) host_play_music_user_cnt
FROM
	(SELECT DISTINCT logdate middle_east_date,
			t.user_id host_id
	FROM wangcai_dw.wangcai_chatroom_online_duration t
	WHERE t.logdate BETWEEN '2019-09-06' AND '2019-09-08'
	  AND t.role='host') h
	LEFT JOIN
	(SELECT DISTINCT logdate middle_east_date,
			t.user_id host_id
	FROM wangcai_dw.wangcai_app_action_log_from_firebase t
	WHERE t.logdate BETWEEN '2019-09-06' AND '2019-09-08'
	  AND t.event_name IN('host_play_music')) m
	ON h.middle_east_date=m.middle_east_date AND h.host_id=m.host_id
	LEFT JOIN
	(SELECT t.logdate middle_east_date,
			t.user_id,
			MAX(t.app_version) app_version
	FROM wangcai_dw.wangcai_app_action_log_from_firebase t
	WHERE t.logdate BETWEEN '2019-09-06' AND '2019-09-08'
	GROUP BY t.logdate,
			t.user_id) dau
	ON h.middle_east_date=dau.middle_east_date AND h.host_id=dau.user_id
GROUP BY h.middle_east_date


issue#4970
0829-0902 参与话题tag的用户数据
数据包括：
用户名 用户ID 8.29-9.2 发布的带话题tag的feed总数

tag id
hi: 5d663245b5bdde000bba056f
ta: 5d66326b11c9b7000bfafee5
te: 5d6632b011c9b7000bfafee6

SELECT t.user_id,
		u.name,
		t.tag_id,
		COUNT(DISTINCT t.id) feed_cnt
FROM sharemax_dw.feed t JOIN user_center.funshare_user_accounts u ON t.user_id=u.user_id
WHERE t.logdate BETWEEN date_sub('2019-08-29',1) AND '2019-09-02'
  AND t.india_date BETWEEN '2019-08-29' AND '2019-09-02'
  AND t.tag_id IN('5d663245b5bdde000bba056f','5d66326b11c9b7000bfafee5','5d6632b011c9b7000bfafee6')
GROUP BY t.user_id,u.name,t.tag_id

SELECT to_date(from_unixtime(unix_timestamp(regexp_replace(substr(t.`timestamp`,1,19),'T',' ')) + 19800)) india_date,
    t.app_v,
    COUNT(DISTINCT t.data['user_id']) free_show_user_cnt,
    COUNT(1) free_show_cnt
FROM sharemax_dw.action_log t
WHERE t.logdate BETWEEN date_sub('2019-08-29',1) AND '2019-09-02'
  AND to_date(from_unixtime(unix_timestamp(regexp_replace(substr(t.`timestamp`,1,19),'T',' ')) + 19800)) BETWEEN '2019-08-29' AND '2019-09-02'
  AND t.data['type'] IN('free_gift_show')
GROUP BY to_date(from_unixtime(unix_timestamp(regexp_replace(substr(t.`timestamp`,1,19),'T',' ')) + 19800)),
		t.app_v


SELECT to_date(from_unixtime(unix_timestamp(regexp_replace(substr(t.`timestamp`,1,19),'T',' ')) + 19800)) india_date,
    COUNT(DISTINCT t.data['user_id']) click_pack_user_cnt,
    COUNT(1) click_pack_cnt
FROM sharemax_dw.action_log t
WHERE t.logdate BETWEEN date_sub('2019-09-02',1) AND '2019-09-04'
  AND to_date(from_unixtime(unix_timestamp(regexp_replace(substr(t.`timestamp`,1,19),'T',' ')) + 19800)) BETWEEN '2019-09-02' AND '2019-09-04'
  AND t.data['type'] IN('gift_dialog_click_tab')
  AND t.data['to']='baggage'
  AND t.app_v>='1.4.3'
GROUP BY to_date(from_unixtime(unix_timestamp(regexp_replace(substr(t.`timestamp`,1,19),'T',' ')) + 19800))


issue#4988
批量查询一批电话号码, 对应的的用户id及其他信息 (假定都是电话号码注册)
需要的字段有:
电话号码
用户id
用户名
房间display id
房间名
profile分享短链接url（没有）
头像url
设备id

批量查询一批 profile分享链接, 对应的的用户id及其他信息

SELECT u.user_id,
		u.name user_name,
		u.phone,
		get_json_object(u.avatar,'$.origin') profile_picture_url,
		c.display_id,
		c.name room_name,
		d.account_id
FROM user_center.funshare_user_accounts u
LEFT JOIN dm.user_authorizations d ON u.user_id=d.user_id AND d.app='funshare'
LEFT JOIN sharemax_dw.chatroom_info c ON u.user_id=c.user_id
WHERE u.user_id IN(10000504,10000690)
OR u.phone IN('918129002675','8618514261122')


issue#4991
可以通过用户id 跑一下内容
房间display id
近三天内所在房间活跃的总时长（有host在线的总时长）
房间里产生的总魅力值
SELECT room_info.user_id room_owner,
		room_info.display_id,
		room_info.room_id,
		online.active_duration,
		g.contribution
FROM
	(SELECT c.user_id,
			c.display_id,
			c.id room_id
	FROM sharemax_dw.chatroom_info c
	WHERE c.user_id IN(20549951)) room_info
	LEFT JOIN
	(SELECT t.room_id,
	       COUNT(1)*10/60/60 active_duration --单位：hour
	FROM sharemax_dw.chatroom_online_duration t
	WHERE t.logdate BETWEEN date_sub('2019-09-03',1) AND '2019-09-05'
	  AND t.role='host'
	  AND to_date(from_unixtime(unix_timestamp(t.`time`)+19800)) BETWEEN '2019-09-03' AND '2019-09-05'
	GROUP BY t.room_id) online
	ON room_info.room_id=online.room_id
	LEFT JOIN
	(SELECT t.room_id,
			SUM(t.gift_contribution) contribution
	FROM sharemax_dw.reward_log t
	WHERE t.logdate BETWEEN date_sub('2019-09-03',1) AND '2019-09-05'
	  AND to_date(from_unixtime(unix_timestamp(t.`time`)+19800)) BETWEEN '2019-09-03' AND '2019-09-05'
	  AND t.reward_info_id<>0
	GROUP BY t.room_id) g
	ON room_info.room_id=g.room_id



SELECT user_id,
		price,
		cash_type,
		sku_iap_id,
		gateway,
		discount,
		coin,
		type,
		time utc_time,
		from_unixtime(unix_timestamp(t.`time`)+10800) middle_east_time,
		to_date(from_unixtime(unix_timestamp(t.`time`)+10800)) middle_east_date,
		get_json_object(escrow_origin['data'],'$.orderId') order_id
FROM iap.transaction t
WHERE t.logdate BETWEEN date_sub('2019-08-01',1) AND '2019-08-31'
AND to_date(from_unixtime(unix_timestamp(t.`time`)+10800)) BETWEEN '2019-08-01' AND '2019-08-31'
AND t.app='wangcai'
AND t.status=1
AND t.type='recharge'
AND get_json_object(t.escrow_origin['data'],'$.orderId') IN('GPA.3399-4490-9081-64586',
															'GPA.3326-0503-8548-14820',
															'GPA.3396-8678-3507-49195',
															'GPA.3372-9334-0536-32499',
															'GPA.3385-6918-6834-36851',
															'GPA.3364-2786-3262-89417',
															'GPA.3374-6964-9858-14922',
															'GPA.3386-1046-6643-33540',
															'GPA.3354-2210-4703-59662',
															'GPA.3357-1869-0467-55030',
															'GPA.3343-1698-9861-89435',
															'GPA.3335-8018-0970-08047',
															'GPA.3335-2564-9116-44098',
															'GPA.3361-4693-7274-08635')


SELECT to_date(from_unixtime(substr(t.event_timestamp,1,10)+10800)) middle_east_date,
		from_unixtime(substr(t.event_timestamp,1,10)+10800) middle_east_time,
		user_id,
		t.event_params.amount recharge_price
FROM wangcai_dw.wangcai_app_action_log_from_firebase t
WHERE t.logdate BETWEEN '2019-08-01' AND '2019-08-31'
AND t.event_name ='recharge_success'


issue#4994
跑一下指定用户的个人信息 包括电话号码 性别 年龄

SELECT u.user_id,
		u.name,
		u.phone,
		u.gender,
		u.age
FROM user_center.funshare_user_accounts u
WHERE u.user_id IN(23469383,23624645,18831707,23803451,24053963,22419908,
					22882341,18020429,23575560,23582124,23092047,18591077,23946824)


issue#5009
需要sql帮忙统计语音匹配中新用户以下字段：
统计维度：在语音匹配中有滑动卡片行为的 男性 新用户
统计字段：日期，滑动卡片数，滑动卡片人数，like次数，like人数
SELECT to_date(from_unixtime(t.`timestamp`+19800)) as logdate,
       sum(CASE
               WHEN t.action IN ('like','dislike') THEN 1
               ELSE 0
           END) AS sliding_cnt,
       count(DISTINCT CASE
                          WHEN t.action IN ('like','dislike') THEN t.user_id
                      END) AS sliding_user_cnt,
       sum(CASE
               WHEN t.action IN ('like') THEN 1
               ELSE 0
           END) AS like_cnt,
       count(DISTINCT CASE
                          WHEN t.action IN ('like') THEN t.user_id
                      END) AS like_user_cnt
FROM sharemax_dw.match_user_action t
INNER JOIN
  (SELECT t.logdate,
          t.user_id
   FROM
     (SELECT DISTINCT t.logdate,
                      t.user_id
      FROM sharemax_dw.funshare_firebase t
      WHERE t.logdate BETWEEN date_sub('2019-09-10',6) AND '2019-09-10'
        AND t.event_name = 'click_voice_match'
        AND to_date(from_unixtime(cast(t.user_first_touch_timestamp /1000/1000 AS int)+19800)) = t.logdate) t
   INNER JOIN user_center.funshare_user_accounts t1
   ON t.user_id = t1.user_id AND t1.gender = 'boy') t1
   ON t.user_id = t1.user_id
   AND to_date(from_unixtime(t.`timestamp`+19800)) = t1.logdate
WHERE t.logdate BETWEEN date_sub('2019-09-10',7) AND '2019-09-10'
  AND to_date(from_unixtime(t.`timestamp`+19800)) BETWEEN date_sub('2019-09-10',6) AND '2019-09-10'
GROUP BY to_date(from_unixtime(t.`timestamp`+19800))


被滑动的top用户及相关信息
SELECT t.logdate logdate,
		t.t_id user_id,
		u.gender user_gender,
		t.cnt user_refreshed_cnt,
		t1.id card_id,
		t1.url card_url,
		t1.audio_tpl_id audio_tpl_id,
		ft.tag_id tag_id,
		tg.title tag_title
FROM
  (SELECT t_id,
          to_date(from_unixtime(t.`timestamp`+19800)) AS logdate,
          count(1) cnt
   FROM sharemax_dw.match_user_action t LATERAL VIEW explode(t.target_ids) abc AS t_id
   WHERE t.logdate BETWEEN '2019-09-10' AND '2019-09-11'
     AND to_date(from_unixtime(t.`timestamp`+19800)) = '2019-09-11'
     AND t.action = 'refresh'
   GROUP BY t_id,
   			to_date(from_unixtime(t.`timestamp`+19800))
   ORDER BY cnt DESC) t
LEFT JOIN
  (SELECT *
   FROM
     (SELECT t.*,
             row_number() over(partition BY t.user_id
                               ORDER BY t.created_at DESC) rn
      FROM sharemax.audio_card t
      WHERE t.india_date <= '2019-09-11') t
   WHERE t.rn = 1) t1 ON t.t_id = t1.user_id
LEFT JOIN sharemax.feed_audio_template ft ON t1.audio_tpl_id=ft.id
LEFT JOIN sharemax_dw.tag tg ON ft.tag_id=tg.id
LEFT JOIN user_center.funshare_user_accounts u ON t.t_id=u.user_id




issue#5021
统计广告获取用户的充值情况。由于当前充值用户量较少可手动跑数据。
每周跑以下数据
device ID, JoinDate, chargeDate, numberOfcharge（当天充值总次数）, chargeValue(当天充值总金额)

麻烦同时补充FB广告充值。可另写SQL或合并成一个
FB Ad set命名如下
fs_Hi*Install_Hindi*States_Make*Friends-Single*Image-Uncapped-Linkless-0920-AA
其中 fs_ 为固定值，目前只有Hi，后续会有Te

WITH charge_tmp AS
(SELECT t.logdate charge_date,
		t.user_id user_id,
		to_date(from_unixtime(cast(substr(t.user_first_touch_timestamp,1,10) AS int)+19800)) join_date,
		t.traffic_source.name lang,
		CAST(regexp_replace(t.event_param.value.string_value,'Rs. ','') AS int) charge_value
FROM sharemax_dw.funshare_firebase t LATERAL VIEW explode(t.event_params) t AS event_param
WHERE t.logdate BETWEEN '2019-09-16' AND '2019-09-23'
  AND t.event_name='recharge_success'
  AND t.event_param.key='amount')

SELECT t1.charge_date charge_date,
		'Googleadwords' media_source, --google
		t1.user_id user_id,
		t1.join_date join_date,
		t1.lang lang,
		COUNT(1) charge_cnt,
		SUM(t1.charge_value) ttl_charge_value
FROM charge_tmp t1
WHERE (t1.lang LIKE 'Hindi%'
  		OR t1.lang LIKE 'Tamil%'
  		OR t1.lang LIKE 'Telugu%')
GROUP BY t1.charge_date,
		t1.user_id,
		t1.join_date,
		t1.lang

UNION ALL

SELECT t2.charge_date charge_date,
		'FaceBook' media_source, --Facebook
		t2.user_id user_id,
		apps.join_date join_date,
		apps.lang lang,
		COUNT(1) charge_cnt,
		SUM(t2.charge_value) ttl_charge_value
FROM
	(SELECT u.user_id,
	        u.account_id
	FROM
	   (SELECT t.user_id,
	           t.account_id,
	           row_number() over(PARTITION BY t.account_id
	                             ORDER BY t.created_at DESC) rn
	    FROM dm.user_authorizations AS t
	    JOIN user_center.funshare_user_accounts AS t1 ON t.user_id = t1.user_id
	    AND t1.user_type IN ('client')
	    WHERE t.app = 'funshare') u
	WHERE u.rn=1) AS user_author
JOIN
	(SELECT DISTINCT t.adset AS lang,
	                t.advertising_id AS advertising_id,
	                'FaceBook' AS media_source,
	                to_date(from_unixtime(unix_timestamp(t.attributed_touch_time)+19800)) join_date
	FROM sharemax_dw.appsflyer t
	WHERE t.logdate>='2019-09-20'
	  AND t.adset LIKE 'fs_%') apps
ON user_author.account_id=apps.advertising_id
JOIN charge_tmp t2 ON user_author.user_id=t2.user_id
GROUP BY t2.charge_date,
		t2.user_id,
		apps.join_date,
		apps.lang


issue#5039
wangcai

统计用户的充值情况以及来源渠道。由于当前充值用户量较少，可手动跑数据。

每周跑以下数据
User ID (取设备ID)
Join date,
charge date, number of charge（当天充值总次数）,
charge value（当天充值总金额 ）,
channel——media_source(fb gp)
adset / campaign name （对应的是FB和Google）,like 'Talla%'
Country
Device Brand
需从 8.2 开始回溯数据

Facebook渠道+Googleadwords渠道

version 1.0
SELECT rs.logdate charge_date,
		rs.advertising_id advertising_id,
		to_date(from_unixtime(CAST(substr(new.first_touch_timestamp,1,10) AS int)+10800)) join_date,
		new.media_source media_source,
		new.adset adset,
		new.country country,
		new.brand brand,
		rs.charge_cnt charge_cnt,
		rs.charge_value charge_value
FROM
	(SELECT t.logdate,
			t.advertising_id advertising_id,
			COUNT(1) charge_cnt,
			SUM(t.event_params['amount']) charge_value
	FROM wangcai_dw.wangcai_app_action_log_from_firebase t
	WHERE t.logdate BETWEEN '2019-08-01' AND '2019-10-20'
	  AND t.event_name='recharge_success'
	GROUP BY t.logdate,
			t.advertising_id) rs
	JOIN wangcai_dm.wangcai_new_device new
	ON rs.advertising_id=new.advertising_id
WHERE new.adset LIKE 'Talla%'


version 2.0
【添加user_id】

SELECT rs.logdate charge_date,
		rs.advertising_id advertising_id,
		rs.user_id user_id,
		to_date(from_unixtime(CAST(substr(new.first_touch_timestamp,1,10) AS int)+10800)) join_date,
		'Facebook' media_source,
		new.adset adset,
		new.country country,
		new.brand brand,
		rs.charge_cnt charge_cnt,
		rs.charge_value charge_value
FROM
	(SELECT t.logdate,
			t.advertising_id advertising_id,
			t.user_id user_id,
			COUNT(1) charge_cnt,
			SUM(t.event_params['amount']) charge_value
	FROM wangcai_dw.wangcai_app_action_log_from_firebase t
	WHERE t.logdate BETWEEN '2019-08-01' AND '2019-10-20'
	  AND t.event_name='recharge_success'
	GROUP BY t.logdate,
			t.advertising_id,
			t.user_id) rs
JOIN wangcai_dm.wangcai_new_device new
ON rs.advertising_id=new.advertising_id
WHERE new.adset LIKE 'Talla%' --wangcai 广告adset
  --AND new.media_source='Facebook Ads'

UNION ALL

SELECT t.logdate charge_date,
		t.advertising_id advertising_id,
		t.user_id user_id,
		to_date(from_unixtime(CAST(substr(t.user_first_touch_timestamp,1,10) AS int)+10800)) join_date,
		'Googleadwords' media_source,
		t.traffic_source.name campaign id
		t.country country,
		t.mobile_brand brand,
		COUNT(1) charge_cnt,
		SUM(t.event_params['amount']) charge_value
FROM wangcai_dw.wangcai_app_action_log_from_firebase t
WHERE t.logdate BETWEEN '2019-09-23' AND '2019-09-24'
  AND t.event_name='recharge_success'
  AND t.traffic_source.name LIKE 'Talla%'
GROUP BY t.logdate,
		t.advertising_id,
		to_date(from_unixtime(CAST(substr(t.user_first_touch_timestamp,1,10) AS int)+10800)),
		t.traffic_source.name,
		t.country,
		t.mobile_brand


广告新增用户当日未进入聊天室，之后进入聊天室的情况

延迟进入聊天室用户
SELECT new.india_date india_date,
		new.lang lang,
		datediff(getin2.india_date,new.india_date) delay_day,
		COUNT(DISTINCT getin2.advertising_id) getin_user_cnt
FROM
	(SELECT t1.logdate india_date,
			t1.traffic_source.name AS lang,
			t1.device.advertising_id AS advertising_id
	FROM sharemax_dw.funshare_firebase t1 LATERAL VIEW explode(t1.event_params) abc AS event_param
	WHERE (t1.traffic_source.name LIKE 'Hindi%'
	       OR t1.traffic_source.name LIKE 'Tamil%'
	       OR t1.traffic_source.name LIKE 'Telugu%')
	  AND t1.event_name='first_open'
	  AND t1.logdate BETWEEN '2019-09-10' AND '2019-09-17'
	  AND abc.event_param.key = 'previous_first_open_count'
	  AND abc.event_param.value.int_value = 0) new
	LEFT JOIN
	(SELECT t.logdate india_date,
			t.device.advertising_id AS advertising_id
	FROM sharemax_dw.funshare_firebase t
	WHERE t.logdate BETWEEN '2019-09-10' AND '2019-09-17'
	  AND t.event_name='getin_chatroom_success') getin
	ON new.india_date=getin.india_date AND new.advertising_id=getin.advertising_id
	LEFT JOIN
	(SELECT t.logdate india_date,
			t.device.advertising_id AS advertising_id
	FROM sharemax_dw.funshare_firebase t
	WHERE t.logdate BETWEEN date_add('2019-09-10',1) AND date_add('2019-09-17',1)
	  AND t.event_name='getin_chatroom_success') getin2
	ON new.advertising_id=getin2.advertising_id
WHERE getin.advertising_id IS NULL
  AND datediff(getin2.india_date,new.india_date)>0
GROUP BY new.india_date,
		new.lang,
		datediff(getin2.india_date,new.india_date)


延迟上麦用户
SELECT new.india_date india_date,
		new.lang lang,
		datediff(queue2.india_date,new.india_date) delay_day,
		COUNT(DISTINCT queue2.advertising_id) getin_user_cnt
FROM
	(SELECT t1.logdate india_date,
			t1.traffic_source.name AS lang,
			t1.device.advertising_id AS advertising_id
	FROM sharemax_dw.funshare_firebase t1 LATERAL VIEW explode(t1.event_params) abc AS event_param
	WHERE (t1.traffic_source.name LIKE 'Hindi%'
	       OR t1.traffic_source.name LIKE 'Tamil%'
	       OR t1.traffic_source.name LIKE 'Telugu%')
	  AND t1.event_name='first_open'
	  AND t1.logdate BETWEEN '2019-09-10' AND '2019-09-17'
	  AND abc.event_param.key = 'previous_first_open_count'
	  AND abc.event_param.value.int_value = 0) new
	LEFT JOIN
	(SELECT t.logdate india_date,
			t.device.advertising_id AS advertising_id
	FROM sharemax_dw.funshare_firebase t
	WHERE t.logdate BETWEEN '2019-09-10' AND '2019-09-17'
	  AND t.event_name='chatroom_queue_success') queue
	ON new.india_date=queue.india_date AND new.advertising_id=queue.advertising_id
	LEFT JOIN
	(SELECT t.logdate india_date,
			t.device.advertising_id AS advertising_id
	FROM sharemax_dw.funshare_firebase t
	WHERE t.logdate BETWEEN date_add('2019-09-10',1) AND date_add('2019-09-17',1)
	  AND t.event_name='chatroom_queue_success') queue2
	ON new.advertising_id=queue2.advertising_id
WHERE queue.advertising_id IS NULL
  AND datediff(queue2.india_date,new.india_date)>0
GROUP BY new.india_date,
		new.lang,
		datediff(queue2.india_date,new.india_date)


WITH tmp AS
(SELECT to_date(from_unixtime(unix_timestamp(regexp_replace(substr(t.`timestamp`,1,19),'T',' ')) + 19800)) india_date,
         CASE WHEN to_date(from_unixtime(unix_timestamp(u.created_at)+19800))=to_date(from_unixtime(unix_timestamp(regexp_replace(substr(t.`timestamp`,1,19),'T',' ')) + 19800)) THEN 'new' ELSE 'old' END AS user_type,
         CASE WHEN t.data['pos']='%E0%A4%A8%E0%A4%AF%E0%A4%BE' THEN 'new'
              WHEN t.data['pos']='%E0%A4%AB%E0%A5%89%E0%A4%B2%E0%A5%8B%E0%A4%87%E0%A4%82%E0%A4%97' THEN 'following'
              WHEN t.data['pos']='%E0%A4%B2%E0%A5%8B%E0%A4%95%E0%A4%AA%E0%A5%8D%E0%A4%B0%E0%A4%BF%E0%A4%AF' THEN 'popular'
              WHEN t.data['pos']='%E0%A4%B9%E0%A4%BE%E0%A4%B2+%E0%A4%B9%E0%A5%80+%E0%A4%AE%E0%A5%87%E0%A4%82' THEN 'recently'
              ELSE t.data['pos'] END AS click_pos,
      t.data['user_id'] user_id
FROM sharemax_dw.action_log t JOIN user_center.funshare_user_accounts u ON t.data['user_id']=u.user_id
WHERE t.logdate BETWEEN date_sub('2019-09-15',2) AND '2019-09-16'
  AND to_date(from_unixtime(unix_timestamp(regexp_replace(substr(t.`timestamp`,1,19),'T',' ')) + 19800)) BETWEEN date_sub('2019-09-15',1) AND '2019-09-16'
  AND t.data['type']='click_chatroom'
  AND t.data['pos']<>''
  AND t.app_v>='1.4.2')

SELECT click_cr.india_date india_date,
       click_cr.user_type user_type,
       COALESCE(click_cr.click_pos,'total') click_pos,
       click_cr.click_user_cnt
FROM
  (SELECT t.india_date india_date,
         t.user_type user_type,
         t.click_pos click_pos,
         COUNT(DISTINCT t.user_id) click_user_cnt
  FROM tmp t
  GROUP BY t.india_date,
           t.user_type,
           t.click_pos
  GROUPING SETS((t.india_date,t.user_type),
                (t.india_date,t.user_type,t.click_pos))) click_cr
WHERE length(click_cr.click_pos)<10



issue#5027
当前大于等于5级的房间的——

房间 display id
房间名
房主id
房主名
房间的主持人数量
房间的管理员数量

togo:
SELECT ci.display_id display_id,
		ci.name room_name,
		ci.id room_id,
		e.total_exp total_exp,
		ci.user_id owner_id,
		u.name owner_name,
		size(ci.host_candidates) host_candidates_cnt,
		size(ci.admins) admins_cnt
FROM sharemax_dw.chatroom_info ci
JOIN user_center.funshare_user_accounts u ON ci.user_id=u.user_id
JOIN chatroom.chatroom_exp e ON ci.id=e.room_id AND e.total_exp>=8000

wangcai:
SELECT ci.display_id display_id,
		ci.name room_name,
		ci.id room_id,
		e.total_exp total_exp,
		ci.user_id owner_id,
		u.name owner_name,
		size(ci.host_candidates) host_candidates_cnt,
		size(ci.admins) admins_cnt
FROM wangcai_dw.wangcai_chatroom_info ci
JOIN user_center.wangcai_user_accounts u ON ci.user_id=u.user_id
JOIN chatroom.chatroom_exp e ON ci.id=e.room_id AND e.total_exp>=50000




issue#5041
点击push进去chatroom的人后续两个小时的活跃表现
push id
1568818000
推送时间（ist）
2019-09-18 T20:16:37

另外一个push
1568903945
2019-09-19 T20:08:59

要看的活跃数据:

在房间内的总停留时长 chatroom_connection

申请排麦次数 chatroom_queue
评论条数 chatroom_comment
送礼魅力值数 reward_log
follow的人数 chatroom_follow

WITH tmp AS
(SELECT t.data['user_id'] user_id,
		MAX(unix_timestamp(regexp_replace(substr(t.`timestamp`,1,19),'T',' '))) click_timestamp
FROM sharemax_dw.action_log t
WHERE t.logdate BETWEEN date_sub('2019-09-18',1) AND '2019-09-18'
  AND t.data['type']='push_click'
  AND t.data['push_id']=1568818000
GROUP BY t.data['user_id'])

SELECT u.user_id user_id,
		act.queue_cnt queue_cnt,
		act.comment_cnt comment_cnt,
		act.follow_user_cnt follow_user_cnt,
		g.ttl_gift_contribution ttl_gift_contribution,
		d.ttl_dur ttl_dur --minute
FROM tmp u
LEFT JOIN
	(SELECT click.user_id user_id,
			COUNT(CASE WHEN act1.data['type']='chatroom_queue' THEN 1 END) AS queue_cnt,
			COUNT(CASE WHEN act1.data['type']='chatroom_comment' THEN 1 END) AS comment_cnt,
			COUNT(DISTINCT CASE WHEN act1.data['type']='chatroom_follow' THEN act1.data['id'] END) follow_user_cnt
	FROM tmp click
	LEFT JOIN
		(SELECT t.data data,
				unix_timestamp(regexp_replace(substr(t.`timestamp`,1,19),'T',' ')) event_timestamp
		FROM sharemax_dw.action_log t
		WHERE t.logdate BETWEEN date_sub('2019-09-18',1) AND date_add('2019-09-18',1)
		  AND t.data['type'] IN('chatroom_queue','chatroom_comment','chatroom_follow')) act1
	ON click.user_id=act1.data['user_id']
	AND act1.event_timestamp-click.click_timestamp BETWEEN 0 AND 7200
	GROUP BY click.user_id) act
ON u.user_id=act.user_id
LEFT JOIN
	(SELECT click.user_id user_id,
			SUM(reward.gift_contribution) ttl_gift_contribution
	FROM tmp click
	LEFT JOIN
		(SELECT t.rewarder_id user_id,
				unix_timestamp(t.`time`) reward_timestamp,
				t.gift_contribution gift_contribution
		FROM sharemax_dw.reward_log t
		WHERE t.logdate BETWEEN date_sub('2019-09-18',1) AND date_add('2019-09-18',1)
		  AND t.reward_info_id<>0) reward
	ON click.user_id=reward.user_id
	AND reward.reward_timestamp-click.click_timestamp BETWEEN 0 AND 7200
	GROUP BY click.user_id) g
ON u.user_id=g.user_id
LEFT JOIN
	(SELECT click.user_id user_id,
			SUM(dur.closed_at-dur.created_at)/60 ttl_dur
	FROM tmp click
	LEFT JOIN
		(SELECT cc.uid user_id,
				cc.closed_at closed_at,
				cc.created_at created_at
		FROM sharemax_dw.chatroom_connection cc
		WHERE cc.logdate BETWEEN date_sub('2019-09-18',1) AND date_add('2019-09-18',1)) dur
	ON click.user_id=dur.user_id
	AND dur.created_at-click.click_timestamp BETWEEN 0 AND 7200
	AND dur.closed_at-click.click_timestamp BETWEEN 0 AND 7200
	GROUP BY click.user_id) d
ON u.user_id=d.user_id


issue#5044
近三天在hindi全部帖子下发表过评论的真实用户id

SELECT DISTINCT c.user_id
FROM sharemax_dw.feed_comment c JOIN sharemax_dw.feed f ON c.feed_seq_id=f.seq_id AND f.lang='hi'
WHERE c.logdate BETWEEN date_sub('2019-09-16',1) AND '2019-09-18'
  AND c.india_date BETWEEN '2019-09-16' AND '2019-09-18'
  AND c.source='C'


issue#5045
Free mode状态统计
SELECT smode.room_id room_id,
		smode.target_mode current_mode
FROM
	(SELECT t.data['target_mode'] target_mode,
			t.data['room_id'] room_id,
			row_number() OVER(PARTITION BY t.data['room_id'] ORDER BY t.`timestamp` DESC) rn
	FROM sharemax_dw.action_log t
	WHERE t.logdate BETWEEN date_sub('2019-09-11',1) AND '2019-09-18'
	  AND to_date(from_unixtime(unix_timestamp(regexp_replace(substr(t.`timestamp`,1,19),'T',' ')) + 19800)) BETWEEN '2019-09-11' AND '2019-09-18'
	  AND t.data['type'] IN ('switch_seat_mode')) smode
WHERE smode.rn=1

wangcai:
SELECT smode.room_id room_id,
		smode.target_mode current_mode
FROM
	(SELECT t.event_params['room_id'] room_id,
			t.event_params['target_mode'] target_mode,
			row_number() OVER(PARTITION BY t.event_params['room_id'] ORDER BY t.event_timestamp DESC) rn
	FROM wangcai_dw.wangcai_app_action_log_from_firebase t
	WHERE t.logdate>='2019-09-10'
	  AND t.event_name='switch_seat_mode') smode
WHERE smode.rn=1


issue#5034
9.12-9.17 对指定tag的feed进行了消费（点赞or评论or分享or下载）但没有在指定tag下发帖的用户ID
tag id:
hi: 5d79b36411c9b7000bfb0202
ta: 5d79b39cb5bdde000bba088d
te: 5d79c1a511c9b7000bfb0203

需要数据：
用户名
用户ID
用户语言

SELECT DISTINCT t.uid user_id,
		u.name name,
		u.current_lang current_lang
FROM sharemax_dw.user_action t
JOIN sharemax_dw.feed f
 ON t.feed_id=f.id
 AND f.tag_id IN('5d79b36411c9b7000bfb0202',
 				'5d79b39cb5bdde000bba088d',
 				'5d79c1a511c9b7000bfb0203')
LEFT JOIN sharemax_dw.feed f2
	  ON f2.india_date BETWEEN '2019-09-12' AND '2019-09-17'
	  AND t.uid=f2.user_id
	  AND f2.tag_id IN('5d79b36411c9b7000bfb0202',
	  				'5d79b39cb5bdde000bba088d',
	  				'5d79c1a511c9b7000bfb0203')
JOIN user_center.funshare_user_accounts u ON t.uid=u.user_id
WHERE t.logdate BETWEEN date_sub('2019-09-12',1) AND '2019-09-17'
  AND to_date(from_unixtime(unix_timestamp(t.`time`)+19800)) BETWEEN '2019-09-12' AND '2019-09-17'
  AND t.action_type IN('like','comment','share','fav','dl')
  AND f2.user_id IS NULL

















