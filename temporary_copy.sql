/*
nearby下的，49:221,49:222,49:223,49:224四个组
语言=hindi，
用户=new（新用户定义需确认）
feed type不限，
日期0507-0509，各个组内各个feed的刷新量，点赞量
*/

SELECT a.india_date india_date,
        a.exp_tag exp_tag,
        a.feed_seq_id feed_seq_id,
        COUNT(DISTINCT concat(a.refresh_id,a.feed_seq_id)) feed_refresh_cnt,
        COUNT(DISTINCT CASE WHEN e.action_type='like' THEN action_id ELSE NULL END) like_cnt,
        COUNT(DISTINCT CASE WHEN e.action_type='share' THEN action_id ELSE NULL END) share_cnt,
        COUNT(DISTINCT CASE WHEN e.action_type IN('fav','save') THEN action_id ELSE NULL END) fav_cnt
FROM
        (SELECT a.india_date india_date,
        	   a.refresh_id refresh_id,
        	   a.exp_tag exp_tag,
        	   a.uid uid,
        	   a.feed_seq_id feed_seq_id
        FROM
        		(SELECT to_date(from_unixtime(unix_timestamp(t.time)+19800)) india_date,
		                      t.refresh_id refresh_id,
		                      t.exp_tag exp_tag,
		                      t.uid uid,
		                      t.feed_ids feed_ids
		        FROM sharemax_dw.refresh t LATERAL VIEW explode(t.experiment) t AS exp_tag
		        WHERE t.refresh_id IS NOT NULL
		        AND t.logdate BETWEEN date_sub('2019-05-07',1) AND '2019-05-09'
		        AND to_date(from_unixtime(unix_timestamp(t.time)+19800)) BETWEEN '2019-05-07' AND '2019-05-09'
		        AND t.source='nearby'
		        AND t.exp_tag IN('49:221','49:222','49:223','49:224')) a LATERAL VIEW explode(a.feed_ids) a AS feed_seq_id) a
		LEFT JOIN sharemax_dw.feed c
		ON a.feed_seq_id=c.seq_id AND c.lang='hi'--限定语言是hindi
		JOIN user_center.funshare_user_accounts d
		ON d.user_id= a.uid AND to_date(from_unixtime(unix_timestamp(d.created_at)+19800))=a.india_date --限定新用户
		LEFT JOIN
				(SELECT * FROM sharemax_dw.user_action e
				                WHERE e.logdate BETWEEN date_sub('2019-05-07',1) AND '2019-05-09'
				                AND to_date(from_unixtime(unix_timestamp(e.time)+19800)) BETWEEN '2019-05-07' AND '2019-05-09') e
		ON a.refresh_id=e.refresh_id AND a.india_date=to_date(from_unixtime(unix_timestamp(e.time)+19800)) AND a.feed_seq_id = e.feed_seq_id AND d.user_id=e.uid
GROUP BY a.india_date,
		 a.exp_tag,
		 a.feed_seq_id


/*
想看下用户是否在Nearby下点击其他用户头像，进入其他用户主页查看并刷新的次数增加了。

需要日期（5月10-17号），限定语言为hi,
区分实验组和对照组（49:221,49:222,49:223,49:224）
区分新老用户，
Nearby下点进其他用户主页的人数，
Nearby下点进其他用户主页的次数，
从Nearby进入其他用户主页后进行的刷新总次数 ？
*/
SELECT to_date(from_unixtime(unix_timestamp(t.time)+19800)) india_date,
		t.exp_tag exp_tag,
		CASE WHEN to_date(from_unixtime(unix_timestamp(t.time)+19800))=to_date(from_unixtime(unix_timestamp(u.created_at)+19800))
			 THEN 'new' ELSE 'old'
		END AS user_type,
		COUNT(DISTINCT a.device.advertising_id) visit_user_cnt,--点进其他主页的人数
		COUNT(a.event_name) visit_times_cnt--点进其他主页的次数（次数逻辑有问题）。确认各个实验组的用户id是否会有重合
FROM
	(SELECT * FROM sharemax_dw.refresh t LATERAL VIEW explode(t.experiment) t AS exp_tag
		WHERE t.exp_tag IN('49:221','49:222','49:223','49:224')
		  AND t.lang='hi'
		  AND t.source='nearby'
		  AND t.refresh_id IS NOT NULL
		  AND t.logdate BETWEEN '2019-05-09' AND '2019-05-17'
		  AND to_date(from_unixtime(unix_timestamp(t.time)+19800)) BETWEEN '2019-05-10' AND '2019-05-17') t
	LEFT JOIN user_center.funshare_user_accounts u
	ON t.uid=u.user_id
	LEFT JOIN
	(SELECT to_date(from_unixtime(unix_timestamp(t.created_at)+19800)) AS user_create_time,
	        account_id,
	        user_id
	FROM
	  (SELECT t.*,
	          row_number() over(partition BY t.account_id
	                            ORDER BY t.created_at) AS rn
	   FROM dm.user_authorizations AS t
	   WHERE t.app = 'funshare') AS t
	WHERE t.logdate between '2019-05-09' and '2019-05-17'
	  AND to_date(from_unixtime(unix_timestamp(t.created_at)+19800)) BETWEEN '2019-05-10' AND '2019-05-17'
	  AND t.rn = 1) f
	ON u.user_id=f.user_id
	LEFT JOIN
	(SELECT * FROM sharemax_dw.funshare_firebase a LATERAL VIEW explode(a.event_params) a AS event_param
	WHERE a.logdate BETWEEN '2019-05-10' AND '2019-05-17'
	  AND a.event_name='visit_user_page'
	  AND a.event_param.key='from'
	  AND a.event_param.value.string_value='nearby') a
	ON a.device.advertising_id=f.account_id AND a.logdate=to_date(from_unixtime(unix_timestamp(t.time)+19800))
GROUP BY to_date(from_unixtime(unix_timestamp(t.time)+19800)),
		 t.exp_tag,
		 CASE WHEN to_date(from_unixtime(unix_timestamp(t.time)+19800))=to_date(from_unixtime(unix_timestamp(u.created_at)+19800))
			  THEN 'new' ELSE 'old' END




SELECT a.india_date india_date,
		t.exp_tag exp_tag,
		CASE WHEN a.india_date=to_date(from_unixtime(unix_timestamp(u.created_at)+19800))
			 THEN 'new' ELSE 'old'
		END AS user_type,
		COUNT(DISTINCT a.device_id) visit_user_cnt,
		SUM(a.visit_times_cnt) visit_times_cnt
FROM
	(SELECT a.logdate india_date,
			a.device.advertising_id device_id,
			COUNT(1) visit_times_cnt --点进其他用户主页的次数
	FROM
		(SELECT * FROM sharemax_dw.funshare_firebase a LATERAL VIEW explode(a.event_params) a AS event_param
			WHERE a.logdate BETWEEN '2019-05-10' AND '2019-05-17'
			  AND a.event_name='visit_user_page'
			  AND a.event_param.key='from'
			  AND a.event_param.value.string_value='nearby') a
	GROUP BY a.device.advertising_id,
			 a.logdate) a
	LEFT JOIN
	(SELECT to_date(from_unixtime(unix_timestamp(t.created_at)+19800)) AS user_create_time,
		        account_id,
		        user_id
		FROM
		  (SELECT t.*,
		          row_number() over(partition BY t.account_id
		                            ORDER BY t.created_at) AS rn
		   FROM dm.user_authorizations AS t
		   WHERE t.app = 'funshare') AS t
		WHERE t.logdate between '2019-05-09' and '2019-05-17'
		  AND to_date(from_unixtime(unix_timestamp(t.created_at)+19800)) BETWEEN '2019-05-10' AND '2019-05-17'
		  AND t.rn = 1) f
	ON a.device_id=f.account_id
	LEFT JOIN user_center.funshare_user_accounts u
	ON f.user_id=u.user_id
	LEFT JOIN
	(SELECT * FROM sharemax_dw.refresh t LATERAL VIEW explode(t.experiment) t AS exp_tag
			WHERE t.exp_tag IN('49:221','49:222','49:223','49:224')
			  AND t.lang='hi'
			  AND t.source='nearby'
			  AND t.logdate ='2019-05-17'
			  AND to_date(from_unixtime(unix_timestamp(t.time)+19800)) BETWEEN '2019-05-16' AND '2019-05-17') t
	ON u.user_id=t.uid
WHERE t.exp_tag IS NOT NULL
GROUP BY a.india_date,
		 t.exp_tag,
		 CASE WHEN a.india_date=to_date(from_unixtime(unix_timestamp(u.created_at)+19800))
			 THEN 'new' ELSE 'old' END


-- >5min的视频的整体表现：内容量占比，分享率，下载率，点赞率
-- 时段：过去一周
-->5min视频量
SELECT COUNT(DISTINCT b.feed_seq_id) video_cnt_5min,-->5min的视频量
	 	COUNT(DISTINCT concat(c.refresh_id,b.feed_seq_id)) refresh_cnt_5min,-->5min视频刷新量
	 	COUNT(DISTINCT CASE WHEN d.action_type='share' THEN d.action_id ELSE NULL END) share_cnt_5min,-->5min视频分享量
	 	COUNT(DISTINCT CASE WHEN d.action_type IN('fav','save') THEN d.action_id ELSE NULL END) fav_cnt_5min,
	 	COUNT(DISTINCT CASE WHEN d.action_type='like' THEN d.action_id ELSE NULL END) like_cnt_5min
FROM
	(SELECT DISTINCT a.event_param2.value.int_value feed_seq_id,
		   a.total_time total_time
	FROM
		(SELECT
		    *,
		    t.event_param.value.int_value total_time
		FROM sharemax_dw.funshare_firebase_video_play_time t lateral view explode(t.event_params) t as event_param
		WHERE t.event_param.KEY='total_time'
		  AND t.logdate BETWEEN '2019-05-18' AND '2019-05-26') a lateral view explode(a.event_params) a as event_param2
	WHERE a.event_param2.KEY='feed_seq_id'
	  AND a.total_time>300) b
	LEFT JOIN
	(SELECT *
	FROM sharemax_dw.refresh c LATERAL VIEW explode(c.feed_ids) c AS feed_seq_id
	WHERE c.logdate BETWEEN '2019-05-18' AND '2019-05-26'
	  AND to_date(from_unixtime(unix_timestamp(c.time)+19800)) BETWEEN '2019-05-19' AND '2019-05-26') c
	ON b.feed_seq_id= c.feed_seq_id
	LEFT JOIN
	(SELECT *
	FROM sharemax_dw.user_action d
	WHERE d.logdate BETWEEN '2019-05-18' AND '2019-05-26'
	  AND to_date(from_unixtime(unix_timestamp(d.time)+19800)) BETWEEN '2019-05-19' AND '2019-05-26') d
	ON c.refresh_id=d.refresh_id AND c.feed_seq_id=d.feed_seq_id


--issue#4059  近一周视频总量数据
SELECT COUNT(DISTINCT f.seq_id) video_cnt,--近一周被展示视频总量
		COUNT(DISTINCT concat(c.refresh_id,f.seq_id)) refresh_cnt,
		COUNT(DISTINCT CASE WHEN d.action_type='share' THEN d.action_id ELSE NULL END) share_cnt,
	 	COUNT(DISTINCT CASE WHEN d.action_type IN('fav','save') THEN d.action_id ELSE NULL END) fav_cnt,
	 	COUNT(DISTINCT CASE WHEN d.action_type='like' THEN d.action_id ELSE NULL END) like_cnt,
	 	COUNT(DISTINCT CASE WHEN duration>300 THEN f.seq_id ELSE NULL END) video_cnt_5min,--近一周被展示视频>5min的视频量
	 	COUNT(DISTINCT CASE WHEN duration>300 THEN concat(c.refresh_id,f.seq_id) ELSE NULL END) refresh_cnt_5min,
	 	COUNT(DISTINCT CASE WHEN duration>300 AND d.action_type='share' THEN d.action_id ELSE NULL END) share_cnt_5min,
	 	COUNT(DISTINCT CASE WHEN duration>300 AND d.action_type IN('fav','save') THEN d.action_id ELSE NULL END) fav_cnt_5min,
	 	COUNT(DISTINCT CASE WHEN duration>300 AND d.action_type='like' THEN d.action_id ELSE NULL END) like_cnt_5min
FROM
	(SELECT *
	FROM sharemax_dw.refresh c LATERAL VIEW explode(c.feed_ids) c AS feed_seq_id
	WHERE c.logdate BETWEEN '2019-05-18' AND '2019-05-26'
	  AND to_date(from_unixtime(unix_timestamp(c.time)+19800)) BETWEEN '2019-05-19' AND '2019-05-26') c
	LEFT JOIN
	(SELECT f.*,
			--round(get_json_object(lower(regexp_replace(f.video,"u'|'","\\\"")),'$.duration')) duration
	FROM sharemax_dw.feed f
	WHERE f.feed_type='video') f
	ON f.seq_id=c.feed_seq_id
	LEFT JOIN
	(SELECT *
	FROM sharemax_dw.user_action d
	WHERE d.logdate BETWEEN '2019-05-18' AND '2019-05-26'
	  AND to_date(from_unixtime(unix_timestamp(d.time)+19800)) BETWEEN '2019-05-19' AND '2019-05-26') d
	ON c.refresh_id=d.refresh_id AND c.feed_seq_id=d.feed_seq_id



--分语言查看重复安装设备占比
--27号新增设备中重复安装设备 分余语言/渠道占比

SELECT u_lang.lang lang,
      count(DISTINCT case when (traffic_source.name like 'Te%' or traffic_source.name like 'Ta%' or traffic_source.name like 'Hi%')
      					   and t.int_value>0 then t.aid end)/count(DISTINCT case when (traffic_source.name like 'Te%' or traffic_source.name like 'Ta%' or traffic_source.name like 'Hi%') then t.aid end) toufang,--投放安装重复占比
      (count(distinct case when t.int_value>0 then t.aid end)-count(DISTINCT case when (traffic_source.name like 'Te%' or traffic_source.name like 'Ta%' or traffic_source.name like 'Hi%') and t.int_value>0 then t.aid end))/(count(distinct t.aid)-count(DISTINCT case when (traffic_source.name like 'Te%' or traffic_source.name like 'Ta%' or traffic_source.name like 'Hi%') then t.aid end)) nature,--自然
       count(distinct case when t.int_value>0 then t.aid end)/count(DISTINCT t.aid)
from
	(select distinct device.advertising_id aid ,
			event_param.value.int_value int_value, --设备重复安装次数
			traffic_source
	from sharemax_dw.funshare_firebase_first_open t LATERAL VIEW explode(t.event_params) abc AS event_param
	where logdate='2019-05-27'
	  and event_param.key = 'previous_first_open_count'
	)t
join
	(SELECT to_date(from_unixtime(unix_timestamp(t.created_at)+19800)) AS user_create_time,
	             account_id,
	             user_id
	      FROM
	        (SELECT t.*,
	                row_number() over(partition BY t.account_id
	                                  ORDER BY t.created_at) AS rn
	         FROM dm.user_authorizations AS t
	         INNER JOIN user_center.funshare_user_accounts AS t1
	         ON t.user_id = t1.user_id
	         AND t1.user_type IN ('client')
	         WHERE t.app = 'funshare') AS t --togo
	      WHERE t.logdate <= '2019-05-27'
	        AND to_date(from_unixtime(unix_timestamp(t.created_at)+19800)) <= '2019-05-27'
	        AND t.rn = 1 ) AS user_author
on t.aid = user_author.account_id
left JOIN
	(SELECT *
	      FROM
	        (SELECT t.*,
	                row_number() over(partition BY t.user_id
	                                  ORDER BY t.time) AS rn
	         FROM user_center.log_lang AS t
	         WHERE t.app = 'funshare') AS t
	      WHERE t.rn = 1 ) AS u_lang --用户首次注册语言
ON user_author.user_id = u_lang.user_id
group by u_lang.lang




SELECT u_lang.lang lang,
      count(DISTINCT case when (traffic_source.name like 'Te%' or traffic_source.name like 'Ta%' or traffic_source.name like 'Hi%')
      					   and t.int_value>0 then t.aid end) toufang_repeat,--投放重复安装量
      count(DISTINCT case when (traffic_source.name like 'Te%' or traffic_source.name like 'Ta%' or traffic_source.name like 'Hi%') then t.aid end) toufang,--投放安装总量
      count(distinct case when t.int_value>0 then t.aid end) repeat_cnt, --重复安装总量
      count(distinct t.aid) install_cnt--安装总量
from
	(select distinct device.advertising_id aid ,
			event_param.value.int_value int_value, --设备重复安装次数
			traffic_source
	from sharemax_dw.funshare_firebase_first_open t LATERAL VIEW explode(t.event_params) abc AS event_param
	where logdate='2019-05-28'
	  and event_param.key = 'previous_first_open_count'
	)t
join
	(SELECT to_date(from_unixtime(unix_timestamp(t.created_at)+19800)) AS user_create_time,
	             account_id,
	             user_id
	      FROM
	        (SELECT t.*,
	                row_number() over(partition BY t.account_id
	                                  ORDER BY t.created_at) AS rn
	         FROM dm.user_authorizations AS t
	         INNER JOIN user_center.funshare_user_accounts AS t1
	         ON t.user_id = t1.user_id
	         AND t1.user_type IN ('client')
	         WHERE t.app = 'funshare') AS t --togo
	      WHERE t.logdate <= '2019-05-28'
	        AND to_date(from_unixtime(unix_timestamp(t.created_at)+19800)) <= '2019-05-28'
	        AND t.rn = 1 ) AS user_author
on t.aid = user_author.account_id
left JOIN
	(SELECT *
	      FROM
	        (SELECT t.*,
	                row_number() over(partition BY t.user_id
	                                  ORDER BY t.time) AS rn
	         FROM user_center.log_lang AS t
	         WHERE t.app = 'funshare') AS t
	      WHERE t.rn = 1 ) AS u_lang --用户首次注册语言
ON user_author.user_id = u_lang.user_id
group by u_lang.lang



-- 用户：过去一周（0522-0528）重复安装的女性用户
-- 数据：
-- ①用户id，最后一次卸载的时间 [过去一周历次安装、卸载时间]，重复安装的总次数，
--最后一次卸载【当天】的用户的主要事件
--最后一次卸载时用户的机型
-- ②用户的基本信息：邦，城市
-- 【③每次安装后是否有登陆(有刷新)】


--过去一周(0522-0528)有重复安装行为的女性用户
SELECT f2.user_id user_id,
		f1.aid aid,
		f3.gender gender,
		f1.int_value repeat_times,
		f4.mobile_brand_name mobile_brand_name,
		f4.mobile_model_name mobile_model_name,
		f4.last_uninstall_time last_uninstall_time,
		f5.uninstall_time_set uninstall_time_set,
		f6.install_time_set install_time_set,
		f7.like_cnt like_cnt,
		f7.share_cnt share_cnt,
		f7.fav_cnt fav_cnt,
		f8.refresh_cnt refresh_cnt
FROM
	(SELECT f1.aid aid,
			f1.int_value int_value
	FROM
		(SELECT f1.*,
				row_number() over(partition BY f1.aid ORDER BY f1.logdate DESC) rn
		FROM
			(SELECT DISTINCT device.advertising_id aid,--设备id
					logdate,
					event_param.value.int_value int_value--设备重复安装次数
			   FROM sharemax_dw.funshare_firebase_first_open t LATERAL VIEW explode(t.event_params) abc AS event_param
			   WHERE logdate BETWEEN '2019-05-22' AND '2019-05-28'
				 AND event_param.key = 'previous_first_open_count'
				 AND event_param.value.int_value>0) f1) f1
	WHERE f1.rn=1) f1
	LEFT JOIN
	(SELECT to_date(from_unixtime(unix_timestamp(t.created_at)+19800)) AS user_create_time,
	        account_id,
	        user_id
		FROM
		  (SELECT t.*,
		          row_number() over(partition BY t.account_id ORDER BY t.created_at DESC) AS rn
		   FROM dm.user_authorizations AS t
		   WHERE t.app = 'funshare') AS t
		WHERE t.rn = 1) f2
	ON f1.aid=f2.account_id
	LEFT JOIN user_center.funshare_user_accounts f3
	ON f2.user_id=f3.user_id
	LEFT JOIN
	(--最后一次卸载的时间
	SELECT t.aid aid,
			t.device.mobile_brand_name mobile_brand_name,--最后一次卸载时的手机品牌
			t.device.mobile_model_name mobile_model_name,--最后一次卸载的手机型号
			to_date(from_unixtime(CAST(substr(t.event_timestamp,1,10) AS bigint)+19800)) last_uninstall_time--最后一次卸载时间
	FROM
		(SELECT t.*,
				row_number() over(partition BY t.aid ORDER BY event_timestamp DESC) rn
		FROM
			(SELECT t.*,
					device.advertising_id aid
				FROM sharemax_dw.funshare_firebase_app_remove t
				WHERE t.logdate BETWEEN '2019-05-22' AND '2019-05-28') t) t
	WHERE t.rn=1)f4
	ON f1.aid=f4.aid
	LEFT JOIN
	(-- 过去一周设备历次卸载时间
	SELECT t.aid aid,
          collect_set(from_unixtime(CAST(substr(t.event_timestamp,1,10) AS bigint)+19800)) uninstall_time_set --历次卸载时间
   FROM
     (SELECT DISTINCT device.advertising_id aid,
     		t.event_timestamp
      FROM sharemax_dw.funshare_firebase_app_remove t
      WHERE t.logdate BETWEEN '2019-05-22' AND '2019-05-28'
      ORDER BY t.event_timestamp DESC) t
   GROUP BY t.aid) f5
	ON f1.aid=f5.aid
	LEFT JOIN
	(-- 过去一周设备历次安装时间
		SELECT t.aid aid,
          collect_set(from_unixtime(CAST(substr(t.user_first_touch_timestamp,1,10) AS bigint)+19800)) install_time_set --历次安装时间
   FROM
     (SELECT DISTINCT t.device.advertising_id aid,
                      t.user_first_touch_timestamp,
                      t.logdate
      FROM sharemax_dw.funshare_firebase_first_open t
      WHERE t.logdate BETWEEN '2019-05-22' AND '2019-05-28'
      ORDER BY t.user_first_touch_timestamp DESC) t
   GROUP BY t.aid) f6
	ON f1.aid=f6.aid
	LEFT JOIN
	(--最后一次卸载【当天】的用户的主要事件：点赞、下载、分享、评论次数
	SELECT t.uid uid,
			to_date(from_unixtime(unix_timestamp(t.time)+19800)) action_date,
			COUNT(DISTINCT CASE WHEN t.action_type='like' THEN t.action_id ELSE NULL END) like_cnt,
			COUNT(DISTINCT CASE WHEN t.action_type='share' THEN t.action_id ELSE NULL END) share_cnt,
			COUNT(DISTINCT CASE WHEN t.action_type IN('fav','save') THEN t.action_id ELSE NULL END) fav_cnt
	FROM sharemax.user_action t
	WHERE t.logdate BETWEEN '2019-05-21' AND '2019-05-28'
	  AND to_date(from_unixtime(unix_timestamp(t.time)+19800)) BETWEEN '2019-05-22' AND '2019-05-28'
	GROUP BY t.uid,
			 to_date(from_unixtime(unix_timestamp(t.time)+19800))
		)f7
	ON f2.user_id=f7.uid AND f4.last_uninstall_time=f7.action_date
	LEFT JOIN
	(--最后一次卸载【当天】的用户的主要事件：刷新次数
	SELECT t.uid uid,
			to_date(from_unixtime(unix_timestamp(t.time)+19800)) refresh_date,
			COUNT(DISTINCT t.refresh_id) refresh_cnt
	FROM sharemax_dw.refresh t
	WHERE t.logdate BETWEEN '2019-05-21' AND '2019-05-28'
	  AND to_date(from_unixtime(unix_timestamp(t.time)+19800)) BETWEEN '2019-05-22' AND '2019-05-28'
	GROUP BY t.uid,
			 to_date(from_unixtime(unix_timestamp(t.time)+19800))
		)f8
	ON f2.user_id=f8.uid AND f4.last_uninstall_time=f8.refresh_date
WHERE f3.gender IN('girl','F','boy','M')


-- 需要过去一周（5.22~5.28）卸载设备的设备ID,
-- 日期，小时（如23时），用户类型（新老用户），
-- APP内所选择的的语言（Hi,Ta, Te），
-- 设备品牌（如小米三星），设备型号（如Galaxy S7），是否允许推送.

SELECT f1.aid aid,
		f2.user_id user_id,
		CASE WHEN f2.user_create_time=f1.logdate THEN 'new' ELSE 'old' END AS user_type,
		f3.current_lang current_lang,
		f1.mobile_brand_name mobile_brand_name,
		f1.mobile_model_name mobile_model_name,
		f1.event_date uninstall_date,
		f1.uninstall_time uninstall_time,
		f4.is_push_enable is_push_enable
FROM
	(SELECT DISTINCT device.advertising_id aid,--设备id
			device.mobile_brand_name mobile_brand_name,--设备品牌
			device.mobile_model_name mobile_model_name,--设备型号
			event_date,--卸载日期
			hour(from_unixtime(cast(substr(event_timestamp,1,10) AS bigint)+19800)) uninstall_time,--卸载时间
			logdate
	FROM sharemax_dw.funshare_firebase_app_remove t
	WHERE t.logdate BETWEEN '2019-05-22' AND '2019-05-28') f1
	LEFT JOIN
	(SELECT to_date(from_unixtime(unix_timestamp(t.created_at)+19800)) AS user_create_time,
		        account_id,
		        user_id
		FROM
		  (SELECT t.*,
		          row_number() over(partition BY t.account_id ORDER BY t.created_at DESC) AS rn
		   FROM dm.user_authorizations AS t
		   WHERE t.app = 'funshare') AS t
		WHERE t.rn = 1) f2
	ON f1.aid=f2.account_id
	LEFT JOIN user_center.funshare_user_accounts f3 ON f2.user_id=f3.user_id
	LEFT JOIN
	(SELECT t.aid aid,
			t.is_push_enable is_push_enable
	FROM
		(SELECT t.*,
				row_number() over(partition BY t.aid ORDER BY t.logdate DESC) rn
		FROM
			(SELECT t.*,
					t.device.advertising_id aid,
					t.event_param.value.int_value is_push_enable
			FROM sharemax_dw.funshare_firebase t LATERAL VIEW explode(t.event_params) t AS event_param
			WHERE logdate BETWEEN '2019-05-22' AND '2019-05-28'
			AND event_name='push_click'
			AND event_param.KEY='push_enable') t) t
	WHERE t.rn=1) f4
	ON f1.aid=f4.aid




--issue#4521
--时间：5月31日，6月1日
-- 1、音频:展示次数、展示人数、播放次数、播放人数(feed_type=audio)
-- 2、音频的:点赞、评论、分享、下载、不喜欢总次数和总人数
-- 3、点赞、评论、分享、下载最高的10个音频的feed_id

-- 1、音频:展示次数、展示人数、播放次数、播放人数(feed_type=audio)
SELECT f1.india_date india_date,
		COUNT(DISTINCT concat(f1.refresh_id,f2.seq_id)) impression_cnt,--展示次数
		COUNT(DISTINCT CASE WHEN f2.feed_type='audio' THEN f1.uid ELSE NULL END) impression_user_cnt,--展示人数
		COUNT(f3.refresh_id) play_cnt,--播放次数
		COUNT(DISTINCT f3.aid) paly_user_cnt--播放人数
FROM
	(SELECT to_date(from_unixtime(unix_timestamp(t.time)+19800)) india_date,
			t.*
	FROM sharemax_dw.refresh t LATERAL VIEW explode(t.feed_ids) t AS feed_seq_id
	WHERE t.logdate BETWEEN '2019-05-30' AND '2019-06-01'
	AND to_date(from_unixtime(unix_timestamp(t.time)+19800)) BETWEEN '2019-05-31' AND '2019-06-01'
	AND t.refresh_id is NOT NULL)f1
	LEFT JOIN sharemax_dw.feed f2
	ON f1.feed_seq_id=f2.seq_id AND f2.feed_type='audio'
	LEFT JOIN
	(SELECT t.*,
			t.device.advertising_id aid,
			t.event_param.value.string_value refresh_id
	FROM sharemax_dw.funshare_firebase t LATERAL VIEW explode(t.event_params) t AS event_param
	WHERE t.event_name='play_audio'
	  AND t.event_param.KEY='refresh_id'
	  AND t.logdate BETWEEN '2019-05-31' AND '2019-06-01')f3
	ON f1.refresh_id=f3.refresh_id
GROUP BY f1.india_date

-- 2、音频的:点赞、评论、分享、下载、不喜欢总次数和总人数
SELECT f1.india_date india_date,
		COUNT(DISTINCT CASE WHEN f3.action_type='like' THEN f3.action_id ELSE NULL END) like_cnt,
		COUNT(DISTINCT CASE WHEN f3.action_type='like' THEN f3.uid ELSE NULL END) like_user_cnt,
		COUNT(DISTINCT CASE WHEN f3.action_type='comment' THEN f3.action_id ELSE NULL END) comment_cnt,
		COUNT(DISTINCT CASE WHEN f3.action_type='comment' THEN f3.uid ELSE NULL END) comment_user_cnt,
		COUNT(DISTINCT CASE WHEN f3.action_type='share' THEN f3.action_id ELSE NULL END) share_cnt,
		COUNT(DISTINCT CASE WHEN f3.action_type='share' THEN f3.uid ELSE NULL END) share_user_cnt,
		COUNT(DISTINCT CASE WHEN f3.action_type IN('save','fav') THEN f3.action_id ELSE NULL END) fav_cnt,
		COUNT(DISTINCT CASE WHEN f3.action_type IN('save','fav') THEN f3.uid ELSE NULL END) fav_user_cnt,
		COUNT(DISTINCT CASE WHEN f3.action_type IN('dislike','Unlike') THEN f3.action_id ELSE NULL END) dislike_cnt,
		COUNT(DISTINCT CASE WHEN f3.action_type IN('dislike','Unlike') THEN f3.uid ELSE NULL END) dislike_user_cnt
FROM
	(SELECT to_date(from_unixtime(unix_timestamp(t.time)+19800)) india_date,
			t.*
	FROM sharemax_dw.refresh t LATERAL VIEW explode(t.feed_ids) t AS feed_seq_id
	WHERE t.logdate BETWEEN '2019-05-30' AND '2019-06-01'
	AND to_date(from_unixtime(unix_timestamp(t.time)+19800)) BETWEEN '2019-05-31' AND '2019-06-01'
	AND t.refresh_id is NOT NULL)f1
	LEFT JOIN sharemax_dw.feed f2
	ON f1.feed_seq_id=f2.seq_id AND f2.feed_type='audio'
	LEFT JOIN
	(SELECT * FROM sharemax.user_action t WHERE t.logdate BETWEEN '2019-05-30' AND '2019-06-01')f3
	ON f1.refresh_id=f3.refresh_id AND f2.id=f3.feed_id
GROUP BY f1.india_date

-- 3、点赞、评论、分享、下载次数最高的10个音频的feed_id，+展现量
SELECT act.*,
		imp.impression_cnt impression_cnt
FROM
	(SELECT f.*
	FROM
		(SELECT f.*,
				row_number() over(partition BY f.india_date,f.action_type ORDER BY action_cnt DESC) rn
		FROM
			(SELECT f1.india_date india_date,
					f2.seq_id seq_id,
					f2.id feed_id,
					f3.action_type action_type,
					COUNT(DISTINCT f3.action_id) action_cnt
			FROM
				(SELECT to_date(from_unixtime(unix_timestamp(t.time)+19800)) india_date,
						t.*
				FROM sharemax_dw.refresh t LATERAL VIEW explode(t.feed_ids) t AS feed_seq_id
				WHERE t.logdate BETWEEN '2019-05-30' AND '2019-06-01'
				AND to_date(from_unixtime(unix_timestamp(t.time)+19800)) BETWEEN '2019-05-31' AND '2019-06-01'
				AND t.refresh_id is NOT NULL)f1
				LEFT JOIN sharemax_dw.feed f2
				ON f1.feed_seq_id=f2.seq_id AND f2.feed_type='audio'
				LEFT JOIN
				(SELECT * FROM sharemax.user_action t WHERE t.logdate BETWEEN '2019-05-30' AND '2019-06-01')f3
				ON f1.refresh_id=f3.refresh_id AND f2.id=f3.feed_id
			WHERE f3.action_type IN ('like','comment','share','fav')
			GROUP BY f1.india_date,
					 f3.action_type,
					 f2.seq_id,
					 f2.id)f )f
	WHERE f.rn<=10)act
	LEFT JOIN
	(SELECT  f4.india_date india_date,
			f5.seq_id seq_id,
			COUNT(DISTINCT concat(f4.refresh_id,f5.seq_id)) impression_cnt
	FROM
		(SELECT to_date(from_unixtime(unix_timestamp(t.time)+19800)) india_date,
				t.*
		FROM sharemax_dw.refresh t LATERAL VIEW explode(t.feed_ids) t AS feed_seq_id
		WHERE t.logdate BETWEEN '2019-05-30' AND '2019-06-01'
		  AND to_date(from_unixtime(unix_timestamp(t.time)+19800)) BETWEEN '2019-05-31' AND '2019-06-01'
		  AND t.refresh_id IS NOT NULL) f4
		LEFT JOIN sharemax_dw.feed f5
		ON f4.feed_seq_id=f5.seq_id AND f5.feed_type='audio'
	GROUP BY f4.india_date,
			 f5.seq_id)imp
	ON act.india_date=imp.india_date AND act.seq_id=imp.seq_id


-- issue#4518
-- 所以需要下面这些类别的老用户人数
-- 老用户没上线（没打开app）【老用户上线】
-- 老用户上线了，但没有点击party tab
-- 老用户上线并且点击了party tab进入了列表页，但是没有进入任何房间
-- 时间：0515-0531
-- chatroom新用户定义：近7天内，当天进入聊天室是这个人的首次进入，且时间与这个人新增日期相差不超过7天
SELECT 	COUNT(DISTINCT CASE WHEN install.user_type='old' THEN install.user_aid ELSE NULL END) old_cnt,
		COUNT(DISTINCT CASE WHEN install.user_type='old' AND active_1.event_name='impression' THEN active_1.user_aid ELSE NULL END) old_imp_cnt,--老用户上线
		COUNT(DISTINCT CASE WHEN install.user_type='old' AND active_1.event_name='click_chatroom_tab' THEN active_1.user_aid ELSE NULL END) old_click_tab_cnt,--点击tab
		COUNT(DISTINCT CASE WHEN install.user_type='old' AND active_1.event_name='getin_chatroom_success' THEN active_1.user_aid ELSE NULL END) old_getinroom_cnt --进入房间
FROM
	(SELECT t.device.advertising_id AS user_aid,
            CASE WHEN t1.user_aid IS NOT NULL THEN 'new' ELSE 'old' END AS user_type
   	FROM sharemax_dw.funshare_firebase_other AS t
   		 LEFT JOIN
	      (SELECT t.advertising_id user_aid
	       FROM
	         (SELECT DISTINCT t.device.advertising_id AS advertising_id,
	                 to_date(from_unixtime(cast(t.user_first_touch_timestamp/1000/1000 AS bigint) + 19800)) new_user_time,
	                 datediff(t.logdate,to_date(from_unixtime(cast(t.user_first_touch_timestamp/1000/1000 AS bigint) + 19800))) time_between,
	                 row_number() over(partition BY t.device.advertising_id
	                                   ORDER BY t.event_timestamp) rn,
	                 t.logdate logdate
	          FROM sharemax_dw.funshare_firebase_other AS t
	          WHERE t.event_name = 'getin_chatroom_success'
	            AND t.logdate BETWEEN date_sub('2019-05-14',6) AND '2019-05-14') t
	       WHERE t.logdate ='2019-05-14'
	         AND t.rn=1
	         AND t.time_between<=7) t1
	      ON t.device.advertising_id = t1.user_aid
	 WHERE t.event_name = 'getin_chatroom_success'
	   AND t.logdate = '2019-05-14') install  --0514 chatroom用户类型
	LEFT JOIN
	(SELECT t.device.advertising_id AS user_aid,
			t.*
   	FROM sharemax_dw.funshare_firebase as t
	   WHERE t.event_name IN('impression','click_chatroom_tab','getin_chatroom_success')--打开app，点击chatroom tab，进入聊天室
	     AND t.logdate ='2019-05-15' --次日留存
    )active_1 ON install.user_aid = active_1.user_aid


-- issue#4535
-- 0606-0612
-- 不同刷新来源下 所有音频的展现次数，点赞次数，下载次数，分享次数，评论次数
SELECT f1.india_date india_date,
		f1.source source,
		f2.seq_id,
		f2.id,
		f2.usable,
		COUNT(DISTINCT concat(f1.refresh_id,f2.id)) impression_cnt,
		COUNT(DISTINCT CASE WHEN f3.action_type='like' THEN f3.action_id ELSE NULL END) like_cnt,
		COUNT(DISTINCT CASE WHEN f3.action_type IN('fav','save') THEN f3.action_id ELSE NULL END) fav_cnt,
		COUNT(DISTINCT CASE WHEN f3.action_type='share' THEN f3.action_id ELSE NULL END) share_cnt,
		COUNT(DISTINCT CASE WHEN f3.action_type='comment' THEN f3.action_id ELSE NULL END) comment_cnt
FROM
	(SELECT to_date(from_unixtime(unix_timestamp(t.time)+19800)) india_date,
			t.*
	FROM sharemax_dw.refresh t LATERAL VIEW explode(t.feed_ids) t AS feed_seq_id
	WHERE t.logdate BETWEEN date_sub('2019-06-16',3) AND '2019-06-16'
	  AND t.refresh_id IS NOT NULL
	  AND to_date(from_unixtime(unix_timestamp(t.time)+19800)) BETWEEN date_sub('2019-06-16',2) AND '2019-06-16'
	  )f1
	LEFT JOIN sharemax_dw.feed f2
	ON f1.feed_seq_id=f2.seq_id AND f2.feed_type='audio'
	LEFT JOIN
	(SELECT *
	FROM sharemax_dw.user_action t
	WHERE t.logdate BETWEEN date_sub('2019-06-16',3) AND '2019-06-16')f3
	ON f1.refresh_id=f3.refresh_id AND f2.id=f3.feed_id
GROUP BY f1.india_date,
		 f1.source,
		 f2.seq_id,
		 f2.id,
		 f2.usable


--getin_chatroom_success进入聊天室匿名用户(只有女性用户可以匿名)
--click_chatroom_queue 排麦匿名
--chatroom_queue 申请上麦匿名用户
--chatroom_queue_success上麦成功匿名用户
--排除付费主播及工作人员
SELECT f6.logdate logdate,
	  COUNT(DISTINCT CASE WHEN f6.gender='girl' AND f7.aid IS NULL THEN f6.advertising_id ELSE NULL END) getin_girl_cnt,
	  COUNT(DISTINCT CASE WHEN f6.gender='girl' AND f6.is_anony=1 AND f7.aid IS NULL THEN f6.advertising_id ELSE NULL END) getin_girl_annoy_cnt,
	  COUNT(DISTINCT CASE WHEN f6.gender='girl' AND f7.aid IS NULL THEN f2.device.advertising_id ELSE NULL END) clickqueue_girl_cnt,
	  COUNT(DISTINCT CASE WHEN f6.gender='girl' AND f6.is_anony=1 AND f7.aid IS NULL THEN f2.device.advertising_id ELSE NULL END) clickqueue_girl_annoy_cnt,
	  COUNT(DISTINCT CASE WHEN f6.gender='girl' AND f7.aid IS NULL THEN f3.device.advertising_id ELSE NULL END) chatroomqueue_girl_cnt,
	  COUNT(DISTINCT CASE WHEN f6.gender='girl' AND f6.is_anony=1 AND f7.aid IS NULL THEN f3.device.advertising_id ELSE NULL END) chatroomqueue_girl_annoy_cnt,
	  COUNT(DISTINCT CASE WHEN f6.gender='girl' AND f7.aid IS NULL THEN f5.device.advertising_id ELSE NULL END) queuesuccess_girl_cnt,
	  COUNT(DISTINCT CASE WHEN f6.gender='girl' AND f6.is_anony=1 AND f7.aid IS NULL THEN f5.device.advertising_id ELSE NULL END) queuesuccess_girl_annoy_cnt
FROM
	(SELECT f4.logdate logdate,
			f4.advertising_id advertising_id,
			f4.is_anony is_anony, --0 & 1
			f4.room_id room_id,
			f4.event_param3.value.string_value gender --boy & girl
	FROM
			(SELECT f1.logdate logdate,
				f1.device.advertising_id advertising_id,
				f1.is_anony is_anony,
		        f1.event_param2.value.string_value room_id,
		        f1.event_params event_params
			FROM
				(SELECT t.*,
						event_param.value.int_value is_anony
				FROM sharemax_dw.funshare_firebase_other t LATERAL VIEW explode(t.event_params) t AS event_param
				WHERE logdate BETWEEN '2019-05-30' AND '2019-06-03'
				  AND event_name='getin_chatroom_success'
				  AND event_param.KEY='is_anonymous'
				  )f1 LATERAL VIEW explode(f1.event_params)f1 AS event_param2
			WHERE f1.event_param2.KEY='room_id'
			)f4 LATERAL VIEW explode(f4.event_params)f4 AS event_param3
	WHERE f4.event_param3.KEY='gender')f6
	LEFT JOIN
	(SELECT DISTINCT t.account_id aid
		FROM dm.user_authorizations t
		WHERE t.app = 'funshare'
	  	  AND t.user_id IN(SELECT user_id FROM tmp.tmp_chatroom_host_type WHERE host_type='fake')
	)f7
	ON f6.advertising_id=f7.aid
	LEFT JOIN
	(SELECT f2.*,
			CASE WHEN f2.event_param.KEY='room_id' THEN f2.event_param.value.string_value END AS room_id
	FROM
		(SELECT t.* FROM sharemax_dw.funshare_firebase_other t LATERAL VIEW explode(t.event_params) t AS event_param
		WHERE logdate BETWEEN '2019-05-30' AND '2019-06-03'
		 AND event_name='click_chatroom_queue')f2 )f2
	ON f6.advertising_id=f2.device.advertising_id AND f6.room_id=f2.room_id AND f6.logdate=f2.logdate
	LEFT JOIN
	(SELECT f3.*,
			CASE WHEN f3.event_param.KEY='room_id' THEN f3.event_param.value.string_value END AS room_id
	FROM
		(SELECT t.* FROM sharemax_dw.funshare_firebase_other t LATERAL VIEW explode(t.event_params) t AS event_param
		WHERE logdate BETWEEN '2019-05-30' AND '2019-06-03'
		 AND event_name='chatroom_queue')f3 )f3
	ON f6.advertising_id=f3.device.advertising_id AND f6.room_id=f3.room_id AND f6.logdate=f3.logdate
	LEFT JOIN
	(SELECT f5.*,
			CASE WHEN f5.event_param.KEY='room_id' THEN f5.event_param.value.string_value END AS room_id
	FROM
		(SELECT t.* FROM sharemax_dw.funshare_firebase_other t LATERAL VIEW explode(t.event_params) t AS event_param
		WHERE logdate BETWEEN '2019-05-30' AND '2019-06-03'
		 AND event_name='chatroom_queue_success')f5 )f5
	ON f6.advertising_id=f5.device.advertising_id AND f6.room_id=f5.room_id AND f6.logdate=f5.logdate
GROUP BY f6.logdate



-- 是否要保留匿名的功能
-- 1、0531匿名用户的留存率（次留、二日留存、七留）
-- 2、留存的匿名用户选择匿名的占比与非匿名占比

SELECT f6.logdate logdate,
		COUNT(DISTINCT CASE WHEN f6.gender='girl' AND f6.is_anony=1 AND f7.aid IS NULL THEN f6.aid ELSE NULL END) getin_girl_annoy_cnt,
		COUNT(DISTINCT CASE WHEN f6.gender='girl' AND f6.is_anony=1 AND f7.aid IS NULL THEN re_1.device.advertising_id ELSE NULL END) re_1_cnt,
		COUNT(DISTINCT CASE WHEN f6.gender='girl' AND f6.is_anony=1 AND f7.aid IS NULL AND re_1.is_anony=1 THEN re_1.device.advertising_id END) re_1_annoy_cnt,
		COUNT(DISTINCT CASE WHEN f6.gender='girl' AND f6.is_anony=1 AND f7.aid IS NULL THEN re_2.device.advertising_id ELSE NULL END) re_2_cnt,
		COUNT(DISTINCT CASE WHEN f6.gender='girl' AND f6.is_anony=1 AND f7.aid IS NULL AND re_2.is_anony=1 THEN re_2.device.advertising_id END) re_2_annoy_cnt,
		COUNT(DISTINCT CASE WHEN f6.gender='girl' AND f6.is_anony=1 AND f7.aid IS NULL THEN re_7.device.advertising_id ELSE NULL END) re_7_cnt,
		COUNT(DISTINCT CASE WHEN f6.gender='girl' AND f6.is_anony=1 AND f7.aid IS NULL AND re_7.is_anony=1 THEN re_7.device.advertising_id END) re_7_annoy_cnt
FROM
	(SELECT f1.logdate logdate,
			f1.device.advertising_id aid,
			f1.is_anony is_anony, --0 & 1
			f1.event_param2.value.string_value gender --boy & girl
	FROM
		(SELECT t.*,
				event_param.value.int_value is_anony
			FROM sharemax_dw.funshare_firebase_other t LATERAL VIEW explode(t.event_params) t AS event_param
			WHERE logdate='2019-05-31'
			  AND event_name='getin_chatroom_success'
			  AND event_param.KEY='is_anonymous'
			  )f1 LATERAL VIEW explode(f1.event_params)f1 AS event_param2
	WHERE f1.event_param2.KEY='gender')f6
	LEFT JOIN
	(SELECT DISTINCT t.account_id aid
		FROM dm.user_authorizations t
		WHERE t.app = 'funshare'
	  	  AND t.user_id IN(SELECT user_id FROM tmp.tmp_chatroom_host_type WHERE host_type='fake')
	)f7
	ON f6.aid=f7.aid
	LEFT JOIN
	(SELECT t.*,
			event_param.value.int_value is_anony
	FROM sharemax_dw.funshare_firebase_other t LATERAL VIEW explode(t.event_params) t AS event_param
	WHERE logdate= date_add('2019-05-31',1)
	  AND event_name='getin_chatroom_success'
	  AND event_param.KEY='is_anonymous')re_1
	ON f6.aid=re_1.device.advertising_id
	LEFT JOIN
	(SELECT t.*,
			event_param.value.int_value is_anony
	FROM sharemax_dw.funshare_firebase_other t LATERAL VIEW explode(t.event_params) t AS event_param
	WHERE logdate= date_add('2019-05-31',2)
	  AND event_name='getin_chatroom_success'
	  AND event_param.KEY='is_anonymous')re_2
	ON f6.aid=re_2.device.advertising_id
	LEFT JOIN
	(SELECT t.*,
			event_param.value.int_value is_anony
	FROM sharemax_dw.funshare_firebase_other t LATERAL VIEW explode(t.event_params) t AS event_param
	WHERE logdate= date_add('2019-05-31',7)
	  AND event_name='getin_chatroom_success'
	  AND event_param.KEY='is_anonymous')re_7
	ON f6.aid=re_7.device.advertising_id
GROUP BY f6.logdate


--version 2.0
SELECT f1.logdate logdate,
		COUNT(DISTINCT CASE WHEN f7.aid IS NULL THEN f1.aid ELSE NULL END) getin_girl_annoy_cnt,
		COUNT(DISTINCT CASE WHEN f7.aid IS NULL THEN re_1.device.advertising_id ELSE NULL END) re_1_cnt,
		COUNT(DISTINCT CASE WHEN f7.aid IS NULL AND re_1.is_anony=1 THEN re_1.device.advertising_id END) re_1_annoy_cnt,
		COUNT(DISTINCT CASE WHEN f7.aid IS NULL THEN re_2.device.advertising_id ELSE NULL END) re_2_cnt,
		COUNT(DISTINCT CASE WHEN f7.aid IS NULL AND re_2.is_anony=1 THEN re_2.device.advertising_id END) re_2_annoy_cnt,
		COUNT(DISTINCT CASE WHEN f7.aid IS NULL THEN re_7.device.advertising_id ELSE NULL END) re_7_cnt,
		COUNT(DISTINCT CASE WHEN f7.aid IS NULL AND re_7.is_anony=1 THEN re_7.device.advertising_id END) re_7_annoy_cnt
FROM
	(SELECT t.*,
			t.device.advertising_id aid
		FROM sharemax_dw.funshare_firebase_other t LATERAL VIEW explode(t.event_params) t AS event_param
		WHERE logdate='2019-06-01'
		  AND event_name='getin_chatroom_success'
		  AND event_param.KEY='is_anonymous'
		  AND event_param.value.int_value=1
		  )f1
	LEFT JOIN
	(SELECT DISTINCT t.account_id aid
		FROM dm.user_authorizations t
		WHERE t.app = 'funshare'
	  	  AND t.user_id IN(SELECT user_id FROM tmp.tmp_chatroom_host_type WHERE host_type='fake')
	)f7
	ON f1.aid=f7.aid
	LEFT JOIN
	(SELECT t.*,
			event_param.value.int_value is_anony
	FROM sharemax_dw.funshare_firebase_other t LATERAL VIEW explode(t.event_params) t AS event_param
	WHERE logdate= date_add('2019-06-01',1)
	  AND event_name='getin_chatroom_success'
	  AND event_param.KEY='is_anonymous')re_1
	ON f1.aid=re_1.device.advertising_id
	LEFT JOIN
	(SELECT t.*,
			event_param.value.int_value is_anony
	FROM sharemax_dw.funshare_firebase_other t LATERAL VIEW explode(t.event_params) t AS event_param
	WHERE logdate= date_add('2019-06-01',2)
	  AND event_name='getin_chatroom_success'
	  AND event_param.KEY='is_anonymous')re_2
	ON f1.aid=re_2.device.advertising_id
	LEFT JOIN
	(SELECT t.*,
			event_param.value.int_value is_anony
	FROM sharemax_dw.funshare_firebase_other t LATERAL VIEW explode(t.event_params) t AS event_param
	WHERE logdate= date_add('2019-06-01',7)
	  AND event_name='getin_chatroom_success'
	  AND event_param.KEY='is_anonymous')re_7
	ON f1.aid=re_7.device.advertising_id
GROUP BY f1.logdate



--version 3.0
--匿名用户的留存情况（不区分留存时是否匿名）
SELECT f1.logdate logdate,
		COUNT(DISTINCT CASE WHEN f7.aid IS NULL THEN f1.aid ELSE NULL END) getin_girl_annoy_cnt,
		COUNT(DISTINCT CASE WHEN f7.aid IS NULL THEN re_1.device.advertising_id ELSE NULL END) re_1_cnt,
		COUNT(DISTINCT CASE WHEN f7.aid IS NULL THEN re_2.device.advertising_id ELSE NULL END) re_2_cnt,
		COUNT(DISTINCT CASE WHEN f7.aid IS NULL THEN re_3.device.advertising_id ELSE NULL END) re_3_cnt,
		COUNT(DISTINCT CASE WHEN f7.aid IS NULL THEN re_7.device.advertising_id ELSE NULL END) re_7_cnt
FROM
	(SELECT t.*,
			t.device.advertising_id aid
		FROM sharemax_dw.funshare_firebase_other t LATERAL VIEW explode(t.event_params) t AS event_param
		WHERE logdate='2019-06-01'
		  AND event_name='getin_chatroom_success'
		  AND event_param.KEY='is_anonymous'
		  AND event_param.value.int_value=1
		  )f1
	LEFT JOIN
	(SELECT DISTINCT t.account_id aid
		FROM dm.user_authorizations t
		WHERE t.app = 'funshare'
	  	  AND t.user_id IN(SELECT user_id FROM tmp.tmp_chatroom_host_type WHERE host_type='fake')
	)f7
	ON f1.aid=f7.aid
	LEFT JOIN
	(SELECT t.*
	FROM sharemax_dw.funshare_firebase_other t
	WHERE logdate= date_add('2019-06-01',1)
	  AND event_name='getin_chatroom_success')re_1
	ON f1.aid=re_1.device.advertising_id
	LEFT JOIN
	(SELECT t.*
	FROM sharemax_dw.funshare_firebase_other t
	WHERE logdate= date_add('2019-06-01',2)
	  AND event_name='getin_chatroom_success')re_2
	ON f1.aid=re_2.device.advertising_id
	LEFT JOIN
	(SELECT t.*
	FROM sharemax_dw.funshare_firebase_other t
	WHERE logdate= date_add('2019-06-01',3)
	  AND event_name='getin_chatroom_success')re_3
	ON f1.aid=re_3.device.advertising_id
	LEFT JOIN
	(SELECT t.*
	FROM sharemax_dw.funshare_firebase_other t
	WHERE logdate= date_add('2019-06-01',7)
	  AND event_name='getin_chatroom_success')re_7
	ON f1.aid=re_7.device.advertising_id
GROUP BY f1.logdate


--近一周进入聊天室女性用户总量、匿名总量、未匿名总量
SELECT f6.logdate logdate,
		COUNT(DISTINCT CASE WHEN f7.aid IS NULL AND f6.gender='girl' THEN f6.aid ELSE NULL END) getin_girl_cnt,
		COUNT(DISTINCT CASE WHEN f7.aid IS NULL AND f6.gender='girl' AND f6.is_anony=1 THEN f6.aid ELSE NULL END) getin_girl_annoy_cnt,
		COUNT(DISTINCT CASE WHEN f7.aid IS NULL AND f6.gender='girl' AND f6.is_anony=0 THEN f6.aid ELSE NULL END) getin_girl_unannoy_cnt
FROM
	(SELECT f1.logdate logdate,
			f1.device.advertising_id aid,
			f1.is_anony is_anony, --0 & 1
			f1.event_param2.value.string_value gender --boy & girl
	FROM
		(SELECT t.*,
				event_param.value.int_value is_anony
			FROM sharemax_dw.funshare_firebase_other t LATERAL VIEW explode(t.event_params) t AS event_param
			WHERE logdate BETWEEN '2019-05-30' AND '2019-06-03'
			  AND event_name='getin_chatroom_success'
			  AND event_param.KEY='is_anonymous'
			  )f1 LATERAL VIEW explode(f1.event_params)f1 AS event_param2
	WHERE f1.event_param2.KEY='gender')f6
	LEFT JOIN
	(SELECT DISTINCT t.account_id aid
		FROM dm.user_authorizations t
		WHERE t.app = 'funshare'
	  	  AND t.user_id IN(SELECT user_id FROM tmp.tmp_chatroom_host_type WHERE host_type='fake')
	)f7
	ON f6.aid=f7.aid
GROUP BY f6.logdate

--同时存在匿名和不匿名行为的用户，是否匿名的先后顺序

SELECT both.logdate logdate,
		COUNT(DISTINCT CASE WHEN f8.is_anony=1 THEN both.aid END) annoy_first_cnt,
		COUNT(DISTINCT CASE WHEN f8.is_anony=0 THEN both.aid END) unannoy_first_cnt
FROM
	(SELECT both.*
	FROM
		(SELECT f6.logdate logdate,
				CASE WHEN f7.aid IS NULL AND f6.gender='girl' THEN f6.aid END AS aid,
				COUNT(CASE WHEN f6.is_anony=1 THEN 1 END) getin_girl_annoy_cnt,
				COUNT(CASE WHEN f6.is_anony=0 THEN 1 END) getin_girl_unannoy_cnt
		FROM
			(SELECT f1.logdate logdate,
						f1.device.advertising_id aid,
						f1.is_anony is_anony, --0 & 1
						f1.event_param2.value.string_value gender --boy & girl
				FROM
					(SELECT t.*,
							event_param.value.int_value is_anony
						FROM sharemax_dw.funshare_firebase_other t LATERAL VIEW explode(t.event_params) t AS event_param
						WHERE logdate BETWEEN '2019-05-30' AND '2019-06-03'
						  AND event_name='getin_chatroom_success'
						  AND event_param.KEY='is_anonymous'
						  )f1 LATERAL VIEW explode(f1.event_params)f1 AS event_param2
				WHERE f1.event_param2.KEY='gender')f6
				LEFT JOIN
				(SELECT DISTINCT t.account_id aid
					FROM dm.user_authorizations t
					WHERE t.app = 'funshare'
				  	  AND t.user_id IN(SELECT user_id FROM tmp.tmp_chatroom_host_type WHERE host_type='fake')
				)f7
				ON f6.aid=f7.aid
		GROUP BY f6.logdate,
				 CASE WHEN f7.aid IS NULL AND f6.gender='girl' THEN f6.aid END)both
	WHERE both.getin_girl_annoy_cnt>0
	  AND both.getin_girl_unannoy_cnt>0)both
	LEFT JOIN
	(SELECT f8.*
		FROM
			(SELECT t.*,
			event_param.value.int_value is_anony,
			row_number() over(PARTITION BY logdate,device.advertising_id ORDER BY event_timestamp) rn
	FROM sharemax_dw.funshare_firebase_other t LATERAL VIEW explode(t.event_params) t AS event_param
	WHERE logdate BETWEEN '2019-05-30' AND '2019-06-03'
	  AND event_name='getin_chatroom_success'
	  AND event_param.KEY='is_anonymous'
	  )f8
	WHERE f8.rn=1)f8
	ON both.logdate=f8.logdate AND both.aid=f8.device.advertising_id
GROUP BY both.logdate


-- 新版上线之后新用户对party按钮的点击率
-- chatroom_queue 排麦
-- chatroom_queue_success 上麦
-- 限定hindi用户，目前只有hindi用户可以看到party按钮
-- 新版上线日期：6月6日
SELECT f1.user_create_time user_create_time,
		COUNT(DISTINCT u.user_id) new_user_cnt,
		COUNT(DISTINCT f2.device.advertising_id) new_user_click_cnt,
		COUNT(DISTINCT f3.device.advertising_id) new_user_getin_cnt,
		COUNT(DISTINCT f4.device.advertising_id) new_user_queue_cnt,
		COUNT(DISTINCT f5.device.advertising_id) new_user_queue_success_cnt
FROM
	(SELECT to_date(from_unixtime(unix_timestamp(t.created_at)+19800)) AS user_create_time,
	        account_id,
	        user_id
	FROM
		  (SELECT t.*,
		          row_number() over(partition BY t.account_id ORDER BY t.created_at DESC) AS rn
		   FROM dm.user_authorizations AS t
		   WHERE t.app = 'funshare') AS t
	WHERE t.rn = 1
	  AND to_date(from_unixtime(unix_timestamp(t.created_at)+19800)) BETWEEN '2019-06-01' AND '2019-06-10')f1
	LEFT JOIN user_center.funshare_user_accounts u
	ON f1.user_id=u.user_id AND u.current_lang='hi'
	LEFT JOIN
	  (SELECT t.*
	   FROM sharemax_dw.funshare_firebase_other t
	   WHERE logdate BETWEEN '2019-06-01' AND '2019-06-10'
	     AND event_name='click_chatroom_tab'
	    )f2
	ON f1.account_id=f2.device.advertising_id AND f1.user_create_time=f2.logdate
	LEFT JOIN
	  (SELECT t.*
	   FROM sharemax_dw.funshare_firebase_other t
	   WHERE logdate BETWEEN '2019-06-01' AND '2019-06-10'
	     AND event_name='getin_chatroom_success'
	    )f3
	ON f1.account_id=f3.device.advertising_id AND f1.user_create_time=f3.logdate
	LEFT JOIN
	  (SELECT t.*
	   FROM sharemax_dw.funshare_firebase_other t
	   WHERE logdate BETWEEN '2019-06-01' AND '2019-06-10'
	     AND event_name='chatroom_queue'
	    )f4
	ON f1.account_id=f4.device.advertising_id AND f1.user_create_time=f4.logdate
	LEFT JOIN
	  (SELECT t.*
	   FROM sharemax_dw.funshare_firebase_other t
	   WHERE logdate BETWEEN '2019-06-01' AND '2019-06-10'
	     AND event_name='chatroom_queue_success'
	    )f5
	ON f1.account_id=f5.device.advertising_id AND f1.user_create_time=f5.logdate
GROUP BY f1.user_create_time


-- 区分新老用户，chatroom漏斗
-- 新用户定义：当天创建账户的用户即为新用户
-- 新增条件click_chatroom_tab(排除当天只通过push进入的用户，即当天发生过click_chatroom事件)
SELECT f1.logdate logdate,
		COUNT(DISTINCT CASE WHEN f4.user_create_time=f1.logdate THEN f1.aid END) new_click_cnt,
		COUNT(DISTINCT CASE WHEN f4.user_create_time<>f1.logdate THEN f1.aid END) old_click_cnt,
		COUNT(DISTINCT CASE WHEN f4.user_create_time=f1.logdate
							 AND f2.event_name='getin_chatroom_success' THEN f1.aid END) new_getin_cnt,
		COUNT(DISTINCT CASE WHEN f4.user_create_time<>f1.logdate
							 AND f2.event_name='getin_chatroom_success' THEN f1.aid END) old_getin_cnt,
		COUNT(DISTINCT CASE WHEN f4.user_create_time=f1.logdate
							 AND f2.event_name='chatroom_queue' THEN f1.aid END) new_queue_cnt,
		COUNT(DISTINCT CASE WHEN f4.user_create_time<>f1.logdate
							 AND f2.event_name='chatroom_queue' THEN f1.aid END) old_queue_cnt,
		COUNT(DISTINCT CASE WHEN f4.user_create_time=f1.logdate
							 AND f2.event_name='chatroom_queue_success' THEN f1.aid END) new_queue_success_cnt,
		COUNT(DISTINCT CASE WHEN f4.user_create_time<>f1.logdate
							 AND f2.event_name='chatroom_queue_success' THEN f1.aid END) old_queue_success_cnt
FROM
		(SELECT DISTINCT t.logdate logdate,
				t.device.advertising_id aid
	   	 FROM sharemax_dw.funshare_firebase_other t
	     LEFT JOIN
	     	(SELECT DISTINCT t.account_id FROM dm.user_authorizations t
		         WHERE t.app = 'funshare'
		           AND t.user_id IN (SELECT user_id FROM tmp.tmp_chatroom_host_type WHERE host_type='fake'))t1
	     ON t.device.advertising_id =t1.account_id
	     WHERE t1.account_id IS NULL
	       AND t.logdate BETWEEN '2019-06-01' AND '2019-06-10'
	       AND t.event_name='click_chatroom_tab'
	     )f1 --有过click_chatroom的用户，排除fake user
		LEFT JOIN
		(SELECT t.*
		 FROM sharemax_dw.funshare_firebase_other t
		 WHERE logdate BETWEEN '2019-06-01' AND '2019-06-10'
		   AND event_name IN('getin_chatroom_success','chatroom_queue','chatroom_queue_success')
		  )f2
		ON f1.aid=f2.device.advertising_id AND f1.logdate=f2.logdate
		LEFT JOIN
		(SELECT account_id,
		        user_id
		FROM
			  (SELECT t.*,
			          row_number() over(partition BY t.account_id ORDER BY t.created_at DESC) AS rn
			   FROM dm.user_authorizations AS t
			   WHERE t.app = 'funshare') AS t
		WHERE t.rn = 1)f3
		ON f1.aid=f3.account_id
		LEFT JOIN
		(SELECT to_date(from_unixtime(unix_timestamp(t.created_at)+19800)) user_create_time,--user_id新增时间
				t.*
		FROM user_center.funshare_user_accounts t)f4
		ON f3.user_id=f4.user_id
GROUP BY f1.logdate


-- 0601-0610未点击party按钮的getin_user_cnt
SELECT f1.logdate logdate,
		COUNT(DISTINCT f1.aid) getin_all_cnt,
		COUNT(DISTINCT CASE WHEN f2.device.advertising_id IS NOT NULL THEN f1.aid END) getin_click_cnt,
		COUNT(DISTINCT CASE WHEN f2.device.advertising_id IS NULL THEN f1.aid END) getin_noclick_cnt
FROM
		(SELECT DISTINCT t.logdate logdate,
				t.device.advertising_id aid
	   	 FROM sharemax_dw.funshare_firebase_other t
	     	  LEFT JOIN
	     		(SELECT DISTINCT t.account_id FROM dm.user_authorizations t
			     WHERE t.app = 'funshare'
			       AND t.user_id IN (SELECT user_id FROM tmp.tmp_chatroom_host_type WHERE host_type='fake'))t1
	     	  ON t.device.advertising_id =t1.account_id
	     WHERE t1.account_id IS NULL
	       AND t.logdate BETWEEN '2019-06-01' AND '2019-06-10'
	       AND t.event_name='getin_chatroom_success'
	     )f1 --getin用户，排除fake user
		LEFT JOIN
		(SELECT *
	   	 FROM sharemax_dw.funshare_firebase_other t
	     WHERE t.logdate BETWEEN '2019-06-01' AND '2019-06-10'
	       AND t.event_name='click_chatroom_tab'
	     )f2 --click_chatroom_tab
		ON f1.aid=f2.device.advertising_id AND f1.logdate=f2.logdate
GROUP BY f1.logdate


-- 0609-0611点击tab但未进入过房间的用户id
SELECT f1.user_id
FROM
	(SELECT DISTINCT t.data['user_id'] AS user_id
	FROM sharemax_dw.action_log t
	WHERE t.logdate BETWEEN '2019-06-08' AND '2019-06-11'
	  AND to_date(from_unixtime(unix_timestamp(regexp_replace(substr(t.`timestamp`,1,19),'T',' ')) + 19800)) BETWEEN '2019-06-09' AND '2019-06-11'
	  AND t.data['type'] = 'click_chatroom_tab')f1
	LEFT JOIN
	(SELECT DISTINCT t.data['user_id'] AS user_id
	FROM sharemax_dw.action_log t
	WHERE t.logdate BETWEEN '2019-06-08' AND '2019-06-11'
	  AND to_date(from_unixtime(unix_timestamp(regexp_replace(substr(t.`timestamp`,1,19),'T',' ')) + 19800)) BETWEEN '2019-06-09' AND '2019-06-11'
	  AND t.data['type'] = 'getin_chatroom_success')f2
	ON f1.user_id=f2.user_id
WHERE f2.user_id IS NULL


--chatroom用户在房间内时长分布
SELECT t.logdate logdate,
		SUM(t3.duration) duration_all,
		COUNT(DISTINCT t1.advertising_id) all_user_cnt,
		SUM(CASE WHEN t3.duration>5*60 THEN t3.duration END) duration_up5,
		COUNT(CASE WHEN t3.duration>5*60 THEN t1.advertising_id END) up5_user_cnt,
		SUM(CASE WHEN t3.duration>10*60 THEN t3.duration END) duration_up10,
		COUNT(CASE WHEN t3.duration>10*60 THEN t1.advertising_id END) up10_user_cnt
FROM
  (SELECT DISTINCT t.logdate,t.device.advertising_id AS advertising_id
   FROM sharemax_dw.funshare_firebase_other as t
   LEFT JOIN
     (SELECT distinct t.account_id FROM dm.user_authorizations as t
         WHERE t.app = 'funshare'
           and t.user_id in (select user_id from tmp.tmp_chatroom_host_type where host_type='fake') ) as t1 ON t.device.advertising_id =t1.account_id
   WHERE t1.account_id is null
     AND t.logdate = '2019-06-11'
     AND t.event_name IN ('click_chatroom_tab'))t
  LEFT JOIN
  (SELECT DISTINCT t.device.advertising_id AS advertising_id
     FROM sharemax_dw.funshare_firebase_other as t
     WHERE t.logdate = '2019-06-11'
       AND t.event_name IN ('getin_chatroom_success'))t1
  ON t.advertising_id = t1.advertising_id
  LEFT JOIN
  (SELECT t.device.advertising_id AS advertising_id,
          sum(event_param.value.int_value)/1000 as duration --单位：秒
   FROM sharemax_dw.funshare_firebase_other as t LATERAL VIEW explode(t.event_params) abc AS event_param
   WHERE t.logdate = '2019-06-11'
     AND t.event_name IN ('leave_chatroom')
     AND event_param.key = 'time'
     AND event_param.value.int_value < 21600000
     and event_param.value.int_value > 0
   GROUP BY t.device.advertising_id)t3
  ON t.advertising_id = t3.advertising_id
GROUP BY t.logdate

-- 聊天室时长均值+中位数
SELECT avg(t3.duration)/60 duration_avg,
		percentile_approx(t3.duration,0.5)/60 duration_percentile
FROM
	(SELECT t.device.advertising_id AS advertising_id,
	          sum(event_param.value.int_value)/1000 as duration --单位：秒
	   FROM sharemax_dw.funshare_firebase_other as t LATERAL VIEW explode(t.event_params) abc AS event_param
	   WHERE t.logdate = '2019-06-11'
	     AND t.event_name IN ('leave_chatroom')
	     AND event_param.key = 'time'
	     AND event_param.value.int_value < 21600000
	     and event_param.value.int_value > 0
	   GROUP BY t.device.advertising_id)t3


-- 点击某条push的用户的留存情况
-- 点击push：push_click
-- 留存：impression
-- 推送ID：1560266085
-- 推送时间：2019-06-11 21:00:00
-- 需要的数据项包括了：点击推送设备的设备ID
-- 点击推送的日期，点击推送的时间（精确到分）
-- 自收到推送起两天内是否有卸载（推送发出当天为Day 0, 两天内指 Day 0 和 Day 1）,
-- 卸载的日期，卸载的时间（精确到分）
-- 收到推送第二天（Day 1）是否有打开过APP

SELECT f1.aid aid,
		f1.click_time click_time,
		CASE WHEN f2.aid IS NULL THEN 0 ELSE 1 END AS is_open_app,
		CASE WHEN f3.aid IS NULL THEN 0 ELSE 1 END AS is_remove_app,
		f3.remove_time remove_time
FROM
	(SELECT f1.device.advertising_id aid,
			from_unixtime(CAST(substr(f1.event_param2.value.int_value,1,10) AS bigint)+19800) click_time
	FROM
		(SELECT t.*
		FROM sharemax_dw.funshare_firebase_other t LATERAL VIEW explode(t.event_params) t AS event_param
		WHERE t.logdate BETWEEN '2019-06-11' AND '2019-06-12'
		  AND t.event_name='push_click'
		  AND t.event_param.KEY='push_id'
		  AND t.event_param.value.string_value='1560266085')f1 LATERAL VIEW explode(f1.event_params) f1 AS event_param2
	WHERE event_param2.KEY='click_at')f1 --0611 push_click用户
	LEFT JOIN
	(SELECT DISTINCT t.device.advertising_id aid
	FROM sharemax_dw.funshare_firebase_impression t
	WHERE t.logdate='2019-06-12'
	)f2
	ON f1.aid=f2.aid
	LEFT JOIN
	(SELECT f3.aid aid,
			f3.remove_time remove_time
	FROM
		(SELECT t.device.advertising_id aid,
				from_unixtime(CAST(substr(t.event_timestamp,1,10) AS bigint)+19800) remove_time,
				row_number() over(PARTITION BY t.device.advertising_id ORDER BY event_timestamp DESC) rn
		FROM sharemax_dw.funshare_firebase_app_remove t
		WHERE t.logdate BETWEEN '2019-06-11' AND '2019-06-12')f3
	WHERE f3.rn=1)f3
	ON f1.aid=f3.aid


-- 验证push被同一个用户点击2次及以上，点击同一个push2次及以上的设备的push_show的次数
SELECT f3.*,
		f4.push_show_cnt push_show_cnt
FROM
	(SELECT f2.push_id push_id,
			f2.push_time push_time,
			f1.aid aid,
			COUNT(DISTINCT f1.click_time) click_cnt--push被点击>=2次的人数
	FROM
		(SELECT t.*,
				to_date(t.start_at) push_time
		FROM sharemax_dw.notification_funshare t
		WHERE to_date(t.start_at) ='2019-05-25')f2 --0525发送的push
		LEFT JOIN
		(SELECT f1.device.advertising_id aid,
				f1.push_id push_id,
				from_unixtime(CAST(substr(f1.event_param2.value.int_value,1,10) AS bigint)+19800) click_time
		FROM
			(SELECT t.*,
					t.event_param.value.string_value push_id
			FROM sharemax_dw.funshare_firebase_other t LATERAL VIEW explode(t.event_params) t AS event_param
			WHERE t.logdate ='2019-05-25'
			  AND t.event_name='push_click'
			  AND t.event_param.KEY='push_id'
			  )f1 LATERAL VIEW explode(f1.event_params) f1 AS event_param2
		WHERE event_param2.KEY='click_at')f1
		ON f1.push_id=f2.push_id
	GROUP BY f2.push_id,
			 f2.push_time,
				f1.aid)f3
	LEFT JOIN
	(SELECT t.event_param.value.string_value push_id,
			t.device.advertising_id aid,
			COUNT(1) push_show_cnt --push_show次数
	FROM sharemax_dw.funshare_firebase_other t LATERAL VIEW explode(t.event_params) t AS event_param
	WHERE t.logdate='2019-05-25'
	  AND t.event_name='push_show'
	  AND t.event_param.KEY='push_id'
	GROUP BY t.event_param.value.string_value,
			 t.device.advertising_id)f4
	ON f3.push_id=f4.push_id AND f3.aid=f4.aid
WHERE f3.click_cnt>=2


SELECT f2.push_id,
		COUNT(DISTINCT f1.aid) click_aid_cnt--push被点击的总人数
FROM
	(SELECT t.*,
			to_date(t.start_at) push_time
	FROM sharemax_dw.notification_funshare t
	WHERE to_date(t.start_at) ='2019-05-25')f2 --0525发送的push
	LEFT JOIN
	(SELECT f1.device.advertising_id aid,
			f1.push_id push_id,
			from_unixtime(CAST(substr(f1.event_param2.value.int_value,1,10) AS bigint)+19800) click_time
	FROM
		(SELECT t.*,
				t.event_param.value.string_value push_id
		FROM sharemax_dw.funshare_firebase_other t LATERAL VIEW explode(t.event_params) t AS event_param
		WHERE t.logdate ='2019-05-25'
		  AND t.event_name='push_click'
		  AND t.event_param.KEY='push_id'
		  )f1 LATERAL VIEW explode(f1.event_params) f1 AS event_param2
	WHERE event_param2.KEY='click_at')f1
	ON f1.push_id=f2.push_id
GROUP BY f2.push_id


-- issue#4585
-- 6月10日：5cfe696c2c0560000133d73b
-- 6月11日：5cffbaab2c056000016f6a82
-- 6月12日：5d010be957d65c00013f0b2e
-- 留存情况：
-- 进到6月10日房间的人，有多少进入到了6月11日的房间，又有多少进到了6月12日的房间。
-- 6月11日进入房间的人，有多少进到了6月12日的房间。
-- 这几个房间内所有男性用户的平均停留时长

-- 1、留存情况
SELECT COUNT(f.advertising_id) getin_cnt_0610,
		COUNT(DISTINCT CASE WHEN f.advertising_id IS NOT NULL THEN f1.advertising_id END) getin_0610_to_0611_cnt,
		COUNT(f2.advertising_id) getin_0610_to_0612_cnt,
		COUNT(f1.advertising_id) getin_cnt_0611,
		COUNT(DISTINCT CASE WHEN f1.advertising_id IS NOT NULL THEN f3.advertising_id END) getin_0611_to_0612_cnt
FROM
	  (SELECT DISTINCT f.device.advertising_id advertising_id
	  FROM
	  	  (SELECT t.*
	  	   FROM sharemax_dw.funshare_firebase_other t
	  	   LEFT JOIN
	  	     (SELECT DISTINCT t.account_id FROM dm.user_authorizations t
	  	         WHERE t.app = 'funshare'
	  	           AND t.user_id IN(SELECT user_id FROM tmp.tmp_chatroom_host_type WHERE host_type='fake'))t1
	  	   ON t.device.advertising_id =t1.account_id
	  	   WHERE t1.account_id IS NULL
	  	     AND t.logdate = '2019-06-10'
	  	     AND t.event_name IN ('getin_chatroom_success'))f LATERAL VIEW explode(f.event_params) f AS event_param
	  WHERE f.event_param.KEY='room_id'
	    AND f.event_param.value.string_value='5cfe696c2c0560000133d73b')f
	  FULL JOIN
	  (SELECT DISTINCT f1.device.advertising_id advertising_id
	  FROM
	  	  (SELECT t.*
	  	   FROM sharemax_dw.funshare_firebase_other t
	  	   LEFT JOIN
	  	     (SELECT DISTINCT t.account_id FROM dm.user_authorizations t
	  	         WHERE t.app = 'funshare'
	  	           AND t.user_id IN(SELECT user_id FROM tmp.tmp_chatroom_host_type WHERE host_type='fake'))t1
	  	   ON t.device.advertising_id =t1.account_id
	  	   WHERE t1.account_id IS NULL
	  	     AND t.logdate = '2019-06-11'
	  	     AND t.event_name IN ('getin_chatroom_success'))f1 LATERAL VIEW explode(f1.event_params) f1 AS event_param
	  WHERE f1.event_param.KEY='room_id'
	    AND f1.event_param.value.string_value='5cffbaab2c056000016f6a82')f1
	  ON f.advertising_id=f1.advertising_id
	  LEFT JOIN
	  (SELECT DISTINCT t.device.advertising_id advertising_id
	  FROM sharemax_dw.funshare_firebase_other t LATERAL VIEW explode(t.event_params) t AS event_param
	  WHERE t.logdate='2019-06-12'
	    AND t.event_name='getin_chatroom_success'
	    AND t.event_param.KEY='room_id'
	    AND t.event_param.value.string_value='5d010be957d65c00013f0b2e')f2
	  ON f.advertising_id=f2.advertising_id
	  LEFT JOIN
	  (SELECT DISTINCT t.device.advertising_id advertising_id
	  FROM sharemax_dw.funshare_firebase_other t LATERAL VIEW explode(t.event_params) t AS event_param
	  WHERE t.logdate='2019-06-12'
	    AND t.event_name='getin_chatroom_success'
	    AND t.event_param.KEY='room_id'
	    AND t.event_param.value.string_value='5d010be957d65c00013f0b2e')f3
	  ON f1.advertising_id=f3.advertising_id

-- 2、各个房间男性用户平均停留时长
SELECT f4.logdate logdate,
		f4.room_id room_id,
		avg(f5.duration)/60 boy_duration
FROM
	(SELECT f4.logdate logdate,
		   f4.device.advertising_id aid,
		   f4.room_id room_id
	FROM
			(SELECT t.*,
					t.event_param.value.string_value room_id
			FROM sharemax_dw.funshare_firebase_other t LATERAL VIEW explode(t.event_params) t AS event_param
			WHERE t.logdate BETWEEN '2019-06-10' AND '2019-06-12'
			  AND t.event_name='getin_chatroom_success'
			  AND t.event_param.KEY='room_id'
			  AND t.event_param.value.string_value IN('5cfe696c2c0560000133d73b',
			  										  '5cffbaab2c056000016f6a82',
			  										  '5d010be957d65c00013f0b2e')
			)f4 LATERAL VIEW explode(f4.event_params) f4 AS event_param2
	WHERE f4.event_param2.KEY='gender'
	  AND f4.event_param2.value.string_value='boy')f4--进入指定房间男性用户id
	LEFT JOIN
	(SELECT f5.logdate logdate,
			f5.device.advertising_id aid,
			f5.room_id room_id,
			SUM(event_param2.value.int_value)/1000 AS duration
	FROM
			(SELECT t.*,
					t.event_param.value.string_value room_id
			FROM sharemax_dw.funshare_firebase_other t LATERAL VIEW explode(t.event_params) t AS event_param
			WHERE t.logdate BETWEEN '2019-06-10' AND '2019-06-12'
			  AND t.event_name='leave_chatroom'
			  AND t.event_param.KEY='room_id'
			  AND t.event_param.value.string_value IN('5cfe696c2c0560000133d73b',
			  										  '5cffbaab2c056000016f6a82',
			  										  '5d010be957d65c00013f0b2e')
			  )f5 LATERAL VIEW explode(f5.event_params) f5 AS event_param2
	WHERE f5.event_param2.KEY='time'
	  AND event_param2.value.int_value<21600000
	  AND event_param2.value.int_value>0
	GROUP BY f5.logdate,
			 f5.device.advertising_id,
			 f5.room_id)f5
	ON f4.logdate=f5.logdate AND f4.aid=f5.aid AND f4.room_id=f5.room_id
GROUP BY f4.logdate,
			f4.room_id


-- issue#4591
-- 6月11、12、13号三天
-- 每天，各时段（分小时）
-- 点击party tab的人数，click_chatroom_tab
-- 点击房间封面图的人数，click_chatroom
-- 进入房间成功的人数，getin_chatroom_success

SELECT t.logdate logdate,
		hour(from_unixtime(CAST(substr(t.event_timestamp,1,10) AS BIGINT)+19800)) hour,
		COUNT(DISTINCT CASE WHEN t.event_name='click_chatroom_tab' THEN t.device.advertising_id END) click_tab_cnt,
		COUNT(DISTINCT CASE WHEN t.event_name='click_chatroom' THEN t.device.advertising_id END) click_chatroom_cnt,
		COUNT(DISTINCT CASE WHEN t.event_name='getin_chatroom_success' THEN t.device.advertising_id END) getin_cnt
FROM sharemax_dw.funshare_firebase_other t
LEFT JOIN
(SELECT DISTINCT t.account_id FROM dm.user_authorizations t
      WHERE t.app = 'funshare'
        AND t.user_id IN(SELECT user_id FROM tmp.tmp_chatroom_host_type WHERE host_type='fake'))t1
ON t.device.advertising_id =t1.account_id
WHERE t1.account_id IS NULL
  AND t.logdate BETWEEN '2019-06-11' AND '2019-06-13'
  AND t.event_name IN ('click_chatroom_tab','click_chatroom','getin_chatroom_success')
GROUP BY t.logdate,
		 hour(from_unixtime(CAST(substr(t.event_timestamp,1,10) AS BIGINT)+19800))

-- issue#4596
-- push id
-- 1559920988 6月7号
-- 1560007370 6月8号
-- 1560094144 6月9号
-- 发push后两小时内点击push的人数，和点击过push且成功进入房间的人数
SELECT f1.push_id push_id,
	   COUNT(DISTINCT CASE WHEN f1.click_time<=from_unixtime(unix_timestamp(f2.start_at)+7200+19800) THEN f1.aid END) click_cnt_2h,
	   COUNT(DISTINCT CASE WHEN f1.click_time<=from_unixtime(unix_timestamp(f2.start_at)+7200+19800) THEN f3.device.advertising_id END) getin_cnt
FROM
	(SELECT f1.logdate logdate,
			f1.device.advertising_id aid,
			f1.push_id push_id,
			from_unixtime(CAST(substr(f1.event_param2.value.int_value,1,10) AS bigint)+19800) click_time
	FROM
		(SELECT t.*,
				t.event_param.value.string_value push_id
		FROM sharemax_dw.funshare_firebase_other t LATERAL VIEW explode(t.event_params) t AS event_param
		WHERE t.logdate BETWEEN '2019-06-06' AND '2019-06-09'
		  AND t.event_name='push_click'
		  AND t.event_param.KEY='push_id'
		  AND t.event_param.value.string_value IN('1559920988','1560007370','1560094144')
		  )f1 LATERAL VIEW explode(f1.event_params) f1 AS event_param2
	WHERE event_param2.KEY='click_at')f1--点击过指定push的设备id
	LEFT JOIN
	(SELECT  to_date(t.start_at) logdate,
			t.push_id push_id,
			t.start_at start_at,
			t.event_id room_id
	FROM sharemax_dw.notification_funshare t
	WHERE to_date(t.start_at) BETWEEN '2019-06-06' AND '2019-06-09'
	  AND t.push_id IN(1559920988,1560007370,1560094144)) f2
	ON f1.push_id=CAST(f2.push_id AS string)
	LEFT JOIN
	(SELECT t.*,
			t.event_param.value.string_value room_id
	FROM sharemax_dw.funshare_firebase_other t LATERAL VIEW explode(t.event_params) t AS event_param
	WHERE t.logdate BETWEEN '2019-06-06' AND '2019-06-09'
	  AND t.event_name='getin_chatroom_success'
	  AND t.event_param.KEY='room_id') f3
	ON f1.aid=f3.device.advertising_id AND f2.room_id=f3.room_id
GROUP BY f1.push_id

-- 新增：
-- 需要知道点击这条push之后，用户为什么没有进入房间。
-- 如我们早上沟通过的，分男女去分析
-- 如果是女性，有多少是选匿名这步流失的，有多少是房间关闭了，有多少是没有登录，有多少是其他原因（网络问题等）；
-- 如果是男性，有多少是房间关闭，有多少是没有登录，有多少是其他原因（网络问题等）
SELECT f1.push_id push_id,
		f2.room_id room_id,
		COUNT(DISTINCT f1.aid) click_cnt,
		COUNT(DISTINCT CASE WHEN f6.device.advertising_id IS NULL AND f1.click_time>f4.close_time THEN f1.aid END) all_close_cnt,
		COUNT(DISTINCT CASE WHEN f6.device.advertising_id IS NULL AND f5.aid IS NULL THEN f3.aid END) choose_anony_lose_cnt
FROM
	(SELECT f1.logdate logdate,
			f1.device.advertising_id aid,
			f1.push_id push_id,
			from_unixtime(CAST(substr(f1.event_param2.value.int_value,1,10) AS bigint)+19800) click_time--点击push时间
	FROM
		(SELECT t.*,
				t.event_param.value.string_value push_id
		FROM sharemax_dw.funshare_firebase_other t LATERAL VIEW explode(t.event_params) t AS event_param
		WHERE t.logdate BETWEEN '2019-06-06' AND '2019-06-10'
		  AND t.event_name='push_click'
		  AND t.event_param.KEY='push_id'
		  AND t.event_param.value.string_value IN('1559920988','1560007370','1560094144')
		  )f1 LATERAL VIEW explode(f1.event_params) f1 AS event_param2
	WHERE event_param2.KEY='click_at')f1--点击过指定push的设备id
	LEFT JOIN
	(SELECT  to_date(t.start_at) logdate,
			t.push_id push_id,
			t.start_at start_at,
			t.event_id room_id
	FROM sharemax_dw.notification_funshare t
	WHERE to_date(t.start_at) BETWEEN '2019-06-06' AND '2019-06-09'
	  AND t.push_id IN(1559920988,1560007370,1560094144))f2
	ON f1.push_id=CAST(f2.push_id AS string)
	LEFT JOIN
	(SELECT t.*,
			t.device.advertising_id aid
	FROM sharemax_dw.funshare_firebase_other t
	WHERE t.logdate BETWEEN '2019-06-06' AND '2019-06-10'
	  AND t.event_name='chatroom_choose_anonymous')f3 --点击push进入匿名选择界面的为女生
	ON f1.aid=f3.aid
	LEFT JOIN
	(SELECT t.*,
			t.device.advertising_id aid
	FROM sharemax_dw.funshare_firebase_other t
	WHERE t.logdate BETWEEN '2019-06-06' AND '2019-06-10'
	  AND t.event_name='chatroom_anonymous')f5 --选择是否匿名
	ON f1.aid=f5.aid
	LEFT JOIN
	(SELECT t.*,
			t.event_param.value.string_value room_id
	FROM sharemax_dw.funshare_firebase_other t LATERAL VIEW explode(t.event_params) t AS event_param
	WHERE t.logdate BETWEEN '2019-06-06' AND '2019-06-10'
	  AND t.event_name='getin_chatroom_success'
	  AND t.event_param.KEY='room_id'
	  AND t.event_param.value.string_value IN('5cfa751deb1bc0000156e36a',
			  								'5cfbc82b4375ca00016c641c',
			  								'5cfd1876eaa0a80001e096c9')
	  ) f6
	ON f1.aid=f6.device.advertising_id AND f2.room_id=f6.room_id
	LEFT JOIN
	(SELECT f4.room_id room_id,
			from_unixtime(CAST(substr(f4.event_timestamp,1,10) AS bigint)+19800+300) close_time --host最后一次离开指定房间时间+5min
	FROM
		(SELECT f4.*,
				row_number() over(PARTITION BY f4.device.advertising_id ORDER BY f4.event_timestamp DESC) rn
		FROM
			(SELECT t.*,
					t.event_param.value.string_value room_id
			FROM sharemax_dw.funshare_firebase_other t LATERAL VIEW explode(t.event_params) t AS event_param
			WHERE t.logdate BETWEEN '2019-06-06' AND '2019-06-10'
			  AND t.event_name='leave_chatroom'
			  AND t.event_param.KEY='room_id'
			  AND t.event_param.value.string_value IN('5cfa751deb1bc0000156e36a',
			  										'5cfbc82b4375ca00016c641c',
			  										'5cfd1876eaa0a80001e096c9')
			  )f4 LATERAL VIEW explode(f4.event_params) f4 AS event_param2
		WHERE f4.event_param2.KEY='user'
		  AND f4.event_param2.value.string_value='host')f4
	WHERE f4.rn=1)f4
	ON f4.room_id=f2.room_id
GROUP BY f1.push_id,
		 f2.room_id

-- 日期：0610-0615
-- user：19750543
-- room_id
-- '5d03b7894265680001ebae13',
-- '5d025e93a9b8b00001dea606',
-- '5d010be8a63fe8000120a067',
-- '5cffbaab2c056000016f6a82',
-- '5cfe696c2c0560000133d73b'

-- chatroom_queue_success

-- chatroom_leave_queue
-- chatroom_kickoff
-- chatroom_ban_user
-- leave_chatroom（host user）


-- issue#4603
-- 想看看用户对audio feed的消费意愿，因此需要看看每天播放2首及以上audio feed的用户及占比，需要跑的字段为：
-- 时间0610-0616日，每天播放audio feed 的总用户，播放2首，3首，4首及5首以上（合在一起跑）feed的用户数，及播放不同数量分别的用户占比
-- ps: 自动播放到下一首的用户也算：只要用户没在第一首播放过程中暂定，顺利听完了，就可以认为他是对 audio feed感兴趣的
-- play_audio

SELECT f.logdate logdate,
		COUNT(DISTINCT f.aid) play_audio_user_cnt,
		COUNT(DISTINCT CASE WHEN f.play_audio_cnt=2 THEN f.aid END) play_audio_2_user_cnt,
		COUNT(DISTINCT CASE WHEN f.play_audio_cnt=3 THEN f.aid END) play_audio_3_user_cnt,
		COUNT(DISTINCT CASE WHEN f.play_audio_cnt=4 THEN f.aid END) play_audio_4_user_cnt,
		COUNT(DISTINCT CASE WHEN f.play_audio_cnt>=5 THEN f.aid END) play_audio_over5_user_cnt
FROM
	(SELECT t.logdate logdate,
			t.device.advertising_id aid,
			COUNT(1) AS play_audio_cnt
	FROM
		(SELECT t.*
		FROM sharemax_dw.funshare_firebase_other t
		WHERE t.logdate BETWEEN '2019-06-10' AND '2019-06-16'
		  AND t.event_name='play_audio')t
	GROUP BY t.logdate,
			 t.device.advertising_id)f
GROUP BY f.logdate

-- issue#4605
-- 目前看，56%的用户都只播放了一首audio feed,想把这部分用户拉出来看看，
-- 他们都播放了什么歌，播放的时长，对应给出解决策略，
-- 数据需要字段：
-- 时间06月10日-16日 ，tag id ，用户id ,feed id , 歌曲时长(post_audio_complete)，歌曲播放时长(play_audio_time)
SELECT f1.india_date india_date,
		f1.user_id user_id,
		f2.feed_id feed_id,
		f3.tag_id tag_id,
		f2.play_time play_time,
		f3.audio_duration audio_duration
FROM
	(SELECT f1.india_date india_date,
			f1.user_id user_id
	FROM
		(SELECT to_date(from_unixtime(unix_timestamp(regexp_replace(substr(t.`timestamp`,1,19),'T',' ')) + 19800)) india_date,
				t.data['user_id'] user_id,
				COUNT(1) play_cnt
		FROM sharemax_dw.action_log t
		WHERE t.logdate BETWEEN '2019-06-09' AND '2019-06-16'
		  AND to_date(from_unixtime(unix_timestamp(regexp_replace(substr(t.`timestamp`,1,19),'T',' ')) + 19800)) BETWEEN '2019-06-10' AND '2019-06-16'
		  AND t.data['type']='play_audio_time'
		GROUP BY to_date(from_unixtime(unix_timestamp(regexp_replace(substr(t.`timestamp`,1,19),'T',' ')) + 19800)),
				 t.data['user_id'])f1
	WHERE f1.play_cnt=1)f1
	LEFT JOIN
	(SELECT to_date(from_unixtime(unix_timestamp(regexp_replace(substr(t.`timestamp`,1,19),'T',' ')) + 19800)) india_date,
			t.data['user_id'] user_id,
			t.data['feed_id'] feed_id,
			t.data['time'] play_time
	FROM sharemax_dw.action_log t
	WHERE t.logdate BETWEEN '2019-06-09' AND '2019-06-16'
	  AND to_date(from_unixtime(unix_timestamp(regexp_replace(substr(t.`timestamp`,1,19),'T',' ')) + 19800)) BETWEEN '2019-06-10' AND '2019-06-16'
	  AND t.data['type']='play_audio_time')f2
	ON f1.india_date=f2.india_date AND f1.user_id=f2.user_id
	LEFT JOIN sharemax_dw.feed f3
	ON f2.feed_id=f3.id



SELECT f1.india_date india_date,
		COUNT(DISTINCT user_id) user_cnt_1
	FROM
		(SELECT to_date(from_unixtime(unix_timestamp(regexp_replace(substr(t.`timestamp`,1,19),'T',' ')) + 19800)) india_date,
				t.data['user_id'] user_id,
				COUNT(1) play_cnt
		FROM sharemax_dw.action_log t
		WHERE t.logdate BETWEEN '2019-06-09' AND '2019-06-16'
		  AND to_date(from_unixtime(unix_timestamp(regexp_replace(substr(t.`timestamp`,1,19),'T',' ')) + 19800)) BETWEEN '2019-06-10' AND '2019-06-16'
		  AND t.data['type']='play_audio'
		  AND t.app_v='1.3.4'
		GROUP BY to_date(from_unixtime(unix_timestamp(regexp_replace(substr(t.`timestamp`,1,19),'T',' ')) + 19800)),
				 t.data['user_id'])f1
	WHERE f1.play_cnt=1
GROUP BY f1.india_date

SELECT f1.*
	FROM
		(SELECT to_date(from_unixtime(unix_timestamp(regexp_replace(substr(t.`timestamp`,1,19),'T',' ')) + 19800)) india_date,
				t.data['user_id'] user_id,
				COUNT(1) play_cnt
		FROM sharemax_dw.action_log t
		WHERE t.logdate BETWEEN '2019-06-15' AND '2019-06-16'
		  AND to_date(from_unixtime(unix_timestamp(regexp_replace(substr(t.`timestamp`,1,19),'T',' ')) + 19800)) ='2019-06-16'
		  AND t.data['type']='play_audio'
		GROUP BY to_date(from_unixtime(unix_timestamp(regexp_replace(substr(t.`timestamp`,1,19),'T',' ')) + 19800)),
				 t.data['user_id'])f1
WHERE f1.play_cnt=1



SELECT f1.india_date india_date,
		COUNT(DISTINCT user_id) user_cnt_1
	FROM
		(SELECT to_date(from_unixtime(unix_timestamp(regexp_replace(substr(t.`timestamp`,1,19),'T',' ')) + 19800)) india_date,
				t.data['user_id'] user_id,
				COUNT(1) play_cnt
		FROM sharemax_dw.action_log t
		WHERE t.logdate BETWEEN '2019-06-09' AND '2019-06-16'
		  AND to_date(from_unixtime(unix_timestamp(regexp_replace(substr(t.`timestamp`,1,19),'T',' ')) + 19800)) BETWEEN '2019-06-10' AND '2019-06-16'
		  AND t.data['type']='play_audio_time'
		  AND t.app_v='1.3.4'
		GROUP BY to_date(from_unixtime(unix_timestamp(regexp_replace(substr(t.`timestamp`,1,19),'T',' ')) + 19800)),
				 t.data['user_id'])f1
	WHERE f1.play_cnt=1
GROUP BY f1.india_date

SELECT t.*
FROM sharemax_dw.action_log t
WHERE t.logdate BETWEEN '2019-06-15' AND '2019-06-16'
  AND to_date(from_unixtime(unix_timestamp(regexp_replace(substr(t.`timestamp`,1,19),'T',' ')) + 19800)) ='2019-06-16'
  AND t.data['type']='play_audio_time'
  AND t.data['user_id']=20387713









-- issue#4601
-- 时间：6月以来
-- My Selfie（ID：5c4eb6e87ffa44000b8bf3a4）
-- 和selfie（ID：5bdfbaa54e8e6b5fa62f61cd）tag下发帖的用户信息，
-- 只跑Hindi用户。
-- 表头格式：用户ID/昵称/发布自拍数量/注册时间/最新活跃时间/电话/state（邦）/city/设备型号

SELECT f1.user_id user_id,
		f2.name user_name,
		f2.phone user_phone,
		to_date(from_unixtime(unix_timestamp(f2.created_at)+19800)) user_create_time,
		f4.mobile_brand_name mobile_brand_name,
		f4.mobile_model_name mobile_model_name,
		geo_loc.administrate_area_level_1 state,
		geo_loc.administrate_area_level_2 city,
		f1.selfie_cnt selfie_cnt,
		f4.latest_active_time latest_active_time
FROM
	(SELECT t.user_id user_id,
			COUNT(DISTINCT t.seq_id) selfie_cnt --发布自拍数量
	FROM sharemax_dw.feed t
	WHERE t.logdate>='2019-06-01'
	  AND t.tag_id IN('5c4eb6e87ffa44000b8bf3a4','5bdfbaa54e8e6b5fa62f61cd')
	  AND t.feed_type='image'
	GROUP BY t.user_id)f1
	LEFT JOIN user_center.funshare_user_accounts f2 --昵称name，电话phone，注册时间created_at
	ON f1.user_id=f2.user_id
	LEFT JOIN
	(SELECT f3.user_id user_id,
			f3.account_id aid
	FROM
		(SELECT t.*,
				row_number() over(PARTITION BY t.user_id ORDER BY created_at DESC) rn
		FROM dm.user_authorizations t
		WHERE t.logdate>='2019-06-01'
		  AND t.app='funshare')f3
	WHERE f3.rn=1)f3
	ON f2.user_id=f3.user_id
	LEFT JOIN
	(SELECT t.*,
	   		split(regexp_replace(cast(t.geo_location AS string),'\\\[|\\\]',''),',')[0] lat,
		    split(regexp_replace(cast(t.geo_location AS string),'\\\[|\\\]',''),',')[1] lng
	FROM
	   	   (SELECT t.user_id,
	   	           t.loc.coordinates geo_location,
	   	           row_number() over(partition BY t.user_id ORDER BY t.created_at DESC) rn
	   	   FROM sharemax_dw.location t )t
	   	   WHERE t.rn=1)user_loc
	ON f2.user_id=user_loc.user_id
	LEFT JOIN user_center.geolocation geo_loc
	ON round(CAST(user_loc.lat AS double),1)=round(CAST(geo_loc.lat AS double),1)--用户地理位置（邦state、市city）
	   AND round(CAST(user_loc.lng AS double),1)=round(CAST(geo_loc.lng AS double),1)
	LEFT JOIN
	(SELECT f4.device.advertising_id aid,
			f4.device.mobile_brand_name mobile_brand_name,--设备品牌
			f4.device.mobile_model_name mobile_model_name,--设备型号
			from_unixtime(CAST(substr(f4.event_timestamp,1,10) AS bigint)+19800) latest_active_time--最新活跃时间
	FROM
		(SELECT t.*,
				row_number() over(PARTITION BY t.device.advertising_id ORDER BY t.event_timestamp DESC) rn
		FROM sharemax_dw.funshare_firebase_impression t
		WHERE t.logdate>='2019-06-01')f4
	WHERE f4.rn=1)f4
	ON f3.aid=f4.aid
WHERE f2.current_lang='hi' --hindi用户






























