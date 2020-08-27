--了解数据

SELECT 
       a.lang lang,
       a.tag_id tag_id,
       a.tag_title tag_title,
       a.tag_title_eng tag_title_eng,
       a.feed_cnt feed_cnt,
       b.alltag_feed_cnt alltag_feed_cnt,
       a.video_post_cnt video_post_cnt,
       b.alltag_video_post_cnt alltag_video_post_cnt,
       a.refresh_feed_cnt refresh_feed_cnt,
       b.alltag_refresh_cnt alltag_refresh_cnt,
       a.video_refresh_cnt video_refresh_cnt,
       a.share_cnt share_cnt,
       a.like_cnt like_cnt,
       a.fav_cnt fav_cnt,
       a.refresh_cnt AS refresh_cnt,
       a.refresh_user_cnt AS refresh_user_cnt
FROM
  (SELECT to_date(from_unixtime(unix_timestamp(t.refresh_time)+19800)) india_date,--UTC时间与印度时区之间相差时间，19800s=5.5h
          t1.lang,
          t1.id AS tag_id,
          t1.title AS tag_title,
          t1.en_title AS tag_title_eng,
          count(DISTINCT concat(t.user_action_refresh_feed_id,t.refresh_refresh_id)) refresh_feed_cnt,--刷新量，刷新一次会有8篇feed，组合得到总刷新量
          count(DISTINCT t.refresh_refrerefresh_id) AS refresh_cnt,--刷新次数
          count(DISTINCT case when t.feed_feed_type = 'video' then concat(t.user_action_refresh_feed_id,t.refresh_refresh_id) end) AS video_refresh_cnt,--该tag下该源视频刷新量
          count(DISTINCT t.refresh_uid) AS refresh_user_cnt,--刷新人数
          count(DISTINCT CASE
                             WHEN t.user_action_action_type='like' THEN t.user_action_action_id
                             ELSE NULL
                         END) like_cnt,--点赞率
          count(DISTINCT CASE
                             WHEN t.user_action_action_type='share' THEN t.user_action_action_id
                             ELSE NULL
                         END) share_cnt,--分享率
          count(DISTINCT CASE
                             WHEN t.user_action_action_type IN('fav','save') THEN t.user_action_action_id
                             ELSE NULL
                         END) fav_cnt,--下载
          count(DISTINCT t.user_action_refresh_feed_id) AS feed_cnt,--发帖量
          count(DISTINCT CASE
                             WHEN t.feed_feed_type='video' THEN t.user_action_refresh_feed_id
                             ELSE NULL
                         END) AS video_post_cnt--视频发帖量
   FROM dm.sharemax_feed_action_new t
   INNER JOIN sharemax_dw.tag t1 ON t.feed_tag_id = t1.id
   AND t1.category_id IN ('5ae43c4c99f58a169c752e48',
                          '5ae43c4d99f58a169c752f26',
                          '5ae43c4c99f58a169c752e99')
   WHERE t.refresh_refresh_id IS NOT NULL
     AND t.logdate BETWEEN date_sub('2019-01-27',1) AND '2019-01-27'
     AND to_date(from_unixtime(unix_timestamp(t.refresh_time)+19800)) = '2019-01-27'
     AND t.feed_india_date = '2019-01-27'
   GROUP BY to_date(from_unixtime(unix_timestamp(t.refresh_time)+19800)),
            t1.lang,
            t1.id,
            t1.title,
            t1.en_title)a
JOIN
  (SELECT to_date(from_unixtime(unix_timestamp(t.refresh_time)+19800)) india_date,
          t.refresh_lang AS lang,
          count(DISTINCT concat(t.user_action_refresh_feed_id,t.refresh_refresh_id)) alltag_refresh_cnt,--所有tag刷新量
          count(DISTINCT t.user_action_refresh_feed_id) AS alltag_feed_cnt,--所有tag发帖量
          count(DISTINCT CASE
                             WHEN t.feed_feed_type='video' THEN t.user_action_refresh_feed_id
                             ELSE NULL
                         END) AS alltag_video_post_cnt--所有tag视频量
   FROM dm.sharemax_feed_action_new t
   WHERE t.refresh_refresh_id IS NOT NULL
     AND t.logdate BETWEEN date_sub('2019-01-27',1) AND '2019-01-27'
     AND to_date(from_unixtime(unix_timestamp(t.refresh_time)+19800)) = '2019-01-27'
     AND t.feed_india_date = '2019-01-27'
   GROUP BY to_date(from_unixtime(unix_timestamp(t.refresh_time)+19800)),
            t.refresh_lang)b 
  ON a.india_date=b.india_date
  AND a.lang = b.lang