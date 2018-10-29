
--$date 新增日期
--$day::integer 测试时间
--$puzzle_langage
--$app_name

--新用户选取
create temp table temp_new_users as
select *
from fact_word.player_install_date
where app_name = $app_name
and platform in ('googleplay','ios')
and puzzle_language = $puzzle_language
and install_date = $date and bias = 'true'
;

--select * from temp_new_users order by install_Date limit 10;

--用户关卡数据_7天
create temp table temp_play_level_0 as
select a.user_id,a.app_name,a.platform,left(a.puzzle_language,2) puzzle_language,
a.ts,a.level,null as challenge_date,
a.coin_after,null as name,
b.install_date,b.first_nation,b.first_iap_date,
'play_level' as type
from raw_data_word.play_levels a
left join temp_new_users b
on a.user_id = b.user_id and a.app_name = b.app_name and a.platform = b.platform
and left(a.puzzle_language,2) = b.puzzle_language
where trunc(ts) >= install_date and trunc(ts) <= dateadd('day',$day::integer-1,install_date)
and a.app_name = $app_name
and a.platform in ('googleplay','ios')
and a.puzzle_language = $puzzle_language
;

--用户关卡数据_8-14
create temp table temp_play_level_1 as
select distinct a.user_id,a.app_name,a.platform,left(a.puzzle_language,2) puzzle_language,
'true' as retention_status,
min(ts) as retention_ts
from raw_data_word.play_levels a
left join temp_new_users b
on a.user_id = b.user_id and a.app_name = b.app_name and a.platform = b.platform
and left(a.puzzle_language,2) = b.puzzle_language
where trunc(ts) >= dateadd('day',$day::integer,install_date) and trunc(ts) <= dateadd('day',2*$day::integer-1,install_date)
and a.app_name = $app_name
and a.platform in ('googleplay','ios')
and a.puzzle_language = $puzzle_language
group by 1,2,3,4,5
;


--用户challenge关卡数据_7天
create temp table temp_challenge_play_level_0 as
select a.user_id,a.app_name,a.platform,left(a.puzzle_language,2) puzzle_language,
a.ts,(case when a.level is not null then a.level else a.challenge_level end) as level,a.challenge_date,
null::integer as coin_after,null as name,
b.install_date,b.first_nation,b.first_iap_date,
'challege_play_level' as type
from raw_data_word.challenge_play_levels a
left join temp_new_users b
on a.user_id = b.user_id and a.app_name = b.app_name and a.platform = b.platform
and left(a.puzzle_language,2) = b.puzzle_language
where trunc(ts) >= install_date and trunc(ts) <= dateadd('day',$day::integer-1,install_date)
and a.app_name = $app_name
and a.platform in ('googleplay','ios')
and a.puzzle_language = $puzzle_language
;

--用户challenge关卡数据_8-14
create temp table temp_challenge_play_level_1 as
select distinct a.user_id,a.app_name,a.platform,left(a.puzzle_language,2) puzzle_language,
'true' as retention_status,
min(ts) as retention_ts
from raw_data_word.challenge_play_levels a
left join temp_new_users b
on a.user_id = b.user_id and a.app_name = b.app_name and a.platform = b.platform
and left(a.puzzle_language,2) = b.puzzle_language
where trunc(ts) >= dateadd('day',$day::integer,install_date) and trunc(ts) <= dateadd('day',2*$day::integer-1,install_date)
and a.app_name = $app_name
and a.platform in ('googleplay','ios')
and a.puzzle_language = $puzzle_language
group by 1,2,3,4,5
;

--用户coin_gain数据_7天
create temp table temp_coin_gain_0 as
select a.user_id,a.app_name,a.platform,left(a.puzzle_language,2) puzzle_language,
a.ts,a.level,null as challenge_date,
a.coin_before + coin_award as coin_after,null as name,
b.install_date,b.first_nation,b.first_iap_date,
'coin_gain' as type
from raw_data_word.coin_gains a
left join temp_new_users b
on a.user_id = b.user_id and a.app_name = b.app_name and a.platform = b.platform
and left(a.puzzle_language,2) = b.puzzle_language
where trunc(ts) >= install_date and trunc(ts) <= dateadd('day',$day::integer-1,install_date)
and a.app_name = $app_name
and a.platform in ('googleplay','ios')
and a.puzzle_language = $puzzle_language
;

--用户coin_gain数据_8-14
create temp table temp_coin_gain_1 as
select distinct a.user_id,a.app_name,a.platform,left(a.puzzle_language,2) puzzle_language,
'true' as retention_status,
min(ts) as retention_ts
from raw_data_word.coin_gains a
left join temp_new_users b
on a.user_id = b.user_id and a.app_name = b.app_name and a.platform = b.platform
and left(a.puzzle_language,2) = b.puzzle_language
where trunc(ts) >= dateadd('day',$day::integer,install_date) and trunc(ts) <= dateadd('day',2*$day::integer-1,install_date)
and a.app_name = $app_name
and a.platform in ('googleplay','ios')
and a.puzzle_language = $puzzle_language
group by 1,2,3,4,5
;

--用户coin_cost数据_7天
create temp table temp_coin_cost_0 as
select a.user_id,a.app_name,a.platform,left(a.puzzle_language,2) puzzle_language,
a.ts,a.level,null as challenge_date,
a.coin_before - a.cost as coin_after,null as name,
b.install_date,b.first_nation,b.first_iap_date,
'coin_cost' as type
from raw_data_word.coin_costs a
left join temp_new_users b
on a.user_id = b.user_id and a.app_name = b.app_name and a.platform = b.platform
and left(a.puzzle_language,2) = b.puzzle_language
where trunc(ts) >= install_date and trunc(ts) <= dateadd('day',$day::integer-1,install_date)
and a.app_name = $app_name
and a.platform in ('googleplay','ios')
and a.puzzle_language = $puzzle_language
;

--用户coin_cost数据_8-14
create temp table temp_coin_cost_1 as
select distinct a.user_id,a.app_name,a.platform,left(a.puzzle_language,2) puzzle_language,
'true' as retention_status,
min(ts) as retention_ts
from raw_data_word.coin_costs a
left join temp_new_users b
on a.user_id = b.user_id and a.app_name = b.app_name and a.platform = b.platform
and left(a.puzzle_language,2) = b.puzzle_language
where trunc(ts) >= dateadd('day',$day::integer,install_date) and trunc(ts) <= dateadd('day',2*$day::integer-1,install_date)
and a.app_name = $app_name
and a.platform in ('googleplay','ios')
and a.puzzle_language = $puzzle_language
group by 1,2,3,4,5
;


--用户item_use_hint数据_7天
create temp table temp_item_use_hint_0 as
select a.user_id,a.app_name,a.platform,left(a.puzzle_language,2) puzzle_language,
a.ts,a.level,null as challenge_date,
null::integer as coin_after,null as name,
b.install_date,b.first_nation,b.first_iap_date,
'item_use_hint' as type
from raw_data_word.item_use a
left join temp_new_users b
on a.user_id = b.user_id and a.app_name = b.app_name and a.platform = b.platform
and left(a.puzzle_language,2) = b.puzzle_language
where trunc(ts) >= install_date and trunc(ts) <= dateadd('day',$day::integer-1,install_date)
and a.app_name = $app_name
and a.platform in ('googleplay','ios')
and a.puzzle_language = $puzzle_language
and item_type = 'hint'
;

--用户item_use_video_hint数据_7天
create temp table temp_item_use_videohint_0 as
select a.user_id,a.app_name,a.platform,left(a.puzzle_language,2) puzzle_language,
a.ts,a.level,null as challenge_date,
null::integer as coin_after,null as name,
b.install_date,b.first_nation,b.first_iap_date,
'item_use_videohint' as type
from raw_data_word.item_use a
left join temp_new_users b
on a.user_id = b.user_id and a.app_name = b.app_name and a.platform = b.platform
and left(a.puzzle_language,2) = b.puzzle_language
where trunc(ts) >= install_date and trunc(ts) <= dateadd('day',$day::integer-1,install_date)
and a.app_name = $app_name
and a.platform in ('googleplay','ios')
and a.puzzle_language = $puzzle_language
and item_type = 'video_hint'
;

--用户item_use数据_8-14
create temp table temp_item_use_1 as
select distinct a.user_id,a.app_name,a.platform,left(a.puzzle_language,2) puzzle_language,
'true' as retention_status,
min(ts) as retention_ts
from raw_data_word.item_use a
left join temp_new_users b
on a.user_id = b.user_id and a.app_name = b.app_name and a.platform = b.platform
and left(a.puzzle_language,2) = b.puzzle_language
where trunc(ts) >= dateadd('day',$day::integer,install_date) and trunc(ts) <= dateadd('day',2*$day::integer-1,install_date)
and a.app_name = $app_name
and a.platform in ('googleplay','ios')
and a.puzzle_language = $puzzle_language
group by 1,2,3,4,5
;



--用户reward_video数据_7天
create temp table temp_reward_video_0 as
select a.user_id,a.app_name,a.platform,left(a.puzzle_language,2) puzzle_language,
a.ts,a.level,null as challenge_date,
null::integer as coin_after,null as name,
b.install_date,b.first_nation,b.first_iap_date,
'reward_video' as type
from raw_data_word.reward_videos a
left join temp_new_users b
on a.user_id = b.user_id and a.app_name = b.app_name and a.platform = b.platform
and left(a.puzzle_language,2) = b.puzzle_language
where trunc(ts) >= install_date and trunc(ts) <= dateadd('day',$day::integer-1,install_date)
and a.app_name = $app_name
and a.platform in ('googleplay','ios')
and a.puzzle_language = $puzzle_language
and slot in ('coin','hint')
;

--用户reward_video数据_8-14
create temp table temp_reward_video_1 as
select distinct a.user_id,a.app_name,a.platform,left(a.puzzle_language,2) puzzle_language,
'true' as retention_status,
min(ts) as retention_ts
from raw_data_word.reward_videos a
left join temp_new_users b
on a.user_id = b.user_id and a.app_name = b.app_name and a.platform = b.platform
and left(a.puzzle_language,2) = b.puzzle_language
where trunc(ts) >= dateadd('day',$day::integer,install_date) and trunc(ts) <= dateadd('day',2*$day::integer-1,install_date)
and a.app_name = $app_name
and a.platform in ('googleplay','ios')
and a.puzzle_language = $puzzle_language
and slot in ('coin','hint')
group by 1,2,3,4,5
;

--用户sys_new_taps数据_7天
create temp table temp_sys_new_tap_0 as
select a.user_id,a.app_name,a.platform,left(a.puzzle_language,2) puzzle_language,
a.ts,a.level,null as challenge_date,
null::integer as coin_after,name,
b.install_date,b.first_nation,b.first_iap_date,
'sys_new_tap' as type
from raw_data_word.sys_new_taps a
left join temp_new_users b
on a.user_id = b.user_id and a.app_name = b.app_name and a.platform = b.platform
and left(a.puzzle_language,2) = b.puzzle_language
where trunc(ts) >= install_date and trunc(ts) <= dateadd('day',$day::integer-1,install_date)
and a.app_name = $app_name
and a.platform in ('googleplay','ios')
and a.puzzle_language = $puzzle_language
and name like '%enjoy%'
;

--用户sys_new_taps数据_8-14
create temp table temp_sys_new_tap_1 as
select distinct a.user_id,a.app_name,a.platform,left(a.puzzle_language,2) puzzle_language,
'true' as retention_status,
min(ts) as retention_ts
from raw_data_word.sys_new_taps a
left join temp_new_users b
on a.user_id = b.user_id and a.app_name = b.app_name and a.platform = b.platform
and left(a.puzzle_language,2) = b.puzzle_language
where trunc(ts) >= dateadd('day',$day::integer,install_date) and trunc(ts) <= dateadd('day',2*$day::integer-1,install_date)
and a.app_name = $app_name
and a.platform in ('googleplay','ios')
and a.puzzle_language = $puzzle_language
and name like '%enjoy%'
group by 1,2,3,4,5
;

--用户前7日数据详细数据
create temp table temp_user_events as
select * from temp_play_level_0
union
select * from temp_challenge_play_level_0
union
select * from temp_coin_gain_0
union
select * from temp_coin_cost_0
union
select * from temp_item_use_hint_0
union
select * from temp_item_use_videohint_0
union
select * from temp_reward_video_0
union
select * from temp_sys_new_tap_0
;

--用户8-14天数据
create temp table temp_user_retention as
select * from temp_play_level_1
union
select * from temp_challenge_play_level_1
union
select * from temp_coin_gain_1
union
select * from temp_coin_cost_1
union
select * from temp_item_use_1
union
select * from temp_reward_video_1
union
select * from temp_sys_new_tap_1
;

--用户前7日数据集合1
create temp table temp_user_info_7_1 as
select user_id,app_name,platform,puzzle_language,
install_date,first_nation,
max(case when type = 'play_level' then level else null end) level_max,
count(distinct trunc(ts)) load_days,
count(distinct challenge_date) challenge_gids,
count(challenge_date) challenge_games
from temp_user_events
group by 1,2,3,4,5,6
;

--用户前7日数据集合2
create temp table temp_user_info_7_2 as
select distinct user_id,app_name,platform,puzzle_language,
install_date,first_nation,
last_value(coin_after ignore nulls)  over (partition by user_id,app_name,platform,puzzle_language order by ts
rows between unbounded preceding and unbounded following) coin_after,
last_value(name ignore nulls)  over (partition by user_id,app_name,platform,puzzle_language order by ts
rows between unbounded preceding and unbounded following) enjoy_status,
last_value(first_iap_date ignore nulls)  over (partition by user_id,app_name,platform,puzzle_language order by ts
rows between unbounded preceding and unbounded following) first_iap_date
from temp_user_events
;

--用户前7日数据集合
create temp table temp_user_info_7 as
select a.*,
b.coin_after,b.enjoy_status,b.first_iap_date
from temp_user_info_7_1 a
left join temp_user_info_7_2 b
on a.user_id = b.user_id and a.app_name = b.app_name and a.platform = b.platform
and a.puzzle_language = b.puzzle_language
;


--用户后8-14天登陆情况
create temp table temp_user_info_14 as
select distinct user_id,app_name,platform,puzzle_language,retention_status,
first_value(retention_ts ignore nulls)  over (partition by user_id,app_name,platform,puzzle_language order by retention_ts
rows between unbounded preceding and unbounded following) retention_ts
from temp_user_retention
;

--7日集合  8-14retention数据
create temp table temp_user_info_before7_after7 as
select a.*,
b.retention_ts
from temp_user_info_7 a
left join temp_user_info_14 b
on a.user_id = b.user_id and a.app_name = b.app_name and a.platform = b.platform and a.puzzle_language = b.puzzle_language
order by 2,3,4,5,1
;



select install_date,app_name,count(1) from temp_user_info_before7_after7 where puzzle_language = 'En' group by 1,2 order by 2,1 desc;

select * from temp_user_info_before7_after7 where install_date = $date;



/*
delete table report_word.user_info_before7_after7 where install_date = $date;

insert into report_word.user_info_before7_after7
select * from temp_user_info_before7_after7
;

select * from temp_user_info_before7_after7 where install_date = $date;
*/
