max_N = 0
take_rate = 0.06
bonus_pool = 0
user_cnt = 5000

# 抽成收益
take_rate_money = 0

# 游戏明细数据
game_round = []
money_list = []
user_win_amount = []
user_max_amount = []
user_min_amount = []
user_result_detail=[]
bonus_pools = []
win_user_cnt = []
final_win_horses = []
odds_list = []

for game_time in range(1000):
    game_round.append(game_time)
    # 随机选择展示的6匹马
    display_choice = game_choice.iloc[random.randint(0,215),:]

    # 5000用户随机选择一个投注金额选项
    bet_choices = [50, 100, 1000, 10000]
    bet_money = random.choices(bet_choices, k = user_cnt)

    # 5000用户从6匹马中随机选择一匹或多匹马下注
    horses = display_choice.iloc[0:6].to_list()

    user_bet = []
    horse_cnt = []
    for u in range(user_cnt):
        bet_horse_cnt = random.randint(1,6)
        bet_horses = random.sample(horses, bet_horse_cnt)
        user_bet.append(bet_horses)
        horse_cnt.append(bet_horse_cnt)

    # 5000用户选择下注次数: 1到4次
    cnt_choices = [1,2,3,4]
    bet_cnt = random.choices(cnt_choices, k = user_cnt)

    # 计算本次每个用户下注金额 money_user, 下注金额money_ttl,及奖池金额 bonus_pool
    money_user = np.multiply(np.multiply(np.array(bet_money),np.array(bet_horse_cnt)),np.array(bet_cnt))
    money_ttl = money_user.sum()
    money_list.append(money_ttl)
    bonus_pool += money_ttl*(1-take_rate)
    take_rate_money += money_ttl*take_rate

    # 奖池上限 N
    if game_time <= 9:
        max_N += money_ttl/10*2+3000
        if game_time >=10:
            max_N = (np.array(money_list[game_time-9,game_time]).sum())/10*2+3000

    # 计算每匹马的赔率
    score_sum = 0
    horse_odds = []
    for h in range(6):
        score_sum += horse_score[horses[h]]
    for hs in horses:
        odds = score_sum/horse_score[hs] * (1-take_rate)
        horse_odds.append(odds)

    # 计算每匹马获胜需要返给用户的金币数
    return_coins = []
    for win_horse in horses:
        return_coin = 0
        for n in range(user_cnt):
            if win_horse in user_bet[n]:
                return_coin += bet_money[n]*bet_cnt[n]* horse_odds[horses.index(win_horse)]
        return_coins.append(return_coin)

    # 选择返回金币数最少的马获胜
    final_win_horse = horses[return_coins.index(min(return_coins))]
    final_win_horses.append(final_win_horse)
    bonus_pool -= min(return_coins)
    bonus_pools.append(bonus_pool)

    # 计算获胜的马的赔率
    odds_list.append(horse_odds[horses.index(final_win_horse)])

    # 计算该轮游戏每个用户最终获得金币列表
    user_result=[]
    for u in range(user_cnt):
        if final_win_horse in user_bet[u]:
            user_result.append(bet_money[u]*bet_cnt[u]*horse_odds[horses.index(final_win_horse)] -list(money_user)[u])
        else:
            user_result.append(-bet_money[u]*bet_cnt[u])

    # 记录每轮游戏用整体用户最终金币得失情、用户结果明细
    user_win_amount.append(sum(user_result))
    user_max_amount.append(max(user_result))
    user_min_amount.append(min(user_result))
    user_result_detail.append(user_result)
    ur_np = np.array(user_result)
    win_user_cnt.append(ur_np[np.where(ur_np<0)].shape[0])









max_N = 0
take_rate = 0.06
bonus_pool = 0
user_cnt = 5000

# 抽成收益
take_rate_money = 0

# 游戏明细数据
game_round = []
money_list = []
user_win_amount = []
user_max_amount = []
user_min_amount = []
user_result_detail=[]
bonus_pools = []
win_user_cnt = []
final_win_horses = []
odds_list = []

for game_time in range(1000):
    game_round.append(game_time)
    # 随机选择展示的6匹马
    display_choice = game_choice.iloc[random.randint(0,215),:]

    # 5000用户随机选择一个投注金额选项
    bet_choices = [50, 100, 1000, 10000]
    bet_money = random.choices(bet_choices, k = user_cnt)

    # 5000用户从6匹马中随机选择一匹或多匹马下注
    horses = display_choice.iloc[0:6].to_list()

    user_bet = []
    horse_cnt = []
    for u in range(user_cnt):
        bet_horse_cnt = random.randint(1,6)
        bet_horses = random.sample(horses, bet_horse_cnt)
        user_bet.append(bet_horses)
        horse_cnt.append(bet_horse_cnt)

    # 5000用户选择下注次数: 1到4次
    cnt_choices = [1,2,3,4]
    bet_cnt = random.choices(cnt_choices, k = user_cnt)
    # print(bet_cnt)

    # 计算本轮游戏每个用户下注金额 money_user, 下注总金额money_ttl,及奖池金额 bonus_pool
    money_user = np.multiply(np.multiply(np.array(bet_money),np.array(horse_cnt)),np.array(bet_cnt))
    money_ttl = money_user.sum()
    money_list.append(money_ttl)
    bonus_pool += money_ttl*(1-take_rate)
    take_rate_money += money_ttl*take_rate
    # print(money_user)

    # 奖池上限 N
    if game_time <= 9:
        max_N += money_ttl/10*2+3000
        if game_time >=10:
            max_N = (np.array(money_list[game_time-9,game_time]).sum())/10*2+3000

    # 计算本轮游戏每匹马的赔率 horse_odds
    score_sum = 0
    horse_odds = []
    for h in range(6):
        score_sum += horse_score[horses[h]]
    for hs in horses:
        odds = score_sum/horse_score[hs] * (1-take_rate)
        horse_odds.append(odds)

    # 计算本轮游戏每匹马获胜需要返给5000名用户的总金币数 return_coins
    return_coins = []
    for win_horse in horses:
        return_coin = 0
        for n in range(user_cnt):
            if win_horse in user_bet[n]:
                return_coin += bet_money[n]*bet_cnt[n]*horse_odds[horses.index(win_horse)]
        return_coins.append(return_coin)

    # 选择在奖池范围内的马获胜,若无,选择return_coins最少的马获胜
    tmp_horse = []
    for r in range(6):
        if abs(bonus_pool - return_coins[r]) <= max_N:
            tmp_horse.append(horses[r])

    if len(tmp_horse) == 0:
        final_win_horse = horses[return_coins.index(min(return_coins))]
    else:
        final_win_horse = random.choice(tmp_horse)

    final_win_horses.append(final_win_horse)
    bonus_pool -= return_coins[horses.index(final_win_horse)]
    bonus_pools.append(bonus_pool)

    # 记录最终获胜的马的赔率
    odds_list.append(horse_odds[horses.index(final_win_horse)])

    # 计算该轮游戏每个用户最终金币净收益
    user_result=[]
    for u in range(user_cnt):
        if final_win_horse in user_bet[u]:
            user_result.append(bet_money[u]*bet_cnt[u]*horse_odds[horses.index(final_win_horse)] -list(money_user)[u])
        else:
            user_result.append(-bet_money[u]*bet_cnt[u])

    # 记录每轮游戏用整体用户最终金币得失情况、用户结果明细
    user_win_amount.append(sum(user_result))
    user_max_amount.append(max(user_result))
    user_min_amount.append(min(user_result))
    user_result_detail.append(user_result)
    ur_np = np.array(user_result)
    win_user_cnt.append(ur_np[np.where(ur_np>0)].shape[0])




【版本1】

max_N = 0
take_rate = 0.1
bonus_pool = 0
user_cnt = 100

# 抽成收益
take_rate_money = 0

# 游戏明细数据
game_round = []
money_list = []
final_return_coins = []
user_get_amount = []
user_max_amount = []
user_min_amount = []
bonus_pools = []
win_user_cnt = []
final_win_horses = []
odds_list = []

# 用户明细数据
money_user_cum = np.zeros((user_cnt,))
user_result_detail = np.zeros((user_cnt,))

for game_time in range(1000):
#     print('='*10)
#     print('game_time:',game_time)
    game_round.append(game_time)
    # 随机选择展示的6匹马
    display_choice = game_choice.iloc[random.randint(0,215),:]

    # 5000用户随机选择一个投注金额选项
    bet_choices = [50, 100, 1000, 10000]
    bet_money = random.choices(bet_choices, k = user_cnt)
#     print('bet_money:', bet_money)

    # 5000用户从6匹马中随机选择一匹或多匹马下注
    horses = display_choice.iloc[0:6].to_list()
#     print('horses:', horses)

    user_bet = []
    horse_cnt = []
    for u in range(user_cnt):
        bet_horse_cnt = random.randint(1,6)
        bet_horses = random.sample(horses, bet_horse_cnt)
        user_bet.append(bet_horses)
        horse_cnt.append(bet_horse_cnt)
#     print('user_bet:', user_bet)
#     print('horse_cnt:', horse_cnt)

    # 5000用户选择下注次数: 1到4次
    cnt_choices = [1,2,3,4]
    bet_cnt = random.choices(cnt_choices, k = user_cnt)

    # 计算本次每个用户下注金额 money_user, 下注金额 money_ttl,及奖池金额 bonus_pool
    money_user = np.multiply(np.multiply(np.array(bet_money),np.array(horse_cnt)),np.array(bet_cnt))
    money_user_cum += money_user
    money_ttl = money_user.sum()
    money_list.append(money_ttl)
    bonus_pool += money_ttl*(1-take_rate)
    take_rate_money += money_ttl*take_rate
#     print('bonus_pool:',bonus_pool)
#     print('money_user:',money_user)
#     print('money_ttl:', money_ttl)

    # 奖池上下限 N
    if game_time <= 9:
        max_N += money_ttl/10*2+3000
        if game_time >=10:
            max_N = (np.array(money_list[(game_time-10):game_time]).sum())/10*2+3000
#     print('max_N:', max_N)

    # 计算本轮游戏每匹马的赔率 horse_odds
    score_sum = 0
    horse_odds = []
    for h in range(6):
        score_sum += horse_score[horses[h]]
    for hs in horses:
        odds = score_sum/horse_score[hs] * (1-take_rate)
        horse_odds.append(odds)

    # 计算本轮游戏每匹马获胜需要返给5000名用户的总金币数 return_coins
    return_coins = []
    for win_horse in horses:
        return_coin = 0
        for n in range(user_cnt):
            if win_horse in user_bet[n]:
                return_coin += bet_money[n]*bet_cnt[n]*horse_odds[horses.index(win_horse)]
            else:
                return_coin += 0
        return_coins.append(return_coin)
#     print('return_coins:',return_coins)

    # 选择在奖池范围内的马获胜,若无,选择return_coins最少的马获胜
    tmp_horse = []
    for r in range(6):
        if abs(bonus_pool - return_coins[r]) <= max_N:
            tmp_horse.append(horses[r])

    if len(tmp_horse) == 0:
        final_win_horse = horses[return_coins.index(min(return_coins))]
    else:
        final_win_horse = random.choice(tmp_horse)

    final_win_horses.append(final_win_horse)
    final_return_coins.append(return_coins[horses.index(final_win_horse)])
    bonus_pool -= return_coins[horses.index(final_win_horse)]
    bonus_pools.append(bonus_pool)
#     print('final_win_horse:', final_win_horse)

    # 计算获胜的马的赔率
    odds_list.append(horse_odds[horses.index(final_win_horse)])

    # 计算该轮游戏每个用户最终获得金币收益user_result, 获奖金币user_reward
    user_result=[]
    user_reward=[]
    for u in range(user_cnt):
        if final_win_horse in user_bet[u]:
            reward = bet_money[u]*bet_cnt[u]*horse_odds[horses.index(final_win_horse)]
            user_reward.append(reward)
            user_result.append(reward - (list(money_user)[u]))
        else:
            user_reward.append(0)
            user_result.append(-list(money_user)[u])
#     print('user_result:', user_result)

    # 记录每轮游戏用整体用户最终金币得失情况、用户结果明细
    user_get_amount.append(sum(user_result))
    user_max_amount.append(max(user_result))
    user_min_amount.append(min(user_result))
    user_result_detail += np.array(user_reward)
    ur_np = np.array(user_result)
    win_user_cnt.append(ur_np[np.where(ur_np>0)].shape[0])





游戏配置：


参与游戏总人数：1000
每轮游戏游戏参与人数：20-50人之间
游戏轮数：2000

下注花费：随机从列表里选一个

下注游戏选项：随机选取1到6个
每个游戏选项下注次数：1次

抽成：10%
奖池阀值：N=最近10次用户下注总金额 / 10 * 2 + 3000

下注花费选项
1000
100
200
100
500
50
200
50
100
1000
10000
5000





