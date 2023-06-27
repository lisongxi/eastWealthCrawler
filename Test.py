from datetime import datetime, date, timedelta

# 将字符串转换为日期对象
date_str = "2023-06-26"
print(type(date_str))
target_date = datetime.strptime(date_str, "%Y-%m-%d").date()

# 获取今天的日期
today = date.today()
print(
    today
)

# 计算时间差
delta = target_date - today
delta_days = delta.days

print("今天与 %s 之间的天数差为: %d" % (date_str, delta_days))