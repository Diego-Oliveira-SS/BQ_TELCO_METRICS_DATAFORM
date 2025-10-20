import random, datetime

START = "2022-10-01"
END = "2025-10-10"

start_date = datetime.datetime.strptime(START, "%Y-%m-%d").date()
end_date = datetime.datetime.strptime(END, "%Y-%m-%d").date()
random_date = start_date + (end_date - start_date) * random.random()
random_date = datetime.datetime.combine(random_date, datetime.datetime.min.time()).date()
print(random_date)