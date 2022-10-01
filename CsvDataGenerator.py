import csv
import random
from random import randint
import time
from datetime import datetime

records = 300000
start = 600000

end = start + records
ids = [start + i for i in range(records+1)]

names = ['Peter','Tony','Jack','Steve','Natasha','Bruce']
surnames = ['Parker', 'Stark', 'Rogers','Romanoff','Banner']
n = 10


def random_date(seed):
    random.seed(seed)
    d = random.randint(1, int(time.time()))
    return datetime.fromtimestamp(d).strftime('%Y-%m-%d')


with open("Users.csv", 'w', newline='') as f:
    writer = csv.writer(f)
    writer.writerow(["empId","empEmail","empFirstName","empLastName","empNumber","empDOB"])
    for i in range(0,records):
        writer.writerow([ids[i],'test'+str(i)+'@gmail.com',random.choice(names),random.choice(surnames),''.join(["{}".format(randint(0, 9)) for num in range(0, n)]),random_date(0)])
