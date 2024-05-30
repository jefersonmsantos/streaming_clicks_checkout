import redis

r = redis.Redis(host='localhost', port=6379, decode_responses=True, password = "Redis2019!")

# r.hset('user-session:123', mapping={
#     'name': 'John',
#     "surname": 'Smith2',
#     "company": 'Redis',
#     "age": 29
# })

# for key in r.scan_iter():
#     print(key)

user = r.hgetall("user_4")
print(user)