from main import redis, Order
import time

key = 'refund_order'
group = 'payment-group'

try:
    redis.xgroup_create(key, group)
except:
    print("group already exists!")

while True:
    try:
        results = redis.xreadgroup(group, key, {key:'>'}, None)
        if(results != []):
            print("results", results)
            for result in results:
                obj = result[1][0][1]
                order = Order.get(obj['pk'])
                order.status = 'refund'
                
    except Exception as e:
        print(e)
    time.sleep(1)