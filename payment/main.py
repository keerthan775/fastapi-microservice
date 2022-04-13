from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from fastapi.background import BackgroundTasks
from redis_om import get_redis_connection, HashModel
from starlette.requests import Request
import requests
import time

app = FastAPI()

app.add_middleware(
    CORSMiddleware,
    allow_origins = ['http://localhost:8000'],
    allow_methods = ['*'],
    allow_headers = ['*']
)

#This should be different database
redis = get_redis_connection(
    host = 'redis-10064.c264.ap-south-1-1.ec2.cloud.redislabs.com',
    port = 10064,
    password = 'J89OZkGAStqjJYmp7Ml7NhEBhLalcBFa',
    decode_responses = True
)

class Order(HashModel):
    product_id : str
    price      : float
    fee        : float
    total      : float
    quantity   : str 
    status     : str #pending, completed, refunded

    class Meta:
        database = redis

@app.get('/orders/{pk}')
def get(pk:str):
    order = Order.get(pk)
    return order

@app.get('/orders')
def all():
    return [format(pk) for pk in Order.all_pks()]

def format(pk : str):
    order = Order.get(pk)
    return {
        'id'        : order.pk,
        'product_id': order.product_id,
        'price'     : order.price,
        'fee'       : order.fee,
        'quantity'  : order.quantity,
        'status'    : order.status
    }

@app.post('/orders')
async def create(request:Request, background_task:BackgroundTasks): # id, quantity
    body = await request.json()

    req = requests.get('http://localhost:8000/products/%s' %body['id'])
    print("req", req)
    product = req.json()
    order = Order(
        product_id = body['id'],
        price = product['price'],
        fee = 0.2 * product['price'],
        total = 1.2 * product['price'],
        quantity = body['quantity'],
        status = 'pending'
    )
    order.save()
    #order_completed(order)
    background_task.add_task(order_completed, order)
    return order

def order_completed(order:Order):
    time.sleep(5)
    order.status = 'completed'
    order.save()
    redis.xadd('order_completed', order.dict(), '*')

@app.delete('/order/{pk}')
def delete(pk :str):
    return Order.delete(pk)