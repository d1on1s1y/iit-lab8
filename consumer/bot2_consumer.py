import os
import json
import time
import telebot
import pika
import schedule
from fluent import sender

BOT_TOKEN = os.getenv('CONSUMER_BOT_TOKEN')
ADMIN_ID = os.getenv('ADMIN_ID')
RABBITMQ_HOST = os.getenv('RABBITMQ_HOST', 'localhost')
FLUENTD_HOST = os.getenv('FLUENTD_HOST', 'localhost')

logger = sender.FluentSender('consumer_bot', host=FLUENTD_HOST, port=24224)

bot = telebot.TeleBot(BOT_TOKEN)

def process_queue(queue_name, routing_key, queue_type):
    try:
        connection = pika. BlockingConnection(pika.ConnectionParameters (host=RABBITMQ_HOST))
        channel = connection.channel()
    
        channel.exchange_declare (exchange='support_exchange', exchange_type='direct')
        channel.queue_declare (queue = queue_name)
        channel.queue_bind(exchange='support_exchange', queue = queue_name, routing_key=routing_key)
        messages_found = False
        while True:
            method_frame, header_frame, body = channel.basic_get(queue=queue_name, auto_ack=False)
            if method_frame:
                messages_found = True
                data = json.loads(body)

                logger.emit('queue_read', {'queue': queue_name, 'user_id': data['user_id'], 'text': data['text']})
                
                if queue_type == 'order':
                    msg_text = f" <b>НАДІЙШЛО ЗАМОВЛЕННЯ</b> !! \nВід: @{data['username']} \nТекст: {data['text']}"
                elif queue_type == 'review':
                    msg_text=f" <b>Отримано відгук</b>\nвід: @{data['username']}\nТекст: {data['text']}"
                bot.send_message(ADMIN_ID, msg_text, parse_mode='HTML')
                channel.basic_ack(delivery_tag=method_frame.delivery_tag)

                logger.emit('message_processed', {'queue': queue_name, 'status': 'success'})
            else:
                break

            print("[{time.strftime(%H:%M:%S')}] Планова перевірка: Знайдено нові відгуки.")
        connection.close()
    except Exception as e:
        print (f"Помилка з'єднання: {e}")
        logger.emit('system_error', {'error': str(e)})

def check_order():
    process_queue(queue_name='queue_order', routing_key='route.order', queue_type='order')

def check_review():
    process_queue (queue_name='queue_review', routing_key='route.review', queue_type='review')

schedule.every(5).seconds.do(check_order)
schedule.every (1).minutes.do(check_review)

if __name__ == '__main__':
    print("Consumer Bot started with Priority Routing...")
    check_order()
    check_review()
    while True:
        schedule.run_pending()
        time.sleep(1)