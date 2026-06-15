import os
import json
import telebot
import pika
from fluent import sender

BOT_TOKEN = os.getenv('PRODUCER_BOT_TOKEN')
RABBITMQ_HOST = os.getenv('RABBITMQ_HOST', 'localhost')
FLUENTD_HOST = os.getenv('FLUENTD_HOST', 'localhost')

logger = sender.FluentSender('producer_bot', host=FLUENTD_HOST, port=24224)

bot = telebot.TeleBot(BOT_TOKEN)


def send_to_rabbitmq(routing_key, payload):
    connection = pika.BlockingConnection(pika.ConnectionParameters(host=RABBITMQ_HOST))
    channel = connection.channel()

    channel.exchange_declare(exchange='support_exchange', exchange_type='direct')

    channel.basic_publish(
        exchange='support_exchange',
        routing_key=routing_key,
        body=json.dumps(payload)
    )
    connection.close()


@bot.message_handler(commands=['start'])
def start(message):
    bot.send_message(message.chat.id,
                     "\nВикористовуйте /order <текст> для здійснення замовлення.\nВикористовуйте /review <текст> того щоб залишити відгук!")


@bot.message_handler(commands=['order', 'review'])
def handle_requests (message):
    command = message.text.split()[0]
    text = message.text.replace(command, '').strip()

    logger.emit('incoming_message', {
        'user_id': message.from_user.id,
        'command': command,
        'status': 'received',
        'text': text 
    })

    if not text:
        bot.send_message (message.chat.id, f"Введіть текст після {command}")
        logger.emit('error', {'user_id': message.from_user.id, 'reason': 'empty_text'})
        return
    payload = {
        "user_id": message.from_user.id,
        "username": message.from_user.username or "Unknown",
        "text": text
    }
    try:
        if command == '/review':
            send_to_rabbitmq('route.review', payload)
            bot.reply_to (message, "Відгук прийнято")
            logger.emit('action', {'type': 'review_sent_to_queue', 'user': payload['username'], 'text': text})
        elif command =='/order':
            send_to_rabbitmq('route.order', payload)
            bot.reply_to(message, " Ваше замовлення отримано, очікуйте повідомлення від опертора")
            logger.emit('action', {'type': 'order_sent_to_queue', 'user': payload['username'], 'text': text})
    except Exception as e:
        bot.reply_to (message, "Помилка сервера((((")
        print(e)
        logger.emit('system_error', {'error_message': str(e)})

if __name__ == "__main__":
    print("Бот-продюсер успішно запущений і чекає на команди...")
    bot.infinity_polling()