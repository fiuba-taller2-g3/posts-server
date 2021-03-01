from pyfcm import FCMNotification

from tokens_service import tokens


def send_notification(user_id, notification_title, notification_body):
    push_service = FCMNotification(
        api_key="AAAAxiaBS5E:APA91bGxW95dHwwRhhq5gEET2_M_zZ4QrzfCWEkezL5TTEXh8_m-9132m6yG6Y_5RrKcNrZlPrs5RP8JuD3ax-vpf84FCuhckNFnqIe0pzhZozodXmqfi6g04v7s3WYa0HR4jydj7YCg")

    try:
        registration_id = tokens.get(user_id)
        message_title = notification_title
        message_body = notification_body
        return push_service.notify_single_device(registration_id=registration_id, message_title=message_title,
                                                 message_body=message_body)
    except:
        print("No se encontró token id para el user id " + user_id)
