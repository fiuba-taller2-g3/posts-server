from pyfcm import FCMNotification

from tokens_service import tokens


def send_notification(user_id, notification_title, notification_body):
    push_service = FCMNotification(
        api_key="AAAAlBhRVjc:APA91bHgKK5gKNTEw-83ZQ9RgA17tT1ZAohbHyI4ZEzSrBJ1kHdJ4NsvvtsRZa-nDjuYklrqyUuEu3lS3orNrep1xwuX4AgWFMBkinUr5AklaRzYJPSQhOMFSiBPBq64CMFhR7NTCt-N")

    # registration_id = "cjDylnSMQvKOX0qa8-JYUA:APA91bHKXAMahYiIHdfNjEtBidPClrfv5fGtmQAue_n-HwYP3WWXYkFKQAjuNP0EQ6AdHDr_eKsYBQAOhdrSvgG_goUm1ttZRDUisHlei6LlzqALSJomg9QFvJnjJOSM90fdKGiKC5MO"

    try:
        registration_id = tokens.get(user_id)
        message_title = notification_title
        message_body = notification_body
        return push_service.notify_single_device(registration_id=registration_id, message_title=message_title,
                                                 message_body=message_body)
    except:
        print("No se encontró token id para el user id " + user_id)
