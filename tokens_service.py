tokens = {}


def save_token(user_id, token_id):
    print("Guardando el token: " + token_id + " para el usuario: " + user_id)
    tokens[user_id] = token_id


def reset_tokens():
    tokens.clear()


def get_tokens():
    return tokens

