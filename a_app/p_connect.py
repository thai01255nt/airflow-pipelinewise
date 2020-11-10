import requests, os

P_API = "http://127.0.0.1:5000/api/"

P_API_CREATE = os.path.join(P_API, "create")


def create_p(p_config):
    response = requests.post(P_API_CREATE, json=p_config)
    print(p_config)
    return eval(response.content)

