import requests

url = "http://127.0.0.1:5000/request"
data = {"tntid": "yunchi", "key": "default_value"}
data_ = {"tntid": "boo", "key": "default_value"}

response = requests.post(url, data=data)
response = requests.post(url, data=data_)

if response.status_code == 200:
    print(response.text)
else:
    print(f"POST request failed with status code {response.status_code}")
