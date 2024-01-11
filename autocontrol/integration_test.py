import json
import requests
import server
import socket
import time


def integration_test():
    print('Starting integration test')

    hostname = socket.gethostname()
    IPAddr = socket.gethostbyname(hostname)
    print("The IP address of your machine is:", IPAddr)

    port = 5003
    server.start_server(host='localhost', port=port)

    time.sleep(2)
    print('Waiting for 5 seconds.')
    time.sleep(5)

    print('Stopping Flask')
    url = 'http://localhost:' + str(port) + '/shutdown'
    headers = {'Content-Type': 'application/json'}
    data = json.dumps({})
    response = requests.post(url, headers=headers, data=data)

    time.sleep(2)

    print('Integration test done.')
    print('Program exit.')


if __name__ == '__main__':
    integration_test()
