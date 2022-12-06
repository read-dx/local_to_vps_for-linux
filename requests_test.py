# -*- coding: utf-8 -*-
import requests
import pprint

def main():
    #GETパラメータはparams引数に辞書で指定する
    #
    url = 'http://10.100.12.105:49153/'
    params = {'lot': '123456-01', 'len': '***',}
    r = requests.get(url, params=params)
    print(r.url)