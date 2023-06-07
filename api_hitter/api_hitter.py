# api_hitter.py
import requests
import pandas as pd
import datetime
from pandas import json_normalize

class ApiHitter:
    def __init__(self, api_type, **kwargs):
        if api_type == 'twitter':
            self.url = "https://api.twitter.com/2/tweets/search/all"
            self.base_url = None
            self.params = {}
            self.headers = {"Authorization": "Bearer {}".format(kwargs.get('BT'))}
        elif api_type == 'diet':
            self.base_url = "https://kokkai.ndl.go.jp/api/speech"
            self.url = None
            self.params = self.input_validation(kwargs)
            self.headers = {}
        else:
            raise ValueError(f"Unknown API type: {api_type}")
        self.type = api_type

    def input_validation(self, kwargs):
        params = {}
        if 'text_input' in kwargs:
            params['any'] = kwargs['text_input']
        if 'house_input' in kwargs:
            params['nameOfHouse'] = kwargs['house_input']
        if 'speaker_input' in kwargs:
            params['speaker'] = kwargs['speaker_input']
        # Add more validations as required...
        return params

    # Add more methods as required...

    def connect_to_endpoint(self, headers, params):
        response = requests.request("GET", self.url, headers=headers, params=params)
        if response.status_code != 200:
            raise Exception(response.status_code, response.text)
        return response

    # Add more methods as required...

def hit(self, cols, BT):
    if self.type == 'twitter':
        keyword = cols['keyword']
        start_time = self.get_japan_time(cols['start_time'])
        end_time = self.get_japan_time(cols['end_time'])

        count = 0
        flag = True
        df =  pd.DataFrame()

        # make_param
        query_params = self.make_param(keyword, start_time, end_time)
        print(f'start with keyword "{keyword}"')

        flag = True
        while flag:
            headers = self.create_headers()
            response = self.connect_to_endpoint(headers, query_params)
            json_response = response.json()

            df_tmp = self.json_to_df(json_response)
            # concat df
            df = pd.concat([df, df_tmp], ignore_index=True)
            print("total:" + str(len(df)))

            # check next_token
            if 'next_token' in json_response['meta']:
                query_params['next_token'] = json_response['meta']['next_token']
            else:
                flag = False

        df = self.adjust_created_at(df)
        print(f'finish collecting {df.shape}')
        return df

    elif self.type == 'diet':
        self.input_validation()
        Records, total_num = self.get_results()
        self.write_csv(Records, total_num)
    else:
        raise ValueError(f"Unknown API type: {self.type}")

