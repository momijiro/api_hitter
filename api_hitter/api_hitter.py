# api_hitter.py
import requests
import pandas as pd
import datetime
from pandas import json_normalize

class TwitterHitter:
    def __init__(self):
        self.url = "https://api.twitter.com/2/tweets/search/all"

    def make_param(self, keyword, start_time, end_time):
        query_params = {'query': keyword,
                        'tweet.fields': 'created_at,referenced_tweets',
                        'expansions': 'author_id,geo.place_id,in_reply_to_user_id',
                        'start_time': start_time,
                        'end_time': end_time,
                        'user.fields': 'created_at,description,location,username',
                        'max_results':500,  # maximum number of results per query
                        'next_token' : {} # param for next page
                        }
        return query_params

    def create_headers(self):
        headers = {"Authorization": "Bearer {}".format(self.BT)}
        return headers

    def connect_to_endpoint(self, headers, params):
        response = requests.request("GET", self.url, headers=headers, params=params)
        if response.status_code != 200:
            raise Exception(response.status_code, response.text)
        return response

    def json_to_df(self, json):
        data = json_normalize(json['data'])
        user = json_normalize(json['includes']['users']).set_index('id')

        data.rename({'geo.place_id': 'place_id'}, inplace = True, axis = 1)
        user.rename({'id': 'author_id', 'created_at':'created_at_user'}, inplace = True, axis = 1)
        df = data.join(user, on = 'author_id', how = 'left')

        return df

    def adjust_created_at(self, df):
        df['created_at'] = df['created_at'].map(lambda x: (pd.to_datetime(x) + datetime.timedelta(hours=9)).strftime('%Y-%m-%d_%H:%M:%S'))
        df = df.sort_values('created_at').reset_index(drop=True)
        return df

    def get_japan_time(self, original_time):
        original_time = datetime.datetime.strptime(original_time, '%Y-%m-%dT%H:%M:%SZ')
        jp_time = original_time - datetime.timedelta(hours=9)
        return jp_time.strftime('%Y-%m-%dT%H:%M:%SZ')

    def hit(self, cols, BT):
        self.BT = BT
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


import requests
import time
import sys
import csv
import re

class DietHitter:
    def __init__(self, text_input, house_input, speaker_input, from_input, until_input):
        self.base_url = "https://kokkai.ndl.go.jp/api/speech"
        self.text_input = text_input
        self.house_input = house_input
        self.speaker_input = speaker_input
        self.from_input = from_input
        self.until_input = until_input
        self.params_01 = {}
        self.params_02 = {}

    def input_validation(self):
        if self.text_input:
            self.params_01['any'] = self.text_input
            self.params_02['any'] = self.text_input

        if self.house_input:
            self.params_01['nameOfHouse'] = self.house_input
            self.params_02['nameOfHouse'] = self.house_input

        if self.speaker_input:
            self.params_01['speaker'] = self.speaker_input
            self.params_02['speaker'] = self.speaker_input

        if re.match(r'[0-9]{4}-[0-1][0-9]-[0-3][0-9]', self.from_input):
            self.params_01['from'] = self.from_input
            self.params_02['from'] = self.from_input
        else:
            self.params_01['from'] = "2020-09-01"
            self.params_02['from'] = "2020-09-01"
            print("'From' date is set to 2020-09-01 due to invalid input")

        if re.match(r'[0-9]{4}-[0-1][0-9]-[0-3][0-9]', self.until_input):
            self.params_01['until'] = self.until_input
            self.params_02['until'] = self.until_input
        else:
            self.params_01['until'] = "2020-09-30"
            self.params_02['until'] = "2020-09-30"
            print("'Until' date is set to 2020-09-30 due to invalid input")

    def get_results(self):
        self.params_01['maximumRecords'] = 1
        self.params_01['recordPacking'] = "json"
        response_01 = requests.get(self.base_url, self.params_01) 
        jsonData_01 = response_01.json()

        try:
            total_num = jsonData_01["numberOfRecords"]
        except:
            print("クエリエラーにより取得できませんでした。")
            sys.exit()

        next_input = input("検索結果は " + str(total_num) + "件です。\nキャンセルする場合は 1 を、データを取得するにはEnterキーまたはその他を押してください。 >> ")
        if next_input == "1":
            print('プログラムをキャンセルしました。')
            sys.exit()

        max_return = 100 
        pages = (int(total_num) // int(max_return)) + 1 
        self.params_02['maximumRecords'] = max_return
        self.params_02['recordPacking'] = "json"
        Records = []

        for i in range(pages):
            i_startRecord = 1 + (i * int(max_return))
            self.params_02['startRecord'] = i_startRecord
            response_02 = requests.get(self.base_url, self.params_02)
            jsonData_02 = response_02.json()

            for list in jsonData_02['speechRecord']:
                list_speech = list['speech'].replace('\r\n', ' ').replace('\n', ' ') 
                Records.append([list['speechID'], list['issueID'], list['imageKind'], list['nameOfHouse'], list['nameOfMeeting'], 
                                list['issue'], list['date'], list['speechOrder'], list['speaker'], list['speakerGroup'], 
                                list['speakerPosition'], list['speakerRole'], list_speech, list['speechURL'], list['meetingURL']])

            sys.stdout.write("\r%d/%d is done." % (i+1, pages))
            time.sleep(0.5)
        return Records, total_num

    def write_csv(self, Records, total_num):
        with open(f"diet_{self.text_input}_{self.from_input}_{self.until_input}.csv", 'w', newline='') as f:
            csvwriter = csv.writer(f, delimiter=',', quotechar='"', quoting=csv.QUOTE_NONNUMERIC)
            csvwriter.writerow(['発言ID', '会議録ID', '種別', '院名', '会議名', '号数', '日付', '発言番号', '発言者名', '発言者所属会派', '発言者肩書き', '発言者役割', '発言内容', '発言URL', '会議録URL'])
            for record in Records:
                csvwriter.writerow(record)

if __name__ == '__main__':
    diethitter = DietHitter("高齢運転", "", "", "2019-01-01", "2022-12-31")
    diethitter.input_validation()
    Records, total_num = diethitter.get_results()
    diethitter.write_csv(Records, total_num)
