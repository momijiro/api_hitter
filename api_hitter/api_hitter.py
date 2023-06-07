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

    # Twitter
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

    # Diet
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

    def get_results(self):
        # Pseudo code to make this run without error
        # You need to replace this with actual code to get results
        return [], 0

    def write_csv(self, Records, total_num):
        # Pseudo code to make this run without error
        # You need to replace this with actual code to write csv
        pass

    def hit(self, cols, BT=None):
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
                response = self.connect_to_endpoint(self.headers, query_params)
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
            Records, total_num = self.get_results()
            self.write_csv(Records, total_num)
            # As it is not clear what should be returned here, return an empty DataFrame for now
            return pd.DataFrame()
        else:
            raise ValueError(f"Unknown API type: {self.type}")
