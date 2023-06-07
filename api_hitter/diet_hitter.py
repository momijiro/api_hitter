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
