from flask import Flask, request
import sqlite3
import App_SQLite_Rider_Ranking as rr


class web_server:
    def __init__(self):
        # create app
        self.app = Flask(__name__)

        # web api setting
        self.app.add_url_rule('/getResult', view_func=self.getResult, methods=['POST'])
        # self.app.add_url_rule('/image/query', view_func=self.queryImg, methods=['GET'])

        # init sqlite core
        self.db = sqlite3.connect('./SQLite_DB/GlobalExpress.db')
        self.cursor = self.db.cursor()
        # run flask
        self.app.run(host='0.0.0.0', port=6000, threaded=False)

    def getResult(self):  # 呼叫騎手排序API

        # decode json
        content = request.json
        GET_ADDRESS = content['GET_ADDRESS']
        SEND_ADDRESS = content['SEND_ADDRESS']
        search_type = content['SEARCH_TYPE']
        ref_rank = 1  # 排序對應索引，如果比較對像是取件次數最多騎手就設定1，第二多的就設定2，依此類推...，也就是如果設定"2"，取件次數最多的騎手就會不會被列入比較
        filter_speed = 100  # 設定排除速度，如果速度大於此閥值就排除計算
        answer = rr.ranker_max_count(GET_ADDRESS, SEND_ADDRESS, self.cursor, ref_rank, filter_speed, search_type)
        return answer

if __name__ == '__main__':
    wbs = web_server()
