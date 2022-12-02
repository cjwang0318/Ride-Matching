import sqlite3
import json
import os


def query_DB(cursor, query):
    cursor.execute(query)
    results = cursor.fetchall()
    return results


def is_number(s):
    try:  # 如聚能運行fLoat(s)語句,返画True(字串s5是浮點嫩)
        float(s)
        return True
    except ValueError:  # ValueError python的一座標举異常,表示您入無激的參数
        pass  # 如聚引餐了ValueError這種異常,不做任何事情(pass.不做任何事情,一般用做估位語句)
    try:
        import unicodedata  # 婴ASCii的包
        unicodedata.numeric(s)  # 把一個表示数字的字串辦换為浮點嫩返回的函嫩
        return True
    except (TypeError, ValueError):
        pass
    return False


def ranker_max_count(get_address, send_address, cursor, ref_rank, filter_speed, search_type):
    if (search_type == "count_min" or search_type == "all"):
        search_query = f"SELECT `GET_EMPLOYEE_CODE`, `time_diff`, `speed` FROM `RMP_Rider_Get_Min_TimeDiff` WHERE `GET_ADDRESS_2_sub` LIKE '{get_address}' AND `SEND_ADDRESS_2_sub` LIKE '{send_address}' ORDER BY `count` DESC"
    elif (search_type == "count_avg" or search_type == "location"):
        search_query = f"SELECT `GET_EMPLOYEE_CODE`, `time_diff`, `speed` FROM `RMP_Rider_Get_AVG_TimeDiff` WHERE `GET_ADDRESS_2_sub` LIKE '{get_address}' AND `SEND_ADDRESS_2_sub` LIKE '{send_address}' ORDER BY `count` DESC"
    elif (search_type == "speed_avg" or search_type == "time"):
        search_query = f"SELECT `GET_EMPLOYEE_CODE`, `time_diff`, `speed` FROM `RMP_Rider_Get_AVG_TimeDiff` WHERE `GET_ADDRESS_2_sub` LIKE '{get_address}' AND `SEND_ADDRESS_2_sub` LIKE '{send_address}' ORDER BY `speed` DESC"
    elif (search_type == "order_avg" or search_type == "rider"):
        search_query = f"SELECT `GET_EMPLOYEE_CODE`, `time_diff`, `speed` FROM `RMP_Rider_Get_AVG_TimeDiff` WHERE `GET_ADDRESS_2_sub` LIKE '{get_address}' AND `SEND_ADDRESS_2_sub` LIKE '{send_address}' ORDER BY `Order_Count` DESC"
    else:
        print("SQL查詢詞錯誤")
        os._exit()
    # print(search_query)
    first_time = 0
    count = 1
    records = query_DB(cursor, search_query)
    result = []
    for (employee_code, time_diff, speed) in records:
        if count < ref_rank:
            count += 1
            continue
        if is_number(speed) and float(speed) < filter_speed:
            first_time = float(time_diff)
            result = [employee_code, time_diff]
            break
    if (first_time == 0):
        print(f"錯誤：取件地址={get_address}; 配送地址={send_address}\t沒有合適推薦騎手")
        result = ["沒有合適推薦騎手", None]
    if (len(result) == 0):
        result = ["沒有合適推薦騎手", None]
    key = ["employee_code", "time"]
    result_dict = dict(zip(key, result))
    return json.dumps(result_dict, indent=2)


if __name__ == '__main__':
    # init sqlite core
    db = sqlite3.connect('./SQLite_DB/GlobalExpress.db')
    cursor = db.cursor()
    # setting
    GET_ADDRESS = "新竹市東區新莊街230號"
    SEND_ADDRESS = "新竹市東區研發二路25號"
    ref_rank = 1  # 排序對應索引，如果比較對像是取件次數最多騎手就設定1，第二多的就設定2，依此類推...，也就是如果設定"2"，取件次數最多的騎手就會不會被列入比較
    filter_speed = 100  # 設定排除速度，如果速度大於此閥值就排除計算
    search_type = "rider"  # 排序使用的模型： count_min or count_avg or speed_avg, count表示使用次數排序, speed表示使用速度排序, avg表示使用平均值當配送時間, min表示使用最小值當配送時間
    ##############
    result = ranker_max_count(GET_ADDRESS, SEND_ADDRESS, cursor, ref_rank, filter_speed, search_type)
    print(result)
