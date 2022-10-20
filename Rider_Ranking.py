import mysql.connector
from mysql.connector import Error
import pandas as pd


def connet_DB():
    try:
        # 連接 MySQL/MariaDB 資料庫
        connection = mysql.connector.connect(
            host='192.168.0.1',  # 主機名稱
            database='GlobalExpress',  # 資料庫名稱
            user='admin',  # 帳號
            password='520laura')  # 密碼

        if connection.is_connected():
            # 顯示資料庫版本
            db_Info = connection.get_server_info()
            print("資料庫版本：", db_Info)

            # 顯示目前使用的資料庫
            cursor = connection.cursor()
            cursor.execute("SELECT DATABASE();")
            record = cursor.fetchone()
            print("目前使用的資料庫：", record)
    except Error as e:
        print("資料庫連接失敗：", e)
    return connection, cursor


def close_DB(connection, cursor):
    cursor.close()
    connection.close()
    print("資料庫連線已關閉")


def query_DB(cursor, query):
    cursor.execute(query)
    # 取回全部的資料
    records = cursor.fetchall()
    print("成功查詢，資料筆數：", cursor.rowcount)
    return records


def get_tesing_get_send_address(cursor, num_of_testing_instance):
    search_query = "SELECT `GET_ADDRESS_2_sub`, `SEND_ADDRESS_2_sub` FROM `RMP_Get_Send_Count` LIMIT " + str(
        num_of_testing_instance)
    records = query_DB(cursor, search_query)
    return records


def ranker_1(row, cursor, ref_rank, compare_rank):
    get_address = row["get_address"]
    send_address = row["send_address"]
    search_query = f"SELECT `min_time_diff` FROM `RMP_Rider_Get_Min_TimeDiff` WHERE `GET_ADDRESS_2_sub` LIKE '{get_address}' AND `SEND_ADDRESS_2_sub` LIKE '{send_address}' ORDER BY `count` DESC"
    print(search_query)
    records = query_DB(cursor, search_query)
    time_diff=records[compare_rank][0]-records[ref_rank][0]
    print(f"時間差={time_diff}")
    return time_diff


if __name__ == '__main__':
    # Setting
    connection, cursor = connet_DB()
    num_of_testing_instance = 1
    ref_rank=0
    compare_rank=1
    ##############
    testing_address = []
    # query top 100 get and send address for testing
    records = get_tesing_get_send_address(cursor, num_of_testing_instance)
    for (get_address, send_address) in records:
        # print("get_address: %s, send_address: %s" % (get_address, send_address))
        testing_address.append([get_address, send_address])
    df_testing_address = pd.DataFrame(testing_address)
    df_testing_address.columns = ["get_address", "send_address"]
    # print(df_testing_address)
    ds=df_testing_address.apply(ranker_1, axis=1, args=(cursor, ref_rank, compare_rank)) # https://ithelp.ithome.com.tw/articles/10268716, https://www.digitalocean.com/community/tutorials/pandas-dataframe-apply-examples
    print(ds)
    # search_query="SELECT `GET_EMPLOYEE_CODE`, `count`, `min_time_diff` FROM `RMP_Rider_Get_Min_TimeDiff` WHERE `GET_ADDRESS_2_sub` LIKE '新竹市東區新莊街230號' AND `SEND_ADDRESS_2_sub` LIKE '新竹縣寶山鄉創新一路13號' ORDER BY `count` DESC"
    # # 列出查詢的資料
    # records=query_DB(cursor, search_query)
    # for (employee_code, count, min_time_diff) in records:
    #     print("employee_code: %s, count: %d, min_time_diff: %d" % (employee_code, count, min_time_diff))
    close_DB(connection, cursor)
