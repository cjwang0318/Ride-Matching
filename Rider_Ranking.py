import mysql.connector
from mysql.connector import Error
import tool_box as tb


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


if __name__ == '__main__':
    connection, cursor=connet_DB()
    send_address="新竹市東區新莊街230號"
    arrival_address="新竹縣寶山鄉創新一路13號"
    search_query="SELECT `GET_EMPLOYEE_CODE`, `count`, `min_time_diff` FROM `RMP_Rider_Get_Min_TimeDiff` WHERE `GET_ADDRESS_2_sub` LIKE '新竹市東區新莊街230號' AND `SEND_ADDRESS_2_sub` LIKE '新竹縣寶山鄉創新一路13號' ORDER BY `count` DESC"
    # 列出查詢的資料
    records=query_DB(cursor, search_query)
    for (employee_code, count, min_time_diff) in records:
        print("employee_code: %s, count: %d, min_time_diff: %d" % (employee_code, count, min_time_diff))
    close_DB(connection, cursor)