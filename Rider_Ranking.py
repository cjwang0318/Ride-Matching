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
    # print("成功查詢，資料筆數：", cursor.rowcount)
    return records


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


def get_tesing_get_send_address(cursor, num_of_testing_instance):
    search_query = "SELECT `GET_ADDRESS_2_sub`, `SEND_ADDRESS_2_sub` FROM `RMP_Get_Send_Count` LIMIT " + str(
        num_of_testing_instance)
    records = query_DB(cursor, search_query)
    return records


def ranker_max_count(row, cursor, ref_rank, filter_speed):
    get_address = row["get_address"]
    send_address = row["send_address"]
    search_query = f"SELECT `min_time_diff`, `speed` FROM `RMP_Rider_Get_Min_TimeDiff` WHERE `GET_ADDRESS_2_sub` LIKE '{get_address}' AND `SEND_ADDRESS_2_sub` LIKE '{send_address}' ORDER BY `count` DESC"
    # print(search_query)
    first_time = 0
    count = 0
    records = query_DB(cursor, search_query)
    for (min_time_diff, speed) in records:
        if count < ref_rank:
            count = count + 1
            continue
        if is_number(speed) and float(speed) < filter_speed:
            first_time = min_time_diff
            break
    if (first_time == 0):
        print(f"錯誤：取件地址={get_address}; 配送地址={send_address}\t不符合測試條件")
        return pd.Series([None, None])
    # print(first_time)
    search_query = f"SELECT `time_diff` FROM `RMP_Rider_Get_Send_Time_After_2022` WHERE `GET_ADDRESS_2_sub` LIKE '{get_address}' AND `SEND_ADDRESS_2_sub` LIKE '{send_address}'"
    records = query_DB(cursor, search_query)
    num_of_testing_data = len(records)
    time_sum_of_ranker = first_time * num_of_testing_data
    time_sum_of_testing_data = 0
    for (time_diff) in records:
        time_sum_of_testing_data = time_sum_of_testing_data + time_diff[0]
    if len(records) == 0:
        print(f"錯誤：取件地址={get_address}; 配送地址={send_address}\t不符合測試條件")
        return pd.Series([None, None])
    improved_time = time_sum_of_testing_data - time_sum_of_ranker
    improved_ratio = improved_time / time_sum_of_testing_data
    print(
        f"取件地址={get_address}; 配送地址={send_address}; 測試資料數量={num_of_testing_data}; 提升率={format(improved_ratio, '.4f')}")
    # return improved_ratio
    return pd.Series([improved_ratio, num_of_testing_data])


if __name__ == '__main__':
    # Setting
    connection, cursor = connet_DB()
    num_of_testing_instance = 10
    ref_rank = 1  # 排序對應索引，如果比較對像是取件次數最多騎手就設定1，第二多的就設定2，依此類推...，也就是如果設定"2"，取件次數最多的騎手就會不會被列入比較
    filter_speed = 100  # 設定排除速度，如果速度大於此閥值就排除計算
    ##############
    testing_address = []
    # query top 100 get and send address for testing
    records = get_tesing_get_send_address(cursor, num_of_testing_instance)
    for (get_address, send_address) in records:
        # print("get_address: %s, send_address: %s" % (get_address, send_address))
        testing_address.append([get_address, send_address])
    df_testing_address = pd.DataFrame(testing_address)
    df_testing_address.columns = ["get_address", "send_address"]
    result_df = df_testing_address.apply(ranker_max_count, axis=1, args=(cursor,
                                                                         ref_rank,
                                                                         filter_speed))  # https://ithelp.ithome.com.tw/articles/10268716, https://www.digitalocean.com/community/tutorials/pandas-dataframe-apply-examples
    result_df.columns = ["improved_ratio", "num_of_testing_data"]
    average_improved_ratio = format(result_df["improved_ratio"].mean(), '.4f')
    sum_of_testing_data = format(result_df["num_of_testing_data"].sum(), '.0f')
    print(f"測試資料數量={sum_of_testing_data}; 平均提升率={average_improved_ratio}")
    # print(result_df)
    result_df.to_csv("output.csv")
    close_DB(connection, cursor)
