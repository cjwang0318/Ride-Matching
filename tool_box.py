# -*- coding: utf-8 -*-
import os
import string
import re


# File Operation
def read_file(path, skip_lines_num):
    with open(path, 'r', encoding='utf-8') as f:
        lines = [line.strip() for line in f.readlines()]
    lines = lines[skip_lines_num:]
    return lines


def write_file(path, write_data):
    with open(path, 'w', encoding='utf-8') as writer:
        writer.writelines(write_data)


def append_file(path, write_data):
    with open(path, 'a', encoding='utf-8') as writer:
        writer.writelines(write_data)


def mkdir(path):
    if not os.path.isdir(path):
        os.mkdir(path)


# English Operation
def replace_punctuation(str):
    punctuation_string = string.punctuation
    for i in punctuation_string:
        str = str.replace(i, "$" + i + "$")
    return str


def Remove_Enlglish_Character_In_File(fileName, position):
    with open(fileName, 'rb+') as filehandle:
        filehandle.seek(position, os.SEEK_END)
        filehandle.truncate()


# Chinese Operation
def remove_chinese_punctuation(line, strip_all=True):
    # 漢字的範圍為”\u4e00-\u9fa5“，這個是用Unicode表示的，所以前面必須要加”u“
    # 字元”r“的意思是表示忽略後面的轉義字元，這樣簡化了後面正則表示式裡每遇到一個轉義字元還得挨個轉義的麻煩
    if strip_all:
        rule = re.compile(r"[^a-zA-Z0-9\u4e00-\u9fa5]", re.U)
        line = rule.sub('', line)
    else:
        punctuation = """！？｡＂＃＄％＆＇（）＊＋－／：；＜＝＞＠［＼］＾＿｀｛｜｝～｟｠｢｣､、〃》「」『』【】〔〕〖〗〘〙〚〛〜〝〞〟〰〾〿–—‘'‛“”„‟…‧﹏"""
        re_punctuation = "[{}]+".format(punctuation)
        line = re.sub(re_punctuation, "", line)
    return line.strip()


def chinese_sentence_split(chinese_split_template, str):
    # chinese_split_template = '，|。|！'
    splitedStr = re.split(chinese_split_template, str)
    return splitedStr


def chinese_sentence_index(chinese_split_template, str):
    # chinese_search_template = '，。！'
    re_punctuation = "[{}]+".format(chinese_split_template)
    match_object = list(re.finditer(re_punctuation, str))
    return match_object


def check_contain_chinese(key):
    zhPattern = re.compile(u'[\u4e00-\u9fa5]+')
    match = zhPattern.search(key)
    return match


def remove_unknown_word(sentence):
    unknow_list = "、◦™‑•"
    sentence = sentence.replace("，", ",")
    sentence = sentence.translate(str.maketrans('', '', unknow_list))
    return sentence


def remove_chars_in_sentence(charList, sentence):
    # charList = "[."
    # sentence = sentence.replace("，", ",")
    sentence = sentence.translate(str.maketrans('', '', charList))
    return sentence


# List Operation
def remove_items_in_list(test_list, item):
    # remove the item for all its occurrences
    test_list = list(filter(lambda x: x != item, test_list))
    # print(test_list)
    return test_list


def check_item_in_list(str, list):
    result = "notMatch"
    for target in list:
        if target in str:
            result = target
    return result


# test area
# a="Type-(I) 2.48"
# print(replace_punctuation(a))

# str=remove_unknown_word("、CD3、")
# print(str)


# if __name__ == '__main__':
#     dataPath = "./Amazon/data/results.json"
#     Remove_Enlglish_Character_In_File(dataPath, -3)
#     append_file(dataPath, "]")
