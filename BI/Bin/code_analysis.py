#!/usr/bin/env python
"""
Goal: from the input csv files, calculate the following variables:

    SuccessRate, ReadByUser, WrittenByUser, AvgSubjectLength, AvgTextLength

where

    SuccessRate = count(SUCCESS) / total_unittest_runs
    ReadByUser  = count(E_user_read) group by user
    WrittenByUser = count(E_written)
    AvgSubLength = sum(SubjectLength) / WrittenByUser
    AvgTextLength = sum(TextLength) / WrittenByUser

The source files are:

    SuccessRate = code_unittest_results.csv
    ReadByUser  = forum_readlist.csv
    WrittenByUser = forum_99_entries.csv
    AvgSubLength = forum_99_entries.csv
    AvgTextLength = forum_99_entries.csv

"""
import os
import sys
from common import load_file

def _lines_by_user(data, user_col):
    """
    input:

        data - raw data file (as CSV)
        user_col - colum where the user is located

    output:

        dict with "username" -> [line1, ..., lineN]

    """
    lines = data.split("\n")[1:-1]
    splitted = [line.split(",") for line in lines]
    usernames = [line[user_col] for line in splitted]
    usernames = set(usernames)

    # "username" -> [line1, ..., lineN]
    line_by_user = {}
    for line in splitted:
        username = line[user_col]
        line_by_user.setdefault(username, [])
        line_by_user[username].append(line)

    return line_by_user

def _counts_by_user(line_by_user):
    """
    input:

        dict with "username" -> [line1, ..., lineN]

    output:

        dict with "username" -> count [Integer]
    """
    # "username" -> count of entries
    counts = {}
    for username in line_by_user:
        forum_entries = line_by_user[username]
        counts[username] = len(forum_entries)
    return counts

def _percent_count_by_user(line_by_user,total=None):
    """
    input:

        dict with "username" -> [line1, ..., lineN]

    output:

        dict with "username" -> count [Integer]
    """
    counts = _counts_by_user(line_by_user)
    result = dict((username, percent_count(counts, username, total)) \
                for username in counts)
    return result

def percent_count(counts, username, total=None):
    """
    input:

        dict with "username" -> count [integer]

    output:

        count as percentage of total counts [float]
    """
    if not total:
        total = sum(counts.values())
    return float(counts[username]) / float(total)

def process_read_by_user():
    """
    header is

        service,course_id,username,nid,id

    returns a dict with

        "username" -> forum entries count [float]

    """
    data = load_file("forum_readlist.csv")
    entries_data = load_file("forum_99_entries.csv")
    line_by_user = _lines_by_user(data, 2)

    course_id = "99"
    #exclude those that are NOT course_id 99 (AlgoDat)
    new_line_by_user = {}
    for username in line_by_user:
        lines = line_by_user[username]
        lines = [line for line in lines if line[1] == course_id]
        new_line_by_user[username] = lines

    total_entries = len(entries_data.split("\n"))

    counts = _percent_count_by_user(new_line_by_user, total_entries)
    #counts = _counts_by_user(new_line_by_user)

    return counts

def calculate_avg_length(lengths):
    """
    can be used for [subject,text]

    input:

        [length1, ..., lengthN]

    return

        Avg - Integer
    """
    return sum(lengths) / len(lengths)

def collect_col_as_int(data, key, col):
    """
    input:

        dict with "key" -> [line1, ..., lineN]

    returns:

        [int1, ..., intN]
    """
    return [int(line[col]) for line in data[key]]

def process_written_by_user():
    """
    header is

        service,course_id,user,nid,id,subject_length,text_length

    returns a dict with

        "username" -> (WrittenByUser, AvgSubjectLength, AvgTextLength)
    """
    USER_COL = 2
    SUBJ_COL = 5
    TEXT_COL = 6

    data = load_file("forum_99_entries.csv")

    line_by_user = _lines_by_user(data, USER_COL)
    counts = _percent_count_by_user(line_by_user)

    # dict with "username" -> (WrittenByUser, AvgSubjectLength, AvgTextLength)
    result = {}
    for username in line_by_user:
        subj_lengths = collect_col_as_int(line_by_user, username, SUBJ_COL)
        text_lengths = collect_col_as_int(line_by_user, username, TEXT_COL)
        avg_sub_length = calculate_avg_length(subj_lengths)
        avg_text_length = calculate_avg_length(text_lengths)
        result[username] = (counts[username], avg_sub_length, avg_text_length)

    return result

def process_success_rate_by_user():
    """
    SuccessRate = count(SUCCESS) / (count(FAILURE) + count(ERROR))

    header is

        service,course_id,group_id,person_id,topic_id,date,success,warning,failure,error,info,timeout

    returns a dict with

        "username" -> success rate [float]
    """
    data = load_file("code_unittest_results.csv")
    USER_COL = 3
    SUCCESS_COL = 12
    FAILURE_COL = 14
    ERROR_COL   = 15
    line_by_user = _lines_by_user(data, USER_COL)
    return calculate_success_rates(line_by_user, SUCCESS_COL, [FAILURE_COL, ERROR_COL])


def calculate_success_rates(lines, success_col, failure_cols):
    """
    input:

        lines - dict with "username" -> [line1, ..., lineN]
        success_col - int
        failure_cols - [int, ..., int]

    output:

        dict with "username" -> float
    """
    result = {}
    for username in lines:
        total_unittest_runs = len(lines[username])
        success = sum(int(line[success_col]) for line in lines[username] if line[success_col] != "0")
        failures = sum(int(line[col]) \
                       for col in failure_cols \
                       for line in lines[username])
        if failures == 0:
            if username.startswith("a"):
                print "NO FAILURES:", username
        #print username, success, total_unittest_runs
        result[username] = float(success) / float(total_unittest_runs)
        #print "success rate:", username, result[username]
    return result

def do_process(filename):
    f = open(filename, "w")
    f.write("username,success_rate,read_rate,written_rate,avg_subject_length,avg_text_length")
    f.write("\n")

    read_percentage = process_read_by_user()
    written_data = process_written_by_user()
    success_rates = process_success_rate_by_user()

    for username in success_rates.keys():
        success_rate = success_rates[username]
        try:
            read_percent = read_percentage[username]
        except KeyError:
            read_percent = 0.0
        try:
            written_percent,avg_sub_len,avg_text_len = written_data[username]
        except KeyError:
            written_percent = 0.0
            avg_sub_len = 0
            avg_text_len = 0
        if not username.startswith("a"):
            continue
        f.write(",".join([username, str(success_rate), str(read_percent), str(written_percent), str(avg_sub_len), str(avg_text_len)]))
        f.write("\n")

    f.close()


if __name__ == "__main__":
    do_process("code_analysis_by_user.csv")
