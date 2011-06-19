#!/usr/bin/env/python
"""
Goal: from the input csv files, calculate the following variables:

    PlusCount, AvgScore, MedianFeedbackLength, EntriesRead, EntriesWritten, MedianSubjectLength, MedianTextLength

for each user in each kursXX/semesterXX pair

where

    PlusCount = count(plus) per user in a course
    AvgScore = sum(score) / count(score) per user in a course
    MedianFeedbackLength = median([feedback1,...,feedbackN])
    EntriesRead = count([Read1, ..., ReadN])
    EntriesWritten = count([Written1, ..., WrittenN])
    MedianSubjectLength = median([subj_length1, ..., subj_lengthN])
    MedianTextLength = median([text_length1, ..., text_lengthN])

The source files are:

    PlusCount = abgabe_assessment_pluses_courses.csv
    AvgScore = abgabe_assesssment_results_courses.csv
    MedianFeedbackLength = abgabe_feedback_courses.csv
    EntriesRead = forum_readlist.csv
    EntriesWritten = forum_entries_courses.csv
    MedianSubjectLength = forum_entries_courses.csv
    MedianTextLength = forum_entries_courses.csv

The headers are:

    abgabe_assessment_pluses_courses.csv

        service,course_id,kurs,semester,description,user_id,plus_date

    abgabe_assessment_results_courses.csv

        service,course_id,kurs,semester,description,user_id,result_id,result_value

    abgabe_feedback_courses.csv

        service,course_id,kurs,semester,description,user_id,task,subtask,author,comment_length

    forum_readlist.csv

        service,course_id,username,nid,id

    forum_entries_courses.csv

        service,course_id,kurs,semester,description,user,name,nid,id,parent_id,date,subject_length,text_length

"""
from common import load_file
from common import load_lines
from common import groupby
from common import is_valid_matrikel_nummer

def average(l):
    return float(sum(l)) / float(len(l))

def median(l):
    return l[len(l)/2]

def process_plus_count():
    """
    input file: "abgabe_assessment_pluses_courses.csv"

    header: service,course_id,kurs,semester,description,user_id,plus_date

    returns a dict with

        (kurs,semester) -> "username" -> plus count [integer]

    """
    def count_pluses(d):
        return dict((x, len(d[x])) for x in d\
                    if is_valid_matrikel_nummer(x))

    lines = load_lines("abgabe_assessment_pluses_courses.csv")
    KURS_COL = 2
    SEMESTER_COL = 3
    USER_COL = 5
    by_kurs_semester = groupby(lines, [KURS_COL, SEMESTER_COL])
    result = groupby(by_kurs_semester, [USER_COL], count_pluses)
    return result

def process_avg_score():
    """
    input file: "abgabe_assessment_results_courses.csv"

    header: service,course_id,kurs,semester,description,user_id,result_id,result_value

    returns a dict with

        (kurs,semester) -> "username" -> (quality score [float],
                                          avg score [float],
                                          number of scores [int])

    where quality score:

        avg score * number of scores
    """
    KURS_COL = 2
    SEMESTER_COL = 3
    USER_COL = 5
    RESULT_ID_COL = 6
    RESULT_SCORE_COL = 7
    def calculate_avg_score(grouped):
        result = {}
        for username in grouped:
            if not is_valid_matrikel_nummer(username):
                continue
            value = grouped[username]
            by_result_id = groupby(value, [RESULT_ID_COL])
            result_scores = [float(line[0][RESULT_SCORE_COL]) \
                             for line in by_result_id.values()]
            avg_score = average(result_scores)
            num_scores = len(by_result_id.keys())
            quality_score = num_scores * avg_score
            result[username] = (quality_score, avg_score, num_scores)
        return result
    lines = load_lines("abgabe_assessment_results_courses.csv")
    by_kurs_semester = groupby(lines, [KURS_COL, SEMESTER_COL])
    result = groupby(by_kurs_semester, [USER_COL], calculate_avg_score)
    return result

def process_median_feedback_length():
    """
    input file: "abgabe_feedback_courses.csv"

    header: service,course_id,kurs,semester,description,user_id,task,subtask,author,comment_length

    returns a dict with

        (kurs,semester) -> "username" -> median feedback [float]

    """
    KURS_COL = 2
    SEMESTER_COL = 3
    USER_COL = 5
    COMMENT_LENGTH_COL = 9
    def calculate_feedback(grouped):
        result = {}
        for username in grouped:
            if not is_valid_matrikel_nummer(username):
                continue
            value = grouped[username]
            feedback_lengths = [int(line[COMMENT_LENGTH_COL]) for line in value]
            median_feedback = median(feedback_lengths)
            result[username] = median_feedback
        return result
    lines = load_lines("abgabe_feedback_courses.csv")
    by_kurs_semester = groupby(lines, [KURS_COL, SEMESTER_COL])
    result = groupby(by_kurs_semester, [USER_COL], calculate_feedback)
    return result

def process_entries_read(kurs_mapping):
    """
    input:

        kurs_mapping - dict with: forum_course_id -> (kurs,semester)

    input file: "forum_readlist.py"

    header: service,course_id,username,nid,id

    returns a dict with

        (kurs,semester) -> "username" -> read count [integer]
    """
    COURSE_ID_COL = 1
    USERNAME_COL = 2
    def calculate_read(grouped):
        return dict((username, len(grouped[username])) for username in grouped\
                    if is_valid_matrikel_nummer(username))
    lines = load_lines("forum_readlist.csv")
    by_course_id = groupby(lines, [COURSE_ID_COL])
    by_user = groupby(by_course_id, [USERNAME_COL], calculate_read)
    result = {}
    for course_id in by_course_id:
        key = kurs_mapping[course_id]
        result[key] = by_user[course_id]
    return result

def process_kurs_mapping():
    """
    input file: "forum_entries_courses.csv"

    header: service,course_id,kurs,semester,description,user,name,nid,id,parent_id,date,subject_length,text_length

    returns a dict with:
        (course_id) -> (kurs,semester)
    """
    KURS_COL = 2
    SEMESTER_COL = 3
    COURSE_ID_COL = 1
    lines = load_lines("forum_entries_courses.csv")
    by_kurs_semester = groupby(lines, [KURS_COL, SEMESTER_COL])
    return dict((by_kurs_semester[x][0][COURSE_ID_COL], x)\
                for x in by_kurs_semester)

def process_forum_entries():
    """
    input file: "forum_entries_courses.csv"

    header: service,course_id,kurs,semester,description,user,name,nid,id,parent_id,date,subject_length,text_length

    returns a dict with:

        (kurs,semester) -> "username" -> (written count [int],
                                          avg subject length [float],
                                          median text length [int])
    """
    KURS_COL = 2
    SEMESTER_COL = 3
    USER_COL = 5
    SUBJ_COL = 11
    TEXT_COL = 12
    def handle_forum_entries(grouped):
        result = {}
        for username in grouped:
            if not is_valid_matrikel_nummer(username):
                continue
            value = grouped[username]
            written_count = len(value)
            subject_lengths = [int(line[SUBJ_COL]) for line in value]
            text_lengths = [int(line[TEXT_COL]) for line in value]
            result[username] = (written_count, average(subject_lengths), median(text_lengths))
        return result
    lines = load_lines("forum_entries_courses.csv")
    by_kurs_semester = groupby(lines, [KURS_COL, SEMESTER_COL])
    result = groupby(by_kurs_semester, [USER_COL], handle_forum_entries)
    return result

def process_all():
    plus_counts = process_plus_count()
    avg_scores = process_avg_score()
    median_feedback = process_median_feedback_length()
    forum_entries = process_forum_entries()
    kurs_mapping = process_kurs_mapping()
    forum_entries_read = process_entries_read(kurs_mapping)

    all_keys = [set(plus_counts.keys()),
            set(avg_scores.keys()),
            set(median_feedback.keys()),
            set(forum_entries.keys()),
            set(forum_entries_read.keys())]

    common_keys = all_keys[0]
    for keys in all_keys:
        common_keys = common_keys.intersection(keys)

    for kurs,semester in common_keys:
        key = (kurs,semester)
        user_rows = {}
        filename = "abgabe_analyse_kurs%(kurs)s_semester%(semester)s.csv" % locals()
        print "Handling", filename
        f = open(filename, "w")
        f.write("username,plus_count, avg_quality_score, median_feedback_length, entries_read, entries_written, avg_subject_length, median_text_length\n")
        # plus counts
        for username in plus_counts[key]:
            value = plus_counts[key][username]
            user_rows.setdefault(username, {})
            user_rows[username]["plus_count"] = value

        # avg scores
        for username in avg_scores[key]:
            quality_score = avg_scores[key][username][0]
            user_rows.setdefault(username, {})
            user_rows[username]["quality_score"] = quality_score

        # median feedback
        for username in median_feedback[key]:
            feedback_length = median_feedback[key][username]
            user_rows.setdefault(username, {})
            user_rows[username]["median_feedback_length"] = feedback_length

        # forum entries
        for username in forum_entries[key]:
            written, subj, text = forum_entries[key][username]
            user_rows.setdefault(username, {})
            user_rows[username]["written_entries"] = written
            user_rows[username]["avg_subj_length"] = subj
            user_rows[username]["median_text_length"] = text

        # forum entries read
        for username in forum_entries_read[key]:
            read = forum_entries_read[key][username]
            user_rows.setdefault(username, {})
            user_rows[username]["read"] = read

        for username in user_rows:
            user = user_rows[username]
            row = [username,
                   user.get("plus_count","0"),
                   user.get("quality_score","0"),
                   user.get("median_feedback_length","0"),
                   user.get("written_entries", "0"),
                   user.get("avg_subj_length", "0"),
                   user.get("median_text_length", "0")]
            row = [str(x) for x in row]
            f.write(",".join(row))
            f.write("\n")

        f.close()

if __name__ == "__main__":
    process_all()

