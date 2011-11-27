"""

input:

    DD.MM.YYYY;<Label> OR YYYY-MM-DD;<Label> OR YYYY-MM-DDTHH:MM:DD

    Example:

        15.10.2008;Vorbesprechung
        17.12.2008;Tutorium
        17.12.2008;Meilenstein 4
        14.01.2009;Meilenstein 5
        21.01.2009;Projektendabnahme

required output:

    for each instance id in a given csv file of the structure:

        timestamp;instance_id;person_id;person_type;event;data

    I have to generate:

        YYYY-MM-DDT00:00:00;<instance_id>;system;system;<Label>;''
"""
import os
from common import load_file
from collections import namedtuple

DEFAULT_DATETIME = "1970-01-01T00:00:00"
DATA_ROOT = "/Users/indygemma/"

# Steps:
# 1) for a given CSV extract all instance_id elements
# 2) for each instance_id create a virtual deadline event:
#   a) convert to correct datetime
#   b) inject the <instance_id>
#   c) write out system for person_id
#   d) write out system for person_type
#   e) write out the <Label>
#   f) write out empty data: ''
# 3) sort the final string and write out to the same file

def convert_datetime(timestamp):
    """
        DD.MM.YYYY          -> YYYY-MM-DDT23:59:59
    OR  YYYY-MM-DD          -> YYYY-MM-DDT23:59:59
    OR  YYYY-MM-DDTHH:MM:DD -> YYYY-MM-DDTHH:MM:DD
    """
    if "T" in timestamp:
        return timestamp
    elif "-" in timestamp:
        return timestamp + "T23:59:59"
    splitted = timestamp.split(".")
    return "%(year)s-%(month)s-%(day)sT23:59:59" %  dict(
        year=splitted[2],
        month=splitted[1],
        day=splitted[0]
    )

def add_line(instance_id, timestamp, label):
    return [
        "'"+convert_datetime(timestamp)+"'",
        instance_id,
        "'system'",
        "'system'",
        "'"+label+"'",
        "''"
    ]

def add_lines(instance_id, virtual_events):
    return [add_line(instance_id, timestamp, label)
            for timestamp, label in virtual_events]

def read_virtual_events(filename):
    """ output: [datetime, label] """
    return [x.split(";") for x in load_file(filename).split("\n")
            if x != '']

def read_original_log(filename):
    lines = [x.split(";") for x in load_file(filename).split("\n")[1:]
             if x != '']
    result = []
    for line in lines:
        if line[0] == "''":
            line[0] = DEFAULT_DATETIME
        result.append(line)
    return result


def extract_instance_ids(log):
    """ output: [instance_id1, ...] """
    try:
        return set([x[1] for x in log if x != ''])
    except Exception as e:
        import ipdb;ipdb.set_trace()

def add_virtual_events(filename, virtual_events):
    log    = read_original_log(filename)
    for instance_id in extract_instance_ids(log):
        lines = add_lines(instance_id, virtual_events)
        if "exercises" in filename:
            # filter out every event that has "milestone" in it
            lines = [line for line in lines if not "milestone" in line[4].lower()]
        elif "milestones" in filename:
            # filter out every event that has "exercise" in it
            lines = [line for line in lines if not "exercise" in line[4].lower()]
        log += lines
    return log

def add_deadlines(root, kurs, semester, virtualevent_path):
    virtual_events = read_virtual_events(virtualevent_path)
    path = os.path.join(root, "hep", kurs, semester)
    for filename in os.listdir(path):
        result = add_virtual_events(os.path.join(path, filename), virtual_events)
        result.sort()
        header = "timestamp;instance_id;person_id;person_type;event;data\n"
        get_path = lambda ns: os.path.join(root, ns, kurs, semester)
        ## sort the file and write out to another directory
        os.system("mkdir -p %s" % get_path("sorted"))
        sorted_path = os.path.join(get_path("sorted"), filename)
        f = open(sorted_path, "w")
        f.write(header + "\n".join(";".join(x) for x in result))
        f.close()

def add_dbs_deadlines(root):
    semesters = ["se01", "se03", "se04"]
    kurs = "KURS02"
    for semester in semesters:
        virtualevent_path = os.path.join(
            DATA_ROOT + "Downloads/data/data_sup/" + kurs,
            semester + ".conf"
        )
        add_deadlines(root, kurs, semester, virtualevent_path)

def add_algodat_deadlines(root):
    semesters = ["se00"]
    kurs = "KURS00"
    for semester in semesters:
        virtualevent_path = os.path.join(
            DATA_ROOT + "Downloads/data/data_sup/" + kurs,
            semester,
            "dates.csv"
        )
        add_deadlines(root, kurs, semester, virtualevent_path)

#### scores

EVENT_MAPPING = {
    "NOTE"          : "Final mark",
    "EXAM"          : "Exam evaluation",
    "SCORE1"        : "Phase evaluation 1",
    "SCORE2"        : "Phase evaluation 2",
    "SCORE3"        : "Phase evaluation 3",
    "SCORE_PROJECT" : "Project evaluation"
}

SCORE_ATTRS = ["note", "exam", "score1", "score2", "score_project", "score3"]
Score = namedtuple("Score", "matrikelnr name " + " ".join(SCORE_ATTRS))

import operator
open_files  = lambda fs: reduce(operator.add, (file(f).read() for f in fs))
read_scores = lambda fs: [parse_score(line.split(";")) for line in open_files(fs).splitlines()]

def parse_score(row):
    if len(row) < 8:
        row.append("0")
    return Score(*row)

# input:
#
#   1) a0127847;Margor Bunda;<Note>;<Test score>;<1st submission score>;<2nd submission score>;<Project score>;<Optimization score>
#      ...
#   2) scoring dates: "EVENT" -> "DATE" with keys: NOTE, EXAM, SCORE1, SCORE2, SCORE3, SCORE_PROJECT
#
# output:
#
#   matrikelnr -> ['<DATE>';'a0127847';'system';'system';'Exam evaluation';'{"score":10}', ...]
#
def score_events(dates, scores):
    build_row = lambda x, a: [convert_datetime(dates[a.upper()]), x.matrikelnr, 'system', 'system',
                              EVENT_MAPPING[a.upper()], '{"score":"'+getattr(x,a)+'"}']
    format_row = lambda x, a: ["'%s'" % x for x in build_row(x,a)]
    result = {}
    for score in scores:
        result.setdefault(score.matrikelnr, [])
        per_student = []
        for attr in SCORE_ATTRS:
            per_student.append(format_row(score, attr))
        result[score.matrikelnr] = per_student
    return result

# keys:
# NOTE, EXAM, SCORE1, SCORE2, SCORE3, SCORE_PROJECT
read_algodat_scoring_dates = lambda filepath: dict((v,k) for k,v in
                                                   dict([tuple(line.split(";")) for line in file(filepath).read().splitlines()])
                                                       .iteritems())
import re
is_valid_score_file  = lambda f: re.compile("\d+.csv").match(f) != None
find_score_filenames = lambda path: filter(is_valid_score_file, os.listdir(path))

def add_algodat_scores(root):
    kurs = "KURS00"
    semesters = ["se00"]
    for semester in semesters:
        deadline_virtualevent_path = os.path.join(
            DATA_ROOT + "Downloads/data/data_sup/" + kurs,
            semester,
            "dates.csv"
        )
        scoring_virtualevent_path = os.path.join(
            DATA_ROOT + "Downloads/data/data_sup/" + kurs,
            semester,
            "scoring_dates.csv"
        )
        base_dir = os.path.dirname(scoring_virtualevent_path)
        scoring_dates = read_algodat_scoring_dates(scoring_virtualevent_path)
        score_filenames = find_score_filenames(base_dir)
        scores = read_scores(os.path.join(base_dir, filename) for filename in score_filenames)
        events = score_events(scoring_dates, scores)
        ## writing logic
        path = os.path.join(root, "hep", kurs, semester)
        for filename in os.listdir(path):
            virtual_events = read_virtual_events(deadline_virtualevent_path)
            result = read_original_log(os.path.join(path, filename))
            for instance_id in extract_instance_ids(result):
                try:
                    score_lines = events[instance_id[1:-1]]
                    result += score_lines
                except:
                    print "No scoring data for", instance_id, "in", os.path.join(path, filename)
                deadline_lines = add_lines(instance_id, virtual_events)
                result += deadline_lines
            result.sort()
            header = "timestamp;instance_id;person_id;person_type;event;data\n"
            get_path = lambda ns: os.path.join(root, ns, kurs, semester)
            ## sort the file and write out to another directory
            os.system("mkdir -p %s" % get_path("sorted"))
            sorted_path = os.path.join(get_path("sorted"), filename)
            f = open(sorted_path, "w")
            f.write(header + "\n".join(";".join(x) for x in result))
            f.close()

if __name__ == '__main__':
    add_dbs_deadlines(".")
    add_algodat_deadlines(".")
    add_algodat_scores(".")

