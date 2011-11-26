"""

input:

    DD.MM.YYYY;<Label>

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
    """ DD.MM.YYYY -> YYYY-MM-DDT00:00:00 """
    splitted = timestamp.split(".")
    return "%(year)s-%(month)s-%(day)sT00:00:00" %  dict(
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
    return [x.split(";") for x in load_file(filename).split("\n")[1:]
            if x != '']

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
        log += lines
    return log

def main(root):
    semesters = ["se01", "se03", "se04"]
    for semester in semesters:
        virtualevent_path = os.path.join(
            "/home/conrad/Downloads/data/dbs",
            semester + ".conf"
        )
        virtual_events = read_virtual_events(virtualevent_path)
        path = os.path.join(root, "hep", "KURS02", semester)
        for filename in os.listdir(path):
            #content = load_file(os.path.join(path, filename))
            #if ",Enrollment," in content:
                #os.remove(os.path.join(path, filename))
            result = add_virtual_events(os.path.join(path, filename), virtual_events)
            result.sort()
            header = "timestamp;instance_id;person_id;person_type;event;data\n"
            get_path = lambda ns: os.path.join(root, ns, "KURS02", semester)
            ## sort the file and write out to another directory
            os.system("mkdir -p %s" % get_path("sorted"))
            sorted_path = os.path.join(get_path("sorted"), filename)
            f = open(sorted_path, "w")
            f.write(header + "\n".join(";".join(x) for x in result))
            f.close()

if __name__ == '__main__':
    main(".")

