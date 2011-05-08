#!/usr/bin/env python

ROOT_DATA = "/home/conrad/Downloads/data/"

# ---- DO NOT CHANGE BELOW THIS LINE ---- #
import os
import re
from lxml import etree

legit_path = re.compile("(?P<course_id>\d+)/User/(?P<username>[a-zA-Z0-9]+)/\d+")

# ROOT/Forum/{{ course_id }}/User/{{ username }}/{{ readlist_file }}

def list_readlist_files(root):
    result = []
    for root, dirs, file in os.walk(os.path.join(root, "Forum")):
        for f in file:
            fullpath = os.path.join(root, f)
            research = re.search(legit_path, fullpath)
            if research != None:
                course_id = research.groupdict()["course_id"]
                username  = research.groupdict()["username"]
                result.append(dict(course_id=course_id,
                                   username=username,
                                   filename=fullpath))
    return result

def extract_file_contents(root, files):
    """
    files is of the form

        [{'course_id':23, 'username':"a2828", 'filename':"..."}, ...]

    result is the the files dict with the updated info:

        ...
        'readlist':[34,12,34,54,],
        ...
    """
    for entry in files:
        print "working on", entry["filename"]
        f = open(entry["filename"], "r")
        content = f.read()
        f.close()
        readlist = [int(x) for x in content.split("\n")[:-1]]
        entry["readlist"] = readlist
        # NOTE: open the associated /Forum/{{ course_id }}/Data/{{ file }}.xml
        # file in order to extract the <entries nid="{{ nid }}"> attribute
        # only with {{ nid }} is the {{ id }} unique.
        # Example: username: a0848375
        #          course_id: 119
        #          file: 3.xml
        #
        # and
        #          username: a0848375
        #          course_id: 119
        #          file: 1.xml
        #
        # there are duplicate ids: [80, 81, 82, 83] which can only be uniquely
        # determined via the associated {{ nid }}
        filename = os.path.basename(entry["filename"])
        xml_path = os.path.join(root,"Forum", str(entry["course_id"]), "Data", "%s.xml" % filename)
        f = open(xml_path, "r")
        content = f.read()
        f.close()
        print "associated xml:", xml_path
        tree = etree.fromstring(content)
        nid = tree.xpath("//entries")[0].get("nid")
        entry["nid"] = int(nid)
    return files

def to_csv(files, filename):
    """
        [d,...]

    where d keys are: ['username', 'course_id', 'nid', 'readlist']

    """
    f = open(filename, "w")
    f.write("service,course_id,username,nid,id\n")
    for entry in files:
        for entry_id in entry["readlist"]:
            values = [
                "Forum",
                str(entry["course_id"]),
                entry["username"],
                str(entry["nid"]),
                str(entry_id)
            ]
            f.write(",".join(values))
            f.write("\n")
    f.close()

if __name__ == '__main__':
    files = list_readlist_files(ROOT_DATA)
    to_csv( extract_file_contents ( ROOT_DATA, files ), "forum_readlist.csv" )

