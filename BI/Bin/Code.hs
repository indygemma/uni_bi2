module Main where

import BI.Api
import BI.Types
import Data.List
import qualified Data.Map as Map
import System.FilePath
import System.Posix.Files
import System.Time

{- | Update Functions -}
upPathIndex name idx (Object service path tag theText ttype attrMap attrTMap children) =
    -- do something with path and save that result in the dict
    (Object service path tag theText ttype newMap attrTMap children)
    where newMap  = Map.insert name result attrMap
          newPath = splitDirectories $ path \\ "/home/conrad/Downloads/data/"
          result  = newPath !! idx

-- "Code/{ course_id }/Data/{ group_id }/..."
upCourseId name obj = upPathIndex name 1 obj
upGroupId  name obj = upPathIndex name 3 obj

upUnittestStates (Object service path tag theText ttype attrMap attrTMap children) =
    Object service path tag theText ttype newMap attrTMap children
    where newMap          = Map.union (Map.union attrMap stateMap) defaultStateMap
          defaultStateMap = Map.fromList [("ERROR",      "0"),
                                          ("FAILURE",    "0"),
                                          ("INFO",       "0"),
                                          ("PROCESSING", "0"),
                                          ("SUCCESS",    "0"),
                                          ("TIMEOUT",    "0"),
                                          ("WARNING",    "0")]
          stateMap = Map.fromList $ map (\l@(x:xs) -> (x, show $ length l))
                         $ group
                         $ sort $ concat
                         $ extractAttrs ["state"]
                         $ select and [hasTag "case"] children

-- | Select statements | --

selectPersons objects = unique
        $ extract [exService,
                   exAttr "course_id",
                   -- IMPORTANT: we have to set "1" as group because we want to join with lecturerer
                   -- list later, which only defines groups with 1. There are no other groups in lecturers.
                   exAttrDefault "group" "1",
                   exAttr "id",
                   exAttr "name",
                   exAttr "email"]
        $ update [upCourseId "course_id"]
        $ select and [hasTag "person", notInPath "topics.xml"] objects

selectLecturers objects = unique
        $ extract [exService,
                   exAttr "course_id",
                   exAttr "group_id",
                   exAttr "id",
                   exTag]
        $ select or [hasTag "lecturer", hasTag "tutor"]
        $ update [pushDown "id" "group_id",
                  pushDown "course_id" "course_id",
                  upCourseId "course_id"]
        $ select and [hasTag "group", inPath "course.xml"] objects

selectTopics objects = extract [exService, exAttr "course_id", exAttr "id", exText]
        $ update [upCourseId "course_id"]
        $ select and [hasTag "topic"] objects

selectTopicAssignments objects = unique
        $ extract [exService,
                   exAttr "course_id",
                   exAttr "group_id",
                   exAttr "id",
                   exText]
        $ select and [hasTag "person"]
        $ update [pushDown "id" "group_id",
                  pushDown "course_id" "course_id",
                  upCourseId "course_id"]
        $ select and [hasTag "group", inPath "topics.xml"] objects

selectUnittestResults objects = extract [exService,
                   exAttr "course_id",
                   exAttr "group_id",
                   exAttr "identifier",
                   exAttr "theme",
                   exAttr "timestamp",
                   exAttr "year",
                   exAttr "month",
                   exAttr "day",
                   exAttr "hour",
                   exAttr "min",
                   exAttr "sec",
                   exAttr "SUCCESS",
                   exAttr "WARNING",
                   exAttr "FAILURE",
                   exAttr "ERROR",
                   exAttr "INFO",
                   exAttr "TIMEOUT"]
        $ update [upCourseId "course_id",
                  upGroupId  "group_id",
                  upUnittestStates]
        $ select and [hasTag "test", inPath "resUnit.xml"] objects

-- | Merge Statements | --

-- initial lists [service,course_id,group_id,person_id,name,email]
--               [service,course_id,group_id,person_id,type]
-- keys are: [service,course_id,group_id,person_id]
-- merge on [service,course_id,group_id,person_id] -> [name,email,type]
-- final structure: [service,course_id,group_id,person_id,name,email,type]
-- 6th column's default value is student
mergePersonsLecturers objects = leftJoin [0..3] (selectPersons objects) (selectLecturers objects)
    [(6,\key -> if isPrefixOf "a" (key!!3) then "student" else "")]

-- IMPORTANT: the topic assignments taken from topics.xml <peron> tag is not consistent with the values
--            taken from resUnit.xml results <test theme="{ topic_id }">. That's why we don't join from
--            the topics.xml but from the values taken from resUnit.xml
-- merge in the topic id a given person has taken
-- initial lists [service,course_id,group_id,person_id,name,email,type]
--               [service,course_id,group_id,person_id,topic_id]
-- keys are: [service,course_id,group_id,person_id]
-- final structure [service,course_id,group_id,person_id,name,email,type,topic_id]
mergeTopicIds objects = leftJoin [0..3] (mergePersonsLecturers objects) (selectTopicAssignments objects) []

-- merge in the unittest results
-- initial lists [service,course_id,group_id,person_id,name,email,type]
--               [service,course_id,group_id,person_id,topic_id,timestamp,year,month,day,hour,min,sec,SUCCESS,WARNING,FAILURE,ERROR,INFO,TIMEOUT]
-- keys are: [service,course_id,group_id,person_id,topic_id]
-- final structure: [service,course_id,group_id,person_id,name,email,type,
--                   topic_id,timestamp,year,month,day,hour,min,sec,
--                   SUCCESS,WARNING,FAILURE,ERROR,INFO,TIMEOUT]
mergeUnittestResults objects = leftJoin [0..3] (mergePersonsLecturers objects) (selectUnittestResults objects) []

-- merge in the topic description
-- initial lists [service,course_id,group_id,person_id,name,email,topic_id,date,
--                SUCCESS,WARNING,FAILURE,ERROR,INFO,TIMEOUT]
--               [service,course_id,group_id,topic_id,text]
-- keys are: [service,course_id,topic_id]
-- final structure [service,course_id,topic_id,group_id,person_id,name,email,type,topic_description]
{-mergeTopicDescription objects = leftJoin [0,1,7] (mergeUnittestResults objects) (selectTopics objects) []-}

-- IMPORTANT: there are two person ids that do not uccur in persons.xml: a710000, a712675, but do occur in
--            resUnit.xml. Note that these are not valid matrikel numbers anyway. Their groups are also 0.
--            They only ones with that group.
--            The other way: those that occur in persons.xml but not in resUnit.xml means those people not
--            running unittests: lecturer, tutors and dropped out students
comparePersonIds objects filename = do
    -- compares which ids occur in unittests results but are omitted in persons.xml
    let person_objects = unique $ extract [exAttr "id"] $ select and [hasTag "person", notInPath "topics.xml"] objects
    let unittest_persons = unique $ extract [exAttr "identifier"] $ select and [hasTag "test", inPath "resUnit.xml"] objects
    writeFile filename $ show (person_objects \\ unittest_persons)

-- | CSV writing Functions | --

-- | returns the name of all states available from the unittest runs.
allStates objects filename = do
    writeFile filename
        $ unlines
        $ unique
        $ concat
        $ extractAttrs ["state"]
        $ select and [hasTag "case"] objects

allInstances objects filename = do
    writeFile filename
        $ to_csv "service,course_id,description\n"
        $ unique
        $ extract [exService, exAttr "id", exText]
        $ select and [hasTag "instance"] objects

allTopics objects filename = do
    writeFile filename
        $ to_csv "service,course_id,topic_id,topic\n"
        $ selectTopics objects

allPersons objects filename = do
    writeFile filename
        $ to_csv "service,course_id,group_id,person_id,name,email\n"
        $ selectPersons objects

allLecturers objects filename = do
    writeFile filename
        $ to_csv "service,course_id,group_id,person_id,type\n"
        $ selectLecturers objects

allTopicPersonAssignments objects filename = do
    writeFile filename
        $ to_csv "service,course_id,group_id,person_id,topic_id\n"
        $ selectTopicAssignments objects

allUnittestResults objects filename = do
    writeFile filename
        $ to_csv "service,course_id,group_id,person_id,topic_id,date,success,warning,failure,error,info,timeout\n"
        $ selectUnittestResults objects

allPersonLecturers objects filename = do
    writeFile filename
        $ to_csv "service,course_id,group_id,person_id,name,email,type\n"
        $ mergePersonsLecturers objects

allPersonTopics objects filename = do
    writeFile filename
        $ to_csv "service,course_id,group_id,person_id,name,email,type,topic_id\n"
        $ mergeTopicIds objects

allFactTable objects filename = do
    writeFile filename
        $ to_csv "service,course_id,group_id,person_id,name,email,type,topic_id,timestamp,year,month,day,hour,min,sec,SUCCESS,WARNING,FAILURE,ERROR,INFO,TIMEOUT\n"
        $ mergeUnittestResults objects

main = do
    objects <- selectFS and [inService "Code"]
    {-allStates                 objects "code_states.csv"-}
    {-allInstances              objects "code_courses.csv"-}
    {-allTopics                 objects "code_topics.csv"-}
    {-allPersons                objects "code_persons.csv"-}
    {-allLecturers              objects "code_lecturers.csv"-}
    {-allTopicPersonAssignments objects "code_topic_person_assignments.csv"-}
    {-allUnittestResults        objects "code_unittest_results.csv"-}
    {-allPersonLecturers objects "code_person_lecturers.csv"-}
    {-allPersonTopics objects "code_person_lecturers_topicids.csv"-}
    allFactTable objects "code_fact_table.csv"
    {-comparePersonIds objects "code_person_in_unittests_not_in_persons_xml.csv"-}
