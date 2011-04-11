module Main where

import BI.Api
import BI.Types
import Data.List
import qualified Data.Map as Map
import System.FilePath
import System.Posix.Files
import System.Time

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
        $ extract [exService, exAttr "course_id", exAttr "id", exText]
        $ update [upCourseId "course_id"]
        $ select and [hasTag "topic"] objects

allPersons objects filename = do
    writeFile filename
        $ to_csv "service,course_id,group_id,person_id,name,email\n"
        $ unique
        $ extract [exService,
                   exAttr "course_id",
                   exAttr "group",
                   exAttr "id",
                   exAttr "name",
                   exAttr "email"]
        $ update [upCourseId "course_id"]
        $ select and [hasTag "person", notInPath "topics.xml"] objects

allLecturers objects filename = do
    writeFile filename
        $ to_csv "service,course_id,group_id,person_id,type\n"
        $ unique
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

allTopicPersonAssignments objects filename = do
    writeFile filename
        $ to_csv "service,course_id,group_id,person_id,topic_id\n"
        $ unique
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

allUnittestResults objects filename = do
    writeFile filename
        $ to_csv "service,course_id,group_id,person_id,topic_id,date,success,warning,failure,error,info,timeout\n"
        $ extract [exService,
                   exAttr "course_id",
                   exAttr "group_id",
                   exAttr "identifier",
                   exAttr "theme",
                   exAttr "timestamp",
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

main = do
    objects <- selectFS and [inService "Code"]
    allStates                 objects "code_states.csv"
    allInstances              objects "code_courses.csv"
    allTopics                 objects "code_topics.csv"
    allPersons                objects "code_persons.csv"
    allLecturers              objects "code_lecturers.csv"
    allTopicPersonAssignments objects "code_topic_person_assignments.csv"
    allUnittestResults        objects "code_unittest_results.csv"
