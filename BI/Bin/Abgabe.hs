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

selectPersons objects = unique
        $ extract [exService,
                   exAttr "course_id",
                   exAttr "group",
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

-- initial lists [service,course_id,group_id,person_id,name,email]
--               [service,course_id,group_id,person_id,type]
-- keys are: [service,course_id,group_id,person_id]
-- merge on [service,course_id,group_id,person_id] -> [name,email,type]
-- final structure: [service,course_id,group_id,person_id,name,email,type]
-- 6th column's default value is student
mergePersonsLecturers objects = leftJoin [0..3] (selectPersons objects) (selectLecturers objects)
    [(6,\key -> if isPrefixOf "a" (key!!3) then "student" else "")]

allInstances objects filename = do
    writeFile filename
        $ to_csv "service,course_id,description\n"
        $ unique
        $ extract [exService, exAttr "id", exText]
        $ select and [hasTag "instance"] objects

allPersonLecturers objects filename = do
    writeFile filename
        $ to_csv "service,course_id,group_id,person_id,name,email,type\n"
        $ mergePersonsLecturers objects

allAssessmentResults objects filename = do
    writeFile filename
        $ to_csv "service,course_id,user_id,result_id,result_value\n"
        $ extract [
            exService,
            exAttr "course_id",
            exAttr "user_id",
            exAttr "id",
            exText]
        $ select and [hasTag "result"]
        $ update [pushDown "id" "user_id",
                  pushDown "course_id" "course_id",
                  upCourseId "course_id"]
        $ select and [hasTag "person", inPath "assessments.xml"] objects

allAssessmentPlus objects filename = do
    writeFile filename
        $ to_csv "service,course_id,user_id,plus_date\n"
        $ extract [
            exService,
            exAttr "course_id",
            exAttr "user_id",
            exAttr "date"]
        $ select and [hasTag "plus"]
        $ update [pushDown "id" "user_id",
                  pushDown "course_id" "course_id",
                  upCourseId "course_id"]
        $ select and [hasTag "person", inPath "assessments.xml"] objects

allFeedback objects filename = do
    writeFile filename
        $ to_csv "service,course_id,user_id,task,subtask,author,comment_length\n"
        $ extract [
            exService,
            exAttr "course_id",
            exAttr "user_id",
            exAttr "task",
            exAttr "subtask",
            exAttr "author",
            exAttr "comment_length"]
        $ update [upLength "comment" "comment_length",
                  pullUp (\o -> concat
                              $ extract [exText] [o]) "comment"]
        $ select and [hasTag "comment"]
        $ update [pushDown "id" "user_id",
                  pushDown "course_id" "course_id",
                  upCourseId "course_id"]
        $ select and [hasTag "person", inPath "feedback.xml"] objects

main = do
    objects <- selectFS and [inService "Abgabe"]
    allInstances              objects "abgabe_courses.csv"
    -- TODO: the merging seems to be wrong. there are some person elements with empty group
    allPersonLecturers        objects "abgabe_person_lecturers.csv" -- merged with courses.xml
    allAssessmentResults      objects "abgabe_assessment_results.csv"
    allAssessmentPlus         objects "abgabe_assessment_pluses.csv"
    allFeedback               objects "abgabe_feedback.csv"
    -- tasks.xml
    -- uploaded files
