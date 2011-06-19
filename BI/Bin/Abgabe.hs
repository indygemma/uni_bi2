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

upKurs name x@(Object service path tag theText ttype attrMap attrTMap children) =
    Object service path tag theText ttype newMap attrTMap children
    where newMap = Map.insert name result attrMap
          result = case theText of
                       Just x -> map (\idx -> x !! idx) [43,44]
                       Nothing -> ""

upSemester name x@(Object service path tag theText ttype attrMap attrTMap children) =
    Object service path tag theText ttype newMap attrTMap children
    where newMap = Map.insert name result attrMap
          result = case theText of
                       Just x -> case map (\idx -> x !! idx) [48,49] of
                                     "/s" -> map(\idx -> x !! idx) [51,52]
                                     _ -> map (\idx -> x !! idx) [48,49]
                        
                       Nothing -> ""

selectCourses objects = unique
        $ extract [exService,
                   exAttr "id",
                   exAttr "kurs",
                   exAttr "semester",
                   exText]
        $ update [upKurs "kurs",
                  upSemester "semester"]
        $ select and [hasTag "instance"] objects

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

selectAssessmentPlus objects =
        extract [
            exService,
            exAttr "course_id",
            exAttr "user_id",
            exAttr "date"]
        $ select and [hasTag "plus"]
        $ update [pushDown "id" "user_id",
                  pushDown "course_id" "course_id",
                  upCourseId "course_id"]
        $ select and [hasTag "person", inPath "assessments.xml"] objects

selectAssessmentResults objects =
        unique
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

selectFeedback objects =
    extract [
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

-- initial lists [service,course_id,group_id,person_id,name,email]
--               [service,course_id,group_id,person_id,type]
-- keys are: [service,course_id,group_id,person_id]
-- merge on [service,course_id,group_id,person_id] -> [name,email,type]
-- final structure: [service,course_id,group_id,person_id,name,email,type]
-- 6th column's default value is student
mergePersonsLecturers objects = leftJoin [0..3] (selectPersons objects) (selectLecturers objects)
    [(6,\key -> if isPrefixOf "a" (key!!3) then "student" else "")]

-- initial lists [service,course_id,kurs,semester,description]
--               [service,course_id,user_id,plus_date]
-- keys are: [service,course_id]
-- final structure: [service,course_id,kurs,semester,description,user_id,plus_date]
mergeCourseAssessmentPlus objects = leftJoin [0,1] (selectCourses objects) (selectAssessmentPlus objects) []

-- initial lists [service,course_id,user_id,result_id,result_value]
--               [service,course_id,user_id,plus_date]
-- keys are: [service,course_id,user_id]
-- final structure: [service,course_id,user_id,result_id,result_value,plus_date]
mergeAssessmentResultWithPlus objects = leftJoin [0..2] (selectAssessmentResults objects) (selectAssessmentPlus objects) []

-- initial lists [service,course_id,user_id,result_id,result_value,plus_date]
--               [service,course_id,kurs,semester,description]
-- keys are: [service,course_id]
-- final structure: [service,course_id,user_id,result_id,result_value,plus_date,kurs,semester,description]
mergeWithCourses otherSelect objects = leftJoin [0,1] (selectCourses objects) otherSelect []

allInstances objects filename = do
    writeFile filename
        $ to_csv "service,course_id,kurs,semester,description\n"
        $ selectCourses objects

allPersonLecturers objects filename = do
    writeFile filename
        $ to_csv "service,course_id,group_id,person_id,name,email,type\n"
        $ mergePersonsLecturers objects

allAssessmentResults objects filename = do
    writeFile filename
        $ to_csv "service,course_id,user_id,result_id,result_value\n"
        $ selectAssessmentResults objects

allAssessmentPlus objects filename = do
    writeFile filename
        $ to_csv "service,course_id,user_id,plus_date\n"
        $ selectAssessmentPlus objects

allFeedback objects filename = do
    writeFile filename
        $ to_csv "service,course_id,user_id,task,subtask,author,comment_length\n"
        $ selectFeedback objects

courseAssessmentPlus objects filename = do
    writeFile filename
        $ to_csv "service,course_id,kurs,semester,description,user_id,plus_date\n"
        $ mergeCourseAssessmentPlus objects

courseAssessmentResults objects filename = do
    writeFile filename
        $ to_csv "service,course_id,kurs,semester,description,user_id,result_id,result_value\n"
        $ mergeWithCourses (selectAssessmentResults objects) objects

courseFeedback objects filename = do
    writeFile filename
        $ to_csv "service,course_id,kurs,semester,description,user_id,task,subtask,author,comment_length\n"
        $ mergeWithCourses (selectFeedback objects) objects

main = do
    objects <- selectFS and [inService "Abgabe"]
    allInstances              objects "abgabe_courses.csv"
    -- TODO: the merging seems to be wrong. there are some person elements with empty group
    allPersonLecturers        objects "abgabe_person_lecturers.csv" -- merged with courses.xml
    allAssessmentResults      objects "abgabe_assessment_results.csv"
    allAssessmentPlus         objects "abgabe_assessment_pluses.csv"
    allFeedback               objects "abgabe_feedback.csv"
    courseAssessmentPlus      objects "abgabe_assessment_pluses_courses.csv"
    courseAssessmentResults   objects "abgabe_assessment_results_courses.csv"
    courseFeedback            objects "abgabe_feedback_courses.csv"
    -- tasks.xml
    -- uploaded files
