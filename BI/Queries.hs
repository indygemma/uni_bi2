module BI.Queries where

import BI.Api
import BI.Types
import qualified BI.Transformations as T
import Data.List
import qualified Data.Map as Map

--
-- Common Queries
--
selectLecturers objects = unique
        $ extract [exService,
                   exAttr "course_id",
                   exAttr "group_id",
                   exAttr "id",
                   exTag]
        $ objLecturers objects

objLecturers objects =
        select or [hasTag "lecturer", hasTag "tutor"]
        $ update [pushDown "id" "group_id",
                  pushDown "course_id" "course_id",
                  T.upCourseId "course_id"]
        $ select and [hasTag "group", inPath "course.xml"] objects

selectCourses objects = unique
        $ extract [exService,
                   exAttr "id",
                   exAttr "kurs",
                   exAttr "semester",
                   exText]
        $ objCourses objects

objCourses objects =
        update [T.upKurs "kurs",
                T.upSemester "semester"]
        $ select and [hasTag "instance"] objects

--
-- Abgabe Queries
--
selectPersons objects = unique
        $ extract [exService,
                   exAttr "course_id",
                   exAttr "group",
                   exAttr "id",
                   exAttr "name",
                   exAttr "email"]
        $ update [T.upCourseId "course_id"]
        $ select and [hasTag "person", notInPath "topics.xml"] objects

selectAssessmentPlus objects =
        extract [
            exService,
            exAttr "course_id",
            exAttr "user_id",
            exAttr "date"]
        $ objAssessmentPlus objects

objAssessmentPlus objects =
        select and [hasTag "plus"]
        $ update [pushDown "id" "user_id",
                  pushDown "course_id" "course_id",
                  T.upCourseId "course_id"]
        $ select and [hasTag "person", inPath "assessments.xml"] objects

selectAssessmentResults objects =
        unique
        $ extract [
            exService,
            exAttr "course_id",
            exAttr "user_id",
            exAttr "id",
            exText]
        $ objAssessmentResults objects

objAssessmentResults objects =
        select and [hasTag "result"]
        $ update [pushDown "id" "user_id",
                  pushDown "course_id" "course_id",
                  T.upCourseId "course_id"]
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
    $ objFeedback objects

objFeedback objects =
    update [upLength "comment" "comment_length",
            pullUp (\o -> concat $ extract [exText] [o]) "comment"]
    $ select and [hasTag "comment"]
    $ update [pushDown "id" "user_id",
              pushDown "course_id" "course_id",
              T.upCourseId "course_id"]
    $ select and [hasTag "person", inPath "feedback.xml"] objects

-- initial lists [service,course_id,group_id,person_id,name,email]
--               [service,course_id,group_id,person_id,type]
-- keys are: [service,course_id,group_id,person_id]
-- merge on [service,course_id,group_id,person_id] -> [name,email,type]
-- final structure: [service,course_id,group_id,person_id,name,email,type]
-- 6th column's default value is student
mergeAbgabePersonsLecturers objects = leftJoin [0..3] (selectPersons objects) (selectLecturers objects)
    [(6,\key -> if isPrefixOf "a" (key!!3) then "student" else "")]

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

--
-- Code Queries
--

-- | Select statements | --

-- like selectPersons but with default group of 1
selectCodePersons objects = unique
        $ extract [exService,
                   exAttr "course_id",
                   -- IMPORTANT: we have to set "1" as group because we want to join with lecturerer
                   -- list later, which only defines groups with 1. There are no other groups in lecturers.
                   exAttrDefault "group" "1",
                   exAttr "id",
                   exAttr "name",
                   exAttr "email"]
        $ update [T.upCourseId "course_id"]
        $ select and [hasTag "person", notInPath "topics.xml"] objects

selectTopics objects = extract [exService, exAttr "course_id", exAttr "id", exText]
        $ update [T.upCourseId "course_id"]
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
                  T.upCourseId "course_id"]
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
        $ objUnittestResults objects

objUnittestResults objects =
        update [T.upCourseId "course_id",
                T.upGroupId  "group_id",
                T.upUnittestStates]
        $ select and [hasTag "test", inPath "resUnit.xml"] objects

-- | Merge Statements | --

-- initial lists [service,course_id,group_id,person_id,name,email]
--               [service,course_id,group_id,person_id,type]
-- keys are: [service,course_id,group_id,person_id]
-- merge on [service,course_id,group_id,person_id] -> [name,email,type]
-- final structure: [service,course_id,group_id,person_id,name,email,type]
-- 6th column's default value is student
mergeCodePersonsLecturers objects = leftJoin [0..3] (selectCodePersons objects) (selectLecturers objects)
    [(6,\key -> if isPrefixOf "a" (key!!3) then "student" else "")]

-- IMPORTANT: the topic assignments taken from topics.xml <peron> tag is not consistent with the values
--            taken from resUnit.xml results <test theme="{ topic_id }">. That's why we don't join from
--            the topics.xml but from the values taken from resUnit.xml
-- merge in the topic id a given person has taken
-- initial lists [service,course_id,group_id,person_id,name,email,type]
--               [service,course_id,group_id,person_id,topic_id]
-- keys are: [service,course_id,group_id,person_id]
-- final structure [service,course_id,group_id,person_id,name,email,type,topic_id]
mergeTopicIds objects = leftJoin [0..3] (mergeCodePersonsLecturers objects) (selectTopicAssignments objects) []

-- merge in the unittest results
-- initial lists [service,course_id,group_id,person_id,name,email,type]
--               [service,course_id,group_id,person_id,topic_id,timestamp,year,month,day,hour,min,sec,SUCCESS,WARNING,FAILURE,ERROR,INFO,TIMEOUT]
-- keys are: [service,course_id,group_id,person_id,topic_id]
-- final structure: [service,course_id,group_id,person_id,name,email,type,
--                   topic_id,timestamp,year,month,day,hour,min,sec,
--                   SUCCESS,WARNING,FAILURE,ERROR,INFO,TIMEOUT]
mergeUnittestResults objects = leftJoin [0..3] (mergeCodePersonsLecturers objects) (selectUnittestResults objects) []

-- merge in the topic description
-- initial lists [service,course_id,group_id,person_id,name,email,topic_id,date,
--                SUCCESS,WARNING,FAILURE,ERROR,INFO,TIMEOUT]
--               [service,course_id,group_id,topic_id,text]
-- keys are: [service,course_id,topic_id]
-- final structure [service,course_id,topic_id,group_id,person_id,name,email,type,topic_description]
{-mergeTopicDescription objects = leftJoin [0,1,7] (mergeUnittestResults objects) (selectTopics objects) []-}

--
-- Forum-specific Queries
--
selectIssues objects =
    extract [exService,
             exAttr "course_id",
             exAttr "issue_id",
             exAttr "what"]
             {-exText]-} -- description not required / important
    $ select and [hasTag "text"]
    $ update [pushDown "course_id" "course_id",
              pushDown "id"        "issue_id",
              pushDown "what"      "what",
              T.upCourseId "course_id"]
    $ select and [hasTag "entry"]
    $ select and [hasTag "issues"] objects

-- NOTE: user "voonly". only occurs in some forum entries but nowhere in
--       persons.xml. who is this?
selectForumEntries objects =
    extract [exService,
             exAttr "course_id",
             exAttr "user",
             exAttr "name",
             exAttr "nid",
             exAttr "id",
             exAttr "parent_id",
             exAttr "date",
             exAttr "subject_length",
             exAttr "text_length"]
    $ objForumEntries objects
    
objForumEntries objects =
    update [
        upLength "subject" "subject_length",
        upLength "text" "text_length",
        pullUp (\o -> concat $ extract [exText] $ select and [hasTag "subject"] $ oChildren o) "subject",
        pullUp (\o -> concat $ extract [exText] $ select and [hasTag "text"] $ oChildren o) "text",
        T.upCourseId "course_id"]
    $ select and [hasTag "entry"]
    $ update [ T.upParentID "parent_id" ]
    $ liftChildren and [hasTag "entry"]
    $ update [pushDown "nid" "nid",
              T.upCourseId "course_id"]
    $ select and [hasTag "entries"] objects

selectForum99Entries objects =
    extract [exService,
             exAttr "course_id",
             exAttr "user",
             exAttr "nid",
             exAttr "id",
             exAttr "subject_length",
             exAttr "text_length"]
    $ select and [attrEq "course_id" "99"]
    $ objForumEntries objects

selectCodeServiceUsersInForum objects = unique
    $ extract [exAttr "course_id", exAttr "user"]
    $ update [
        upLength "subject" "subject_length",
        upLength "text" "text_length",
        pullUp (\o -> concat $ extract [exText] $ select and [hasTag "subject"] $ oChildren o) "subject",
        pullUp (\o -> concat $ extract [exText] $ select and [hasTag "text"] $ oChildren o) "text",
        T.upCourseId "course_id"]
    $ select and [hasTag "entry"]
    $ update [ T.upParentID "parent_id" ]
    $ liftChildren and [hasTag "entry"]
    $ update [pushDown "nid" "nid",
              T.upCourseId "course_id"]
    $ select and [hasTag "entries"] objects

