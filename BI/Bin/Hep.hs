module Main where

import BI.Api
import BI.Types
import Data.List
import qualified BI.Queries as Q
import qualified BI.Transformations as T
import qualified Data.Map as Map
import System.FilePath
import System.Posix.Files
import System.Time
import Text.Printf

-- TODO: problem in timestamp conversion when digit < 10, without prepended 0 it will come up later after sort
-- TODO: how to differentiate between exercise upload and milestone upload?
-- TODO: have to track *.sql *.zip
-- TODO: write transformation for timestamps in all events
-- TODO: write mapping of the defined process actions to the data here (are all events covered)
-- TODO: write queries for registration events

-- DONE: Refactor out the most common logic
-- DONE: write csv files in the following format: matrikelnummer_courseid_semester.csv
--       --> Find out if the course_id are always equal in all occurences
--       --> Need Update function to extract current semester.
-- DONE: find out where to commonly extract semester
-- DONE: filter out hep-specific courses only
-- DONE: write out extra information as JSON on a single line in final CSV
-- DONE: how to track if a person has uploaded? If possible don't do this, have to regenerate index
--      --> similar to Extract.hs line 62+ where I extract the time for "container.h"
--      --> have to track the modifed times of every file under data/Abgabe/{course_id}/Data/{matrikelnr}/{task_id}/{subtask_id}/*.pdf
-- DONE: post-step, remove first three columns
-- DONE: change output to be {kurs}/{semester}/{matrikelnr}.csv

-- selectCourses schema: [service, course_id, kurs, semester, description]
-- selectCourses data: [["Abgabe", "10", "02", "04", "description"],...]
-- selectUnitestResults schema: [service, course_id, group_id, identifier, theme, timestamp, year, month, day,
-- selectAssessmentPlus schema: [service, course_id, user_id, date]
-- selectAssessmentResults schema: [service, course_id, user_id, id, text]
-- selectFeedback schema: [service, course_id, user_id, task, subtask, author, comment_length]

isKursSemester :: String -> String -> Object -> Bool
isKursSemester kurs semester obj =
    and [exAttr "semester" obj == semester,
         exAttr "kurs"     obj == kurs]

filterKursSemester :: [(String,String)] -> [Object] -> [Object]
filterKursSemester xs = filter (\x ->
    any id $ map (\(kurs,semester) ->
    isKursSemester kurs semester x) xs)

hepCourses = filterKursSemester [
        ("02", "03"), -- DBS
        ("02", "04"), -- DBS
        ("00", "00") -- AlgoDat
    ]

-- Code Stuff

selectUnittestResults objects = extract [
            exAttr "matrikelnr",
            exAttr "kurs",
            exAttr "semester",
            exAttr "iso_date",
            exAttr "event",
            exAttr "extra"
        ]
        $ update [T.upJSON "extra" [
            "matrikelnr",
            "kurs",
            "semester",
            "event",
            "course_id",
            "group_id",
            "timestamp",
            "iso_date", -- TODO: fix the type in Extract.hs where I wrote this field as iso_date
            "theme",
            "SUCCESS",
            "WARNING",
            "FAILURE",
            "ERROR",
            "INFO",
            "TIMEOUT"
        ]]
        $ update [T.upAttr "event" "code unittest run",
                  T.upAttrValue "matrikelnr" "identifier"]
        $ hepCourses
        $ mergeWithCourses objects
        $ Q.objUnittestResults objects

-- Forum stuff

selectForumEntries objects = extract [
        exAttr "matrikelnr",
        exAttr "kurs",
        exAttr "semester",
        exAttr "date", -- TODO: convert to iso_date!
        exAttr "event",
        exAttr "extra"
    ]
    -- TODO: check all attributes that exist for these objects
    $ update [T.upJSON "extra" [
        "course_id",
        "name",
        "nid",
        "id",
        "parent_id",
        "date",
        "subject",
        "subject_length",
        "text_length"
    ]]
    $ update [T.upAttr "event" "forum entry",
              T.upAttrValue "matrikelnr" "user"]
    $ hepCourses
    $ mergeWithCourses objects
    $ Q.objForumEntries objects

-- Abgabe stuff

mergeWithCourses objects otherObjects =
    objLeftJoin [(exService, exService),
                 (exAttr "id", exAttr "course_id")] -- "id" in course data, "course_id" in others
                (Q.objCourses objects)
                otherObjects

selectAssessmentResultsCourses objects = extract [
        exAttr "matrikelnr",
        exAttr "kurs",
        exAttr "semester",
        exAttr "iso_datetime", -- TODO: implement iso_datetime for evaluations!
        exAttr "event",
        exAttr "extra"
    ]
    -- TODO: check all attributes that exist for these objects
    $ update [T.upJSON "extra" [
        "course_id",
        "user_id",
        "id",
        "score"
    ]]
    $ update [T.upAttr "event" "Evaluation",
              T.upAttrValue "matrikelnr" "user_id"]
    $ hepCourses
    $ mergeWithCourses objects
    $ Q.objAssessmentResults objects

selectAssessmentPlusCourses objects = extract [
        exAttr "matrikelnr",
        exAttr "kurs",
        exAttr "semester",
        exAttr "iso_datetime", -- TODO: implement iso_datetime for pluses!
        exAttr "event",
        exAttr "extra" -- TODO: implement "extra" for pluses
    ]
    -- TODO: check all attributes that exist for these objects
    {--- TODO: convert extra data to JSON and write out in single line-}
    $ hepCourses
    $ update [T.upAttr "event" "plus",
              T.upAttrValue "matrikelnr" "user_id"]
    $ mergeWithCourses objects
    $ Q.objAssessmentPlus objects
    -- left:pdf right:assessment
    {-$ objLeftJoin [(exService, exService),-}
                   {-(exAttr "course_id", exAttr "course_id"),-}
                   {-(exAttr "matrikelnr", exAttr "user_id" ),-}
                   {-Q.objAssessmentPlus objects-}


selectFeedbackCourses objects = extract [
        exAttr "matrikelnr",
        exAttr "kurs",
        exAttr "semester",
        exAttr "iso_datetime", -- TODO: implement iso_datetime for feedback!
        exAttr "event",
        exAttr "extra" -- TODO: implement "extra" for feedback!
    ]
    -- TODO: check all attributes that exist for these objects
    {--- TODO: convert extra data to JSON and write out in single line-}
    $ hepCourses
    $ update [T.upAttr "event" "feedback",
              T.upAttrValue "matrikelnr" "user_id"]
    $ mergeWithCourses objects
    $ Q.objFeedback objects

selectPDFFiles objects = extract [
        exAttr "matrikelnr",
        exAttr "kurs",
        exAttr "semester",
        exAttr "iso_datetime",
        exAttr "event",
        exAttr "extra"
    ]
    $ update [T.upJSON "extra" [
        "matrikelnr",
        "kurs",
        "semester",
        "course_id",
        "timestamp",
        "iso_datetime",
        "task_id",
        "subtask_id",
        "filename"
    ]]
    $ update [T.upAttr "event" "Excercise upload"] -- TODO: have to differentiate between exercise and milestone!
    $ hepCourses
    $ mergeWithCourses objects
    $ objPDFFiles objects

objPDFFiles objects =
    update [T.upCourseId         "course_id" ,
            T.upAbgabeMatrikelNr "matrikelnr",
            T.upAbgabeTaskId     "task_id"   ,
            T.upAbgabeSubTaskId  "subtask_id",
            T.upAbgabeFilename   "filename"  ]
    $ select and [hasTag "pdf", inPath ".pdf"] objects

groupAll code_objects forum_objects abgabe_objects = grouped
    where forum           = selectForumEntries forum_objects
          code            = selectUnittestResults code_objects
          abgabe_pluses   = selectAssessmentPlusCourses abgabe_objects
          abgabe_results  = selectAssessmentResultsCourses abgabe_objects
          abgabe_feedback = selectFeedbackCourses abgabe_objects
          abgabe_uploads  = selectPDFFiles abgabe_objects
          {-combined     = forum ++ code ++ abgabe_results-}
          {-combined     =  concat [forum,code,abgabe_pluses,abgabe_results,abgabe_results]-}
          combined     =  concat [forum,code,abgabe_pluses,abgabe_results,abgabe_feedback, abgabe_uploads]
          validStudentsOnly = filter (\x -> all id [isPrefixOf "a" (x!!0),
                                                   length (x!!0) == length "a0607688"])
          fields x y   = all id [(x!!0) == (y!!0),
                                 (x!!1) == (y!!1),
                                 (x!!2) == (y!!2)]
          grouped      = groupBy fields $ sort $ validStudentsOnly $ combined
          {-grouped      = groupBy fields $ sort $ combined-}

main = do
    code_objects  <- selectFS and [inService "Code"]
    forum_objects <- selectFS and [inService "Forum"]
    abgabe_objects <- selectFS and [inService "Abgabe"]
    mapM (\group -> do
        let row       = group !! 0
        -- setup pre_name matrikelnummer=0, kurs=1, semester=2
        let matrikelnr = row!!0
        let kurs = row!!1
        let semester = row!!2
        let filename  = printf "%s.csv" matrikelnr
        let fullpath  = joinPath ["hep", "KURS"++kurs, "se"++semester, filename]
        -- TODO: have to create non-existent paths
        -- NOTE: somehow there are duplicate entries? nub them
        let content   = to_csv "" $ nub $ sort $ map (\row -> [row!!3,row!!4,row!!5]) group
        writeFile fullpath content
        return ()
        ) $ groupAll code_objects forum_objects abgabe_objects

main2 = do
    objects <-  selectFS and [inService "Abgabe"]
    writeFile "test.csv" $ to_csv "" $ selectFeedbackCourses objects
