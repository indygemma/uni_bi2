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

-- TODO: modify the csv structure: timestamp, instance, user_id, user type, event, extra
-- TODO: write transformation for timestamps in all events. left: forum date (have to append Timezone)
-- TODO: modify activity label according to new PDF
-- TODO: how to differentiate between exercise upload and milestone upload?
-- TODO: write mapping of the defined process actions to the data here (are all events covered)
-- TODO: randomize registration date between begin-reg and end-reg

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
-- DONE: problem in timestamp conversion when digit < 10, without prepended 0 it will come up later after sort
-- DONE: "score" in assessment is referencing the wrong thing, why? maybe after join the exText is different?
-- DONE: have to track *.sql *.zip, generalize pdf tracking + object creation (in this case the xml processing is a special case)
-- DONE: write queries for registration events
-- DONE: write out a version where all the data is stored in one place

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

extractHEPStudentGroup = extract [
        exAttr "matrikelnr",
        exAttr "kurs",
        exAttr "semester",
        exAttr "iso_datetime",
        exAttr "event",
        exAttr "extra"
    ]

extractHEPCSGroup = extract [
        exAttr "kurs",
        exAttr "semester",
        exAttr "matrikelnr",
        exAttr "iso_datetime",
        exAttr "event",
        exAttr "extra"
    ]

-- Register Stuff
-- TODO: there's no specific time when a student enrolled, just use begin-reg
-- 2011-01-22T18:23:14;Registration/20;Enroll;{"slot": 11, "unit":, "time": "11:30-11:45"}
selectRegistrations objects =
    update [
        T.upAttrValue "iso_datetime" "begin-reg",
        T.upAttr "event" "Enrollment",
        T.upJSON "extra" [
            "group_id", "title", "description", "slot_id", "units",
            "slot_info", "course_id", "service", "unit"
        ]
    ]
    $ hepCourses
    $ mergeWithCourses objects
    $ objRegistrationData objects

-- + selectRegistrationGroups
-- group_id=1, title="Abgabegespräch", description="description",
-- slot_id=1, units=3, slot_info="Dienstag ..."
-- + objRegisterTimes
-- mode=person, begin-reg=2010-01-13T14:00:00", end-reg=2010-01-14T16:00:00, course_id=5, service=Register
-- + objRegistrationsForStudents
-- unit=1, group=1, slot=1, matrikelnr=a0607688, course_id=5, service=Register
objRegistrationData objects =
    objLeftJoin [(exAttr "service" , exAttr "service"),
                 (exAttr "group_id", exAttr "group"),
                 (exAttr "slot_id" , exAttr "slot"),
                 (exAttr "course_id", exAttr "course_id")]
                 (selectRegistrationGroups objects)
                 $ mergeRegisterTimesForStudents objects


-- mode=person, begin-reg=2010-01-13T14:00:00", end-reg=2010-01-14T16:00:00, course_id=5, service=Register
-- unit=1, group=1, slot=1, matrikelnr=a0607688, course_id=5, service=Register
selectRegisterTimesForStudents objects =
    extract [
        exAttr "service",
        exAttr "course_id",
        exAttr "mode",
        exAttr "begin-reg",
        exAttr "end-reg",
        exAttr "unit",
        exAttr "group",
        exAttr "slot",
        exAttr "matrikelnr"
    ]
    $ mergeRegisterTimesForStudents objects

mergeRegisterTimesForStudents objects =
    objLeftJoin [
        (exAttr "service", exAttr "service"),
        (exAttr "course_id", exAttr "course_id")
    ] (objRegisterTimes objects)
      (objRegistrationsForStudents objects)

-- group_id=1, title="Abgabegespräch", description="description",
-- slot_id=1, units=3, slot_info="Dienstag ..."
selectRegistrationGroups objects =
    -- 6) copy "id" as "slot_id" in <slot>
    update [T.upAttrValue "slot_id" "id",
            T.upAttrLookup "slot_info" exText,
            T.upCourseId "course_id",
            T.upAttrLookup "service" exService]
    -- 5) lift <group> children <slots> and select them
    $ select and [hasTag "slot"]
    $ liftChildren and [hasTag "slot"]
    $ update [
        -- 4) push down <group>'s "description" as <slot>'s "description"
        pushDown "description" "description",
        -- 3) pull up <desc> exText as "description" in <group>
        pullUp (\o -> concat $ extract [exText] $ select and [hasTag "desc"] $ oChildren o) "description",
        -- 2) push down <group>'s "title" as <slot>'s "title"
        pushDown "title" "title",
        -- 1) push down <group>'s "id" as <slot>'s "group_id"
        pushDown "id" "group_id"
    ]
    $ select and [hasTag "group", inService "Register"] objects

selectRegisterTimes objects = extract [
        exAttr "mode"
    ] $ objRegisterTimes objects

-- mode=person, begin-reg=2010-01-13T14:00:00", end-reg=2010-01-14T16:00:00, course_id=5, service=Register
objRegisterTimes objects =
    update [T.upCourseId "course_id",
            T.upAttrLookup "service" exService]
    $ select and [hasTag "register",
                  inPath "custom.xml",
                  inService "Register"] objects

selectRegistrationsForStudents objects = extract [
        exAttr "matrikelnr",
        exAttr "group",
        exAttr "slot",
        exAttr "course_id",
        exAttr "service"
    ] $ objRegistrationsForStudents objects

-- unit=1, group=1, slot=1, matrikelnr=a0607688, course_id=5, service=Register
objRegistrationsForStudents objects =
    update [T.upAttrLookup "matrikelnr" exText,
            T.upCourseId "course_id",
            T.upAttrLookup "service" exService]
    $ select and [hasTag "registration",
                  inPath "registrations.xml",
                  inService "Register"] objects

-- Code Stuff

selectUnittestResults objects =
        update [T.upJSON "extra" [
            "matrikelnr",
            "kurs",
            "semester",
            "event",
            "service",
            "course_id",
            "group_id",
            "timestamp",
            "iso_datetime",
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

selectForumEntries objects =
    -- TODO: convert to iso_datetime!
    -- TODO: check all attributes that exist for these objects
    update [T.upJSON "extra" [
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
              T.upAttrValue "matrikelnr" "user",
              T.upAttrValue "iso_datetime" "date"]
    $ hepCourses
    $ mergeWithCourses objects
    $ Q.objForumEntries objects

-- Abgabe stuff

mergeWithCourses objects otherObjects =
    objLeftJoin [(exService, exService),
                 (exAttr "id", exAttr "course_id")] -- "id" in course data, "course_id" in others
                (Q.objCourses objects)
                otherObjects

selectAssessmentResultsCourses objects =
    -- TODO: implement iso_datetime for evaluations!
    -- TODO: check all attributes that exist for these objects
    update [T.upJSON "extra" [
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

selectAssessmentPlusCourses objects =
    -- TODO: implement iso_datetime for pluses!
    -- TODO: implement "extra" for pluses
    -- TODO: check all attributes that exist for these objects
    -- TODO: the person who gave the plus is the first person under <group> in course.xml
    {--- TODO: convert extra data to JSON and write out in single line-}
    hepCourses
    $ update [T.upAttr "event" "plus",
              T.upAttrValue "matrikelnr" "user_id"]
    $ mergeWithCourses objects
    $ Q.objAssessmentPlus objects
    -- left:pdf right:assessment
    {-$ objLeftJoin [(exService, exService),-}
                   {-(exAttr "course_id", exAttr "course_id"),-}
                   {-(exAttr "matrikelnr", exAttr "user_id" ),-}
                   {-Q.objAssessmentPlus objects-}

selectFeedbackCourses objects =
    -- TODO: implement iso_datetime for feedback!
    -- TODO: implement "extra" for feedback!
    -- TODO: check all attributes that exist for these objects
    {--- TODO: convert extra data to JSON and write out in single line-}
    hepCourses
    $ update [T.upAttr "event" "feedback",
              T.upAttrValue "matrikelnr" "user_id"]
    $ mergeWithCourses objects
    $ Q.objFeedback objects

mergeAbgabePersons 

-- this is the list of persons that exist in an Abgabe service per course_id
-- admin="true"
-- name="string"
-- id="person002" etc
-- email="string"
-- group="2"
-- team=""
selectAbgabePersons objects = select and [hasTag "person", inPath "persons.xml"] objects

-- TODO: extract all the other files: sql, zip, find out correct upload type (using course_id)
selectPDFFiles objects =
    update [T.upJSON "extra" [
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
    $ update [T.upAttr "event" "Exercise upload"] -- TODO: have to differentiate between exercise and milestone!
    $ hepCourses
    $ mergeWithCourses objects
    $ objPDFFiles objects

objPDFFiles objects =
    update [T.upCourseId         "course_id" ,
            T.upAbgabeMatrikelNr "matrikelnr",
            T.upAbgabeTaskId     "task_id"   ,
            T.upAbgabeSubTaskId  "subtask_id",
            T.upAbgabeFilename   "filename"  ,
            T.upAttrLookup       "service" exService]
    $ select and [hasTag "pdf", inPath ".pdf"] objects

groupAll code_objects forum_objects abgabe_objects register_objects = grouped
    where combined = selectHEP extractHEPStudentGroup code_objects forum_objects abgabe_objects register_objects
          validStudentsOnly = filter (\x -> all id [isPrefixOf "a" (x!!0),
                                                   length (x!!0) == length "a0607688"])
          fields x y   = all id [(x!!0) == (y!!0),
                                 (x!!1) == (y!!1),
                                 (x!!2) == (y!!2)]
          grouped      = groupBy fields $ sort $ validStudentsOnly $ combined

groupByCourseSemester code_objects forum_objects abgabe_objects register_objects = grouped
    where combined = selectHEP extractHEPCSGroup code_objects forum_objects abgabe_objects register_objects
          validStudentsOnly = filter (\x -> all id [isPrefixOf "a" (x!!2),
                                                   length (x!!2) == length "a0607688"])
          fields x y = all id [(x!!0) == (y!!0),
                               (x!!1) == (y!!1)]
          grouped    = groupBy fields $ sort $ validStudentsOnly $ combined

selectHEP extraction code_objects forum_objects abgabe_objects register_objects = combined
    where forum           = extraction $ selectForumEntries forum_objects
          code            = extraction $ selectUnittestResults code_objects
          abgabe_pluses   = extraction $ selectAssessmentPlusCourses abgabe_objects
          abgabe_results  = extraction $ selectAssessmentResultsCourses abgabe_objects
          abgabe_feedback = extraction $ selectFeedbackCourses abgabe_objects
          abgabe_uploads  = extraction $ selectPDFFiles abgabe_objects
          register        = extraction $ selectRegistrations register_objects
          combined     =  concat [forum,code,abgabe_pluses,abgabe_results,abgabe_feedback, abgabe_uploads, register]

writeGroupedInstances code_objects forum_objects abgabe_objects register_objects = 
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
        ) $ groupAll code_objects forum_objects abgabe_objects register_objects

writeInstancesSingleFile code_objects forum_objects abgabe_objects register_objects =
    mapM (\course -> do
        let row      = course !! 0
        let kurs     = row !! 0
        let semester = row !! 1
        let filename = printf "all.csv"
        let fullpath = joinPath ["hep", "KURS"++kurs, "se"++semester, filename]
        let content = to_csv "" $ nub $ sort $ map (\row -> [
                            row !! 3, -- timestamp
                            row !! 2, -- matrikelnummer
                            row !! 4, -- event
                            row !! 5 -- extra data
                        ]) course
        {-let content = show $ course-}
        writeFile fullpath content
        return ()
    ) $ groupByCourseSemester code_objects forum_objects abgabe_objects register_objects

main = do
    code_objects     <- selectFS and [inService "Code"]
    forum_objects    <- selectFS and [inService "Forum"]
    abgabe_objects   <- selectFS and [inService "Abgabe"]
    register_objects <- selectFS and [inService "Register"]
    -- writeGroupedInstances code_objects forum_objects abgabe_objects register_objects
    writeInstancesSingleFile code_objects forum_objects abgabe_objects register_objects


main2 = do
    objects <-  selectFS and [inService "Register"]
    writeFile "test.csv" $ to_csv "" $ extractHEPStudentGroup $ selectRegistrations objects
