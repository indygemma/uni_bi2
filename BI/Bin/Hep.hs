module Main where

import BI.Api
import BI.Types
import Data.List
import qualified Data.List.Split as S
import qualified BI.Queries as Q
import qualified BI.Transformations as T
import qualified Data.Map as Map
import System.FilePath
import System.Posix.Files
import System.Time
import Text.Printf

-- TODO: wrap JSON column around with single quotes

-- DONE: there are too many duplicates after join, why?
-- DONE: modify the csv structure: timestamp, instance, user_id, user type, event, extra
-- TODO: write transformation for timestamps in all events. left: forum date (have to append Timezone)
-- DONE: modify activity label according to new PDF
-- DONE: write mapping of the defined process actions to the data here (are all events covered)
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
-- DONE: how to differentiate between exercise upload and milestone upload?

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
        ("02", "01"), -- DBS
        ("02", "03"), -- DBS
        ("02", "04"), -- DBS
        ("00", "00") -- AlgoDat
    ]

extractHEPStudentGroup = extract [
        exAttr "matrikelnr",
        exAttr "kurs",
        exAttr "semester",
        exAttr "iso_datetime",
        exAttr "matrikelnr",
        exAttr "person_id",
        exAttr "person_type",
        exAttr "event",
        exAttr "extra"
    ]

extractHEPCSGroup = extract [
        exAttr "kurs",
        exAttr "semester",
        exAttr "matrikelnr",
        exAttr "iso_datetime",
        exAttr "matrikelnr",
        exAttr "person_id",
        exAttr "person_type",
        exAttr "event",
        exAttr "extra"
    ]

upRegistration key x@(Object service path tag theText ttype attrMap attrTMap children) =
    Object service path tag theText ttype newMap attrTMap children
    where newMap = Map.insert key otherValue attrMap
          otherValue = case exAttr "course_id" x of
            "5" -> "Enroll Slot - Tutorium - DBS"
            "6" -> "Enroll Slot - Milestones - DBS"
            "7" -> "Enroll Slot - Milestones - DBS"
            "8" -> "Enroll Slot - Milestones - DBS"
            "15" -> "Enroll Slot - Code - AlgoDat"
            "20" -> "Enroll Slot - Milestones - DBS"
            "21" -> "Enroll Slot - Milestones - DBS"
            "22" -> "Enroll Slot - Milestones - DBS"

-- Register Stuff
-- DONE: there's no specific time when a student enrolled, just use begin-reg
-- 2011-01-22T18:23:14;Registration/20;Enroll;{"slot": 11, "unit":, "time": "11:30-11:45"}
selectRegistrations objects =
    update [
        T.upAttrValue "iso_datetime" "begin-reg",
        upRegistration "event",
        T.upJSON "extra" [
            "group_id", "title", "kurs", "semester", "slot_id", "units",
            "slot_info", "course_id", "service", "unit", "student"
        ],
        T.upAttrValue "person_id" "matrikelnr",
        T.upAttr      "person_type" "student"
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

upUnittestEvent key x@(Object service path tag theText ttype attrMap attrTMap children) =
    Object service path tag theText ttype newMap attrTMap children
    where newMap = Map.insert key event attrMap
          event = case exAttr "course_id" x of
            "1"  -> "Upload code phase 1"
            "3"  -> "Upload code phase 2"
            "5"  -> "Upload code phase 3"

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
        $ update [
            upUnittestEvent "event",
            T.upAttrValue   "matrikelnr"  "identifier",
            T.upAttr        "person_type" "student",
            T.upAttrValue   "person_id"   "identifier"
        ]
        $ hepCourses
        $ mergeWithCourses objects
        $ Q.objUnittestResults objects

selectPerformanceTestResults objects =
    update [T.upJSON "extra" [
        "matrikelnr",
        "kurs",
        "semester",
        "event",
        "service",
        "course_id",
        "group_id",
        "timestamp", -- TODO: need proper upload date
        "iso_datetime",
        "theme",
        "SUCCESS",
        "WARNING",
        "FAILURE",
        "ERROR",
        "INFO",
        "TIMEOUT"
    ]]
    $ update [
        -- TODO: proper event name
        T.upAttr "event" "Performance run",
        T.upAttrValue "matrikelnr" "identifier",
        T.upAttr "person_type" "student",
        T.upAttrValue "person_id" "identifier"
    ]
    $ hepCourses
    $ mergeWithCourses objects
    $ Q.objPerformancetestResults objects

-- Forum stuff
upForumEvent key x@(Object service path tag theText ttype attrMap attrTMap children) =
    Object service path tag theText ttype newMap attrTMap children
    where newMap = Map.insert key event attrMap
          event = case (exAttr "course_id" x, exAttr "nid" x) of
            ("26","81")   -> "Lecture Forum: ask question"
            ("26","323")  -> "Exercise Forum: ask question"
            ("26","158")  -> "Milestone Forum: ask question"
            ("73","110")  -> "Lecture Forum: ask question"
            ("73","231")  -> "Exercise Forum: ask question"
            ("73","224")  -> "Milestone Forum: ask question"
            ("73","53")   -> "Tutorial Forum: ask question"
            ("113","56")  -> "Lecture Forum: ask question"
            ("113","136") -> "Exercise Forum: ask question"
            ("113","77")  -> "Milestone Forum: ask question"
            ("113","31")  -> "Tutorial Forum: ask question"
            ("99","20")   -> "General Forum: ask question"
            ("99","201")  -> "Programming Forum: ask question"
            ("99","656")  -> "Project Forum: ask question"
            ("99","39")   -> "Lecture Forum: ask question"
            ("99","25")   -> "Misc. Forum: ask question"

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
    $ update [
          T.upAttrValue "person_id" "user",
          T.upAttrValue "matrikelnr" "user",
          T.upAttr      "person_type" "student",
          T.upAttrValue "iso_datetime" "date",
          upForumEvent "event"
    ]
    $ hepCourses
    $ mergeWithCourses objects
    $ Q.objForumEntries objects

-- Abgabe stuff

mergeWithCourses objects otherObjects =
    objLeftJoin [(exService, exService),
                 (exAttr "id", exAttr "course_id")] -- "id" in course data, "course_id" in others
                (Q.objCourses objects)
                otherObjects

upAssessmentResultTime key x@(Object service path tag theText ttype attrMap attrTMap children) =
    Object service path tag theText ttype newMap attrTMap children
    where newMap = Map.insert key otherValue attrMap
          otherValue = case length descValue == length "YY.MM.DD" of
            True  -> toDatetime descValue
            False -> exAttr "to" x
          descValue = (!!0) $ S.splitOn " " $ exAttr "test_desc" x
          -- Assumes input in "DD.MM.YY" format and returns an ISO Date in "YY-MM-DDT00:00:00"
          toDatetime :: String -> String
          toDatetime x = printf "20%s-%s-%sT00:00:00" year month day
            where year = s !! 2
                  month = s !! 1
                  day = s !! 0
                  s = S.splitOn "." x

upAssessmentResult key x@(Object service path tag theText ttype attrMap attrTMap children) =
    Object service path tag theText ttype newMap attrTMap children
    where newMap = Map.insert key event attrMap
          exerciseUpload = "Evaluate presentation " ++ (exAttr "task_id" x)
          milestoneUpload = "Evaluate milestone " ++ (exAttr "task_id" x)
          event = case exAttr "course_id" x of
            "1"  -> exerciseUpload
            "2"  -> milestoneUpload
            "9"  -> exerciseUpload
            "10" -> milestoneUpload
            "16" -> exerciseUpload
            "17" -> milestoneUpload

selectAssessmentResults objects =
    -- TODO: check all attributes that exist for these objects
    update [T.upJSON "extra" [
        "course_id",
        "user_id",
        "id",
        "score",
        "test_desc",
        "test_id",
        "result_id",
        "points",
        "group_id",
        "presence",
        "from",
        "to",
        "iso_datetime",
        "lecturer_id",
        "course_desc",
        "kurs",
        "semester"
    ]]
    $ update [upAssessmentResult "event",
              T.upAttrValue "matrikelnr" "user_id",
              upAssessmentResultTime "iso_datetime",
              T.upAttrValue "person_id" "lecturer_id",
              T.upAttr "person_type" "lecturer"] -- it's always a lecturer who gives points out to students
    $ hepCourses
    $ objAssessmentResultsWithCoursesPersonsTestsPresenceDateFirstLecturer objects

objAssessmentResultsWithCoursesPersonsTestsPresenceDateFirstLecturer objects =
    objLeftJoin [(exService, exService),
                 (exAttr "course_id", exAttr "course_id"),
                 (exAttr "group_id", exAttr "id")]
                 (objAssessmentResultsWithCoursesPersonsTestsPresenceDate objects)
                 (objAssessmentFirstLecturer objects)

objAssessmentResultsWithCoursesPersonsTestsPresenceDate objects =
    objLeftJoin [(exService, exService),
                   (exAttr "course_id", exAttr "course_id"),
                   (exAttr "group_id", exAttr "group_id"),
                   (exAttr "test_id", exAttr "task_id")]
                  (objAssessmentResultsWithCoursesPersonsTests objects)
                  (objAbgabeTasks objects)

objAssessmentResultsWithCoursesPersonsTests objects =
    objLeftJoin [(exService, exService),
                 (exAttr "course_id", exAttr "course_id"),
                 (exAttr "group", exAttr "group_id"),
                 (exAttr "result_id", exAttr "test_id")]
                 (objAssessmentResultsWithCoursesPersons objects)
                 (objAbgabeTest objects)

objAssessmentResultsWithCoursesPersons objects =
    (objLeftJoin [(exService, exService),
                  (exAttr "course_id", exAttr "course_id"),
                  (exAttr "user_id", exAttr "id")]
                  (objAssessmentResultsWithCourses objects)
                  (selectAbgabePersons objects))

objAssessmentResultsWithCourses objects =
    (objLeftJoin [(exService, exService),
                  (exAttr "course_id", exAttr "id")]
                 (Q.objAssessmentResults objects)
                 (Q.objCourses objects))

objAssessmentFirstLecturer objects =
    update [
        pullUp (\o -> return . exAttr "id" $ (!!0) $ oChildren o) "lecturer_id",
        T.upCourseId "course_id",
        T.upAttrLookup "service" exService,
        T.upAttrValue "course_desc" "desc"
    ]
    $ select and [hasTag "group", inService "Abgabe", inPath "course.xml"] objects

objAbgabeTest objects =
    update [
        T.upCourseId "course_id",
        T.upAttrLookup "service" exService,
        T.upAttrValue "test_id" "id",
        T.upAttrValue "test_desc" "desc"
    ]
    $ select and [hasTag "test"]
    $ liftChildren and [hasTag "test"]
    $ update [pushDown "id" "group_id"]
    $ select and [hasTag "group", inService "Abgabe", inPath "tasks.xml"] objects

upAssessmentPlusTime key x@(Object service path tag theText ttype attrMap attrTMap children) =
    Object service path tag theText ttype newMap attrTMap children
    where newMap = Map.insert key (otherValue++"T00:00:00") attrMap
          otherValue = exAttr "date" x

selectAssessmentPlus objects =
    -- TODO: implement iso_datetime for pluses!
    -- TODO: implement "extra" for pluses
    -- TODO: check all attributes that exist for these objects
    update [T.upJSON "extra" [
                "course_id",
                "user_id",
                "id",
                "date",
                "lecturer_id",
                "type",
                "group_id",
                "group",
                "iso_datetime",
                "kurs",
                "semester"
    ]]
    $ update [T.upAttr             "event" "plus",
              T.upAttrValue        "matrikelnr" "user_id",
              T.upAttrValue        "person_id" "lecturer_id",
              T.upAttr             "person_type" "lecturer",
              upAssessmentPlusTime "iso_datetime"]
    $ hepCourses
    $ objAssessmentPlusWithCoursesPersonsFirstLecturer objects

objAssessmentPlusWithCoursesPersonsFirstLecturer objects =
    objLeftJoin [(exService, exService),
                 (exAttr "course_id", exAttr "course_id"),
                 (exAttr "group", exAttr "id")]
                 (objAssessmentPlusWithCoursesPersons objects)
                 (objAssessmentFirstLecturer objects)

objAssessmentPlusWithCoursesPersons objects =
    objLeftJoin [(exService, exService),
                  (exAttr "course_id", exAttr "course_id"),
                  (exAttr "user_id", exAttr "id")]
                  (objAssessmentPlusWithCourses objects)
                  (selectAbgabePersons objects)

objAssessmentPlusWithCourses objects =
    objLeftJoin [(exService, exService),
                 (exAttr "course_id", exAttr "id")]
                 (Q.objAssessmentPlus objects)
                 (Q.objCourses objects)

upFeedback key x@(Object service path tag theText ttype attrMap attrTMap children) =
    Object service path tag theText ttype newMap attrTMap children
    where newMap = Map.insert key event attrMap
          exerciseUpload = "Exercise feedback " ++ (exAttr "task" x)
          milestoneUpload = "Milestone feedback " ++ (exAttr "task" x)
          event  = case exAttr "course_id" x of
            "1"  -> exerciseUpload
            "2"  -> milestoneUpload
            "9"  -> exerciseUpload
            "10" -> milestoneUpload
            "16" -> exerciseUpload
            "17" -> milestoneUpload

selectFeedbackCourses objects =
    -- TODO: implement iso_datetime for feedback!
    -- TODO: implement "extra" for feedback!
    -- TODO: check all attributes that exist for these objects
    {--- TODO: convert extra data to JSON and write out in single line-}
    update [T.upJSON "extra" [
        "matrikelnr", "user_id", "author", "type", "group", "group_id", "course_id", "kurs", "semester",
        "comment_length", "task", "subtask", "tag", "id", "presence", "from", "to"
    ]]
    $ hepCourses
    $ update [upFeedback   "event",
              T.upAttrValue  "matrikelnr"  "user_id",
              T.upAttrLookup "tag"         exTag,
              T.upAttrValue  "person_id"   "author",
              T.upAttrValue  "person_type" "type",
              T.upAttrValue  "iso_datetime" "to"]
    $ objFeedbackWithCoursesPersonsTypesPresenceDate objects

objFeedbackWithCoursesPersonsTypesPresenceDate objects =
      objLeftJoin [(exService, exService),
                   (exAttr "course_id", exAttr "course_id"),
                   (exAttr "group_id", exAttr "group_id"),
                   (exAttr "task", exAttr "task_id")]
                  (objFeedbackWithCoursesPersonsTypes objects)
                  (objAbgabeTasks objects)

objFeedbackWithCoursesPersonsTypes objects =
      objLeftJoin [(exService, exService),
                   (exAttr "course_id", exAttr "course_id"),
                   (exAttr "group", exAttr "group_id"),
                   (exAttr "author", exAttr "id")]
                  (objFeedbackWithCoursesPersons objects)
                  (selectAbgabeCourseGroups objects)

objFeedbackWithCoursesPersons objects =
      (objLeftJoin [(exService, exService),
                    (exAttr "course_id", exAttr "course_id"),
                    (exAttr "user_id", exAttr "id")]
                    (objFeedbackWithCourses objects)
                    (selectAbgabePersons objects))

objFeedbackWithCourses objects =
      (objLeftJoin [(exService, exService),
                    (exAttr "course_id", exAttr "id")]
                   (Q.objFeedback objects)
                   (Q.objCourses objects))

objAbgabeTasks objects =
    update [T.upCourseId "course_id",
            T.upAttrLookup "service" exService,
            T.upAttrValue "task_id" "id"]
    $ select and [hasTag "task"]
    $ liftChildren and [hasTag "task"]
    $ update [pushDown "id" "group_id"]
    $ select and [hasTag "group", inPath "tasks.xml", inService "Abgabe"] objects

selectAbgabeCourseGroups objects =
    update [
        T.upAttrLookup "type" exTag
    ]
    $ select or [hasTag "lecturer", hasTag "tutor"]
    $ liftChildren or [hasTag "lecturer", hasTag "tutor"]
    $ update [pushDown "course_id" "course_id",
              pushDown "service" "service",
              T.upCourseId "course_id",
              T.upAttrLookup "service" exService,
              pushDown "id" "group_id"]
    $ select and [hasTag "group", inPath "course.xml"] objects

-- this is the list of persons that exist in an Abgabe service per course_idbgabePersons q1 q2 objects =
--     objLeftJoin [(exAttr "user_id", exAttr "id")]
--                     (q1 objects)
--                     (q2 objects)
-- admin="true"
-- name="string"
-- id="person002" etc
-- email="string"
-- group="2"
-- team=""
selectAbgabePersons objects =
    update [T.upAttrLookup "service" exService,
            T.upCourseId "course_id"]
    $ select and [hasTag "person", inPath "persons.xml"] objects

-- DONE: have to differentiate between exercise and milestone!
upUploadEvent key x@(Object service path tag theText ttype attrMap attrTMap children) =
    Object service path tag theText ttype newMap attrTMap children
    where newMap = Map.insert key event attrMap
          exerciseUpload = "Upload exercise " ++ (exAttr "task_id" x)
          milestoneUpload = "Upload milestone " ++ (exAttr "task_id" x)
          event = case exAttr "course_id" x of
            "1"  -> exerciseUpload
            "2"  -> milestoneUpload
            "9"  -> exerciseUpload
            "10" -> milestoneUpload
            "16" -> exerciseUpload
            "17" -> milestoneUpload

selectAbgabeUploads objects =
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
    $ update [
        upUploadEvent "event",
        T.upAttr      "person_type" "student",
        T.upAttrValue "person_id"   "matrikelnr"
    ]
    $ hepCourses
    $ mergeWithCourses objects
    $ objUploadedFiles objects

-- DONE: extract all the other files: sql, zip, find out correct upload type (using course_id)
objUploadedFiles objects =
    update [T.upCourseId         "course_id" ,
            T.upAbgabeMatrikelNr "matrikelnr",
            T.upAbgabeTaskId     "task_id"   ,
            T.upAbgabeSubTaskId  "subtask_id",
            T.upAbgabeFilename   "filename"  ,
            T.upAttrLookup       "service" exService]
    $ select or [hasTag "pdf", inPath ".pdf",
                 hasTag "zip", inPath ".zip",
                 hasTag "sql", inPath ".sql"]
                 objects

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
          abgabe_pluses   = extraction $ selectAssessmentPlus abgabe_objects
          abgabe_results  = extraction $ selectAssessmentResults abgabe_objects
          abgabe_feedback = extraction $ selectFeedbackCourses abgabe_objects
          abgabe_uploads  = extraction $ selectAbgabeUploads abgabe_objects
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
        let content   = to_csv "timestamp;instance_id;person_id;person_type;event;data\n" $ sort $ map (\row -> [
                            row !! 3, -- timestamp
                            row !! 4, -- instance_id
                            row !! 5, -- person_id
                            row !! 6, -- person_type
                            row !! 7, -- event label
                            row !! 8  -- extra data
                        ]) group
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
        let content = to_csv "timestamp;instance_id;person_id;person_type;event;data\n" $ sort $ map (\row -> [
                            row !! 3, -- timestamp
                            row !! 4, -- instance_id
                            row !! 5, -- person_id
                            row !! 6, -- person_type
                            row !! 7, -- event label
                            row !! 8  -- extra data
                        ]) course
        {-let content = show $ course-}
        writeFile fullpath content
        return ()
    ) $ groupByCourseSemester code_objects forum_objects abgabe_objects register_objects

main2 = do
    code_objects     <- selectFS and [inService "Code"]
    forum_objects    <- selectFS and [inService "Forum"]
    abgabe_objects   <- selectFS and [inService "Abgabe"]
    register_objects <- selectFS and [inService "Register"]
    writeGroupedInstances code_objects forum_objects abgabe_objects register_objects
    writeInstancesSingleFile code_objects forum_objects abgabe_objects register_objects

main = do
    -- OK
    -- DONE: update event label
    {-objects <-  selectFS and [inService "Forum"]-}
    {-writeFile "test_forum.csv" $ to_csv "" $ extractHEPStudentGroup $ selectForumEntries objects-}

    -- OK
    -- DONE: update event label
    -- TODOLATER: some entries don't have timestamps, why? We can skip this for now by removing these lines...does not affect outcome
    {-objects <-  selectFS and [inService "Code"]-}
    {-writeFile "test_unittests.csv" $ to_csv "" $ extractHEPStudentGroup $ selectUnittestResults objects-}

    objects <-  selectFS and [inService "Code"]
    writeFile "test_performancetests.csv" $ to_csv "" $ extractHEPStudentGroup $ selectPerformanceTestResults objects

    -- OK
    -- DONE: update event label
    {-objects <-  selectFS and [inService "Abgabe"]-}
    {-writeFile "test_assplus.csv" $ to_csv "" $ extractHEPStudentGroup $ selectAssessmentPlus objects-}

    -- OK
    -- DONE: update event label
    {-objects <-  selectFS and [inService "Abgabe"]-}
    {-writeFile "test_assresults.csv" $ to_csv "" $ extractHEPStudentGroup $ selectAssessmentResults objects-}

    -- OK
    -- DONE: update event label
    -- DONE: iso_datetime still required
    {-objects <-  selectFS and [inService "Abgabe"]-}
    {-writeFile "test_feedback.csv" $ to_csv "" $ extractHEPStudentGroup $ selectFeedbackCourses objects-}

    -- OK
    -- TODO: update event label
    {-objects <-  selectFS and [inService "Abgabe"]-}
    {-writeFile "test_abgabeuploads.csv" $ to_csv "" $ extractHEPStudentGroup $ selectAbgabeUploads objects-}

    -- TODO: update event label
    -- NOT RELEVANT because outside HEP: not complete. sometimes no matrikelnr + no timestamp
    -- DONE: have to differentiate what kind of enrollment that is
    {-objects <-  selectFS and [inService "Register"]-}
    {-writeFile "test_registrations.csv" $ to_csv "" $ extractHEPStudentGroup $ selectRegistrations objects-}
    
    {-writeFile "test.csv" $ to_csv "" $ extractHEPStudentGroup $ selectAbgabeUploads objects-}
    {-writeFile "test.csv" $ show $ objAbgabeTasks objects-}
    {-writeFile "test.csv" $ show $ selectFeedbackCourses objects-}
    {-writeFile "test.csv" $ show $ selectAbgabeCourseGroups objects-}
