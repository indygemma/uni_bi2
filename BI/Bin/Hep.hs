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

-- DONE: Refactor out the most common logic
-- TODO: write out extra information as JSON on a single line in final CSV
-- TODO: write csv files in the following format: matrikelnummer_courseid_semester.csv
--       --> Find out if the course_id are always equal in all occurences
--       --> Need Update function to extract current semester.
-- TODO: write mapping of the defined process actions to the data here
-- TODO: find out where to commonly extract semester
-- TODO: how to track if a person has uploaded?



-- selectCourses schema: [service, course_id, kurs, semester, description]
-- selectCourses data: [["Abgabe", "10", "02", "04", "description"],...]
-- selectUnitestResults schema: [service, course_id, group_id, identifier, theme, timestamp, year, month, day, 
-- selectAssessmentPlus schema: [service, course_id, user_id, date]
-- selectAssessmentResults schema: [service, course_id, user_id, id, text]
-- selectFeedback schema: [service, course_id, user_id, task, subtask, author, comment_length]

-- Code Stuff

selectUnittestResults objects = extract [
            exAttr "matrikelnr",
            exAttr "kurs",
            exAttr "semester",
            exAttr "event"
        ]
        {--- TODO: convert extra data to JSON and write out in single line-}
        $ update [T.upAttr "event" "code unittest run",
                  T.upAttrValue "matrikelnr" "identifier"]
        $ mergeWithCourses Q.objUnittestResults objects

-- Forum stuff

selectForumEntries objects = extract [
        exAttr "matrikelnr",
        exAttr "kurs",
        exAttr "semester",
        exAttr "event"
    ]
    {--- TODO: convert extra data to JSON and write out in single line-}
    $ update [T.upAttr "event" "forum entry",
              T.upAttrValue "matrikelnr" "user"]
    $ mergeWithCourses Q.objForumEntries objects

-- Abgabe stuff

mergeWithCourses otherObjects objects =
    objLeftJoin [(exService, exService),
                 (exAttr "id", exAttr "course_id")] -- "id" in course data, "course_id" in others
                (Q.objCourses objects)
                (otherObjects objects)

selectAssessmentResultsCourses objects = extract [
        exAttr "matrikelnr",
        exAttr "kurs",
        exAttr "semester",
        exAttr "event"
    ]
    {--- TODO: convert extra data to JSON and write out in single line-}
    $ update [T.upAttr "event" "assessment result",
              T.upAttrValue "matrikelnr" "user_id"]
    $ mergeWithCourses Q.objAssessmentResults objects

selectAssessmentPlusCourses objects = extract [
        exAttr "matrikelnr",
        exAttr "kurs",
        exAttr "semester",
        exAttr "event"
    ]
    {--- TODO: convert extra data to JSON and write out in single line-}
    $ update [T.upAttr "event" "plus",
              T.upAttrValue "matrikelnr" "user_id"]
    $ mergeWithCourses Q.objAssessmentPlus objects

selectFeedbackCourses objects = extract [
        exAttr "matrikelnr",
        exAttr "kurs",
        exAttr "semester",
        exAttr "event"
    ]
    {--- TODO: convert extra data to JSON and write out in single line-}
    $ update [T.upAttr "event" "feedback",
              T.upAttrValue "matrikelnr" "user_id"]
    $ mergeWithCourses Q.objFeedback objects

groupAll code_objects forum_objects abgabe_objects = grouped
    where forum           = selectForumEntries forum_objects
          code            = selectUnittestResults code_objects
          abgabe_pluses   = selectAssessmentPlusCourses abgabe_objects
          abgabe_results  = selectAssessmentResultsCourses abgabe_objects
          abgabe_feedback = selectFeedbackCourses abgabe_objects
          {-combined     = forum ++ code ++ abgabe_results-}
          combined     =  --forum 
                       {-++ code-}
                       code
                       {-++ abgabe_pluses-}
                       {-++ abgabe_results-}
                       {-++ abgabe_feedback-}
          studentsOnly = filter (\x -> isPrefixOf "a" (x!!0))
          fields x y   = all id [(x!!0) == (y!!0),
                                 (x!!1) == (y!!1),
                                 (x!!2) == (y!!2)]
          grouped      = groupBy fields $ sort $ studentsOnly $ combined
          {-grouped      = groupBy fields $ sort $ combined-}

main = do
    code_objects  <- selectFS and [inService "Code"]
    forum_objects <- selectFS and [inService "Forum"]
    abgabe_objects <- selectFS and [inService "Abgabe"]
    mapM (\group -> do
        let row       = group !! 0
        -- setup pre_name matrikelnummer=0, kurs=1, semester=2
        let pre_name  = intercalate "_" [row!!0, row!!1, row!!2]
        let filename  = (intercalate "." [pre_name, "csv"])
        let fullpath  = joinPath ["hep", filename]
        let content   = to_csv "" group
        writeFile fullpath content
        return ()
        ) $ groupAll code_objects forum_objects abgabe_objects
