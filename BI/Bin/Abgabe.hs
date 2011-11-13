module Main where

import BI.Api
import qualified BI.Queries as Q

main :: IO()
main = do
    objects <- selectFS and [inService "Abgabe"]
    -- all instances
    writeFile "abgabe_courses.csv"
        $ to_csv "service,course_id,kurs,semester,description\n"
        $ Q.selectCourses objects

    -- TODO: the merging seems to be wrong. there are some person elements with empty group
    writeFile "abgabe_person_lecturers.csv"
        $ to_csv "service,course_id,group_id,person_id,name,email,type\n"
        $ Q.mergeAbgabePersonsLecturers objects

    writeFile "abgabe_assessment_results.csv"
        $ to_csv "service,course_id,user_id,result_id,result_value\n"
        $ Q.selectAssessmentResults objects

    writeFile "abgabe_assessment_pluses.csv"
        $ to_csv "service,course_id,user_id,plus_date\n"
        $ Q.selectAssessmentPlus objects

    writeFile "abgabe_feedback.csv"
        $ to_csv "service,course_id,user_id,task,subtask,author,comment_length\n"
        $ Q.selectFeedback objects

    writeFile "abgabe_assessment_pluses_courses.csv"
        $ to_csv "service,course_id,kurs,semester,description,user_id,plus_date\n"
        $ Q.mergeWithCourses (Q.selectAssessmentPlus objects) objects

    writeFile "abgabe_assessment_results_courses.csv"
        $ to_csv "service,course_id,kurs,semester,description,user_id,result_id,result_value\n"
        $ Q.mergeWithCourses (Q.selectAssessmentResults objects) objects

    writeFile "abgabe_feedback_courses.csv"
        $ to_csv "service,course_id,kurs,semester,description,user_id,task,subtask,author,comment_length\n"
        $ Q.mergeWithCourses (Q.selectFeedback objects) objects

    -- tasks.xml
    -- uploaded files
