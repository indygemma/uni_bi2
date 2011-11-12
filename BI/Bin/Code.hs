module Main where

import BI.Api
import qualified BI.Types as T
import qualified BI.Queries as Q
import Data.List

-- IMPORTANT: there are two person ids that do not uccur in persons.xml: a710000, a712675, but do occur in
--            resUnit.xml. Note that these are not valid matrikel numbers anyway. Their groups are also 0.
--            They only ones with that group.
--            The other way: those that occur in persons.xml but not in resUnit.xml means those people not
--            running unittests: lecturer, tutors and dropped out students
comparePersonIds :: [T.Object] -> FilePath -> IO()
comparePersonIds objects filename = do
    -- compares which ids occur in unittests results but are omitted in persons.xml
    let person_objects = unique $ extract [exAttr "id"] $ select and [hasTag "person", notInPath "topics.xml"] objects
    let unittest_persons = unique $ extract [exAttr "identifier"] $ select and [hasTag "test", inPath "resUnit.xml"] objects
    writeFile filename $ show (person_objects \\ unittest_persons)

main :: IO()
main = do
    objects <- selectFS and [inService "Code"]

    -- | all states. returns the name of all states available from the unittest runs.
    {-writeFile "code_states.csv"-}
        {-$ unlines-}
        {-$ unique-}
        {-$ concat-}
        {-$ extractAttrs ["state"]-}
        {-$ select and [hasTag "case"] objects-}

    -- | all instances
    {-writeFile "code_courses.csv"-}
        {-$ to_csv "service,course_id,description\n"-}
        {-$ unique-}
        {-$ extract [exService, exAttr "id", exText]-}
        {-$ select and [hasTag "instance"] objects-}

    -- | all topics
    {-writeFile "code_topics.csv"-}
        {-$ to_csv "service,course_id,topic_id,topic\n"-}
        {-$ Q.selectTopics objects-}

    -- | all persons
    {-writeFile "code_persons.csv"-}
        {-$ to_csv "service,course_id,group_id,person_id,name,email\n"-}
        {-$ Q.selectCodePersons objects-}

    -- | all lecturers
    {-writeFile "code_lecturers.csv"-}
        {-$ to_csv "service,course_id,group_id,person_id,type\n"-}
        {-$ Q.selectLecturers objects-}

    -- | all topic person assignments
    {-writeFile "code_topic_person_assignments.csv"-}
        {-$ to_csv "service,course_id,group_id,person_id,topic_id\n"-}
        {-$ Q.selectTopicAssignments objects-}

    -- | all code unittest results
    writeFile "code_unittest_results.csv"
        $ to_csv "service,course_id,group_id,person_id,topic_id,timestamp,year,month,day,hour,min,sec,success,warning,failure,error,info,timeout\n"
        $ Q.selectUnittestResults objects

    -- | all person lecturers
    {-writeFile "code_person_lecturers.csv"-}
        {-$ to_csv "service,course_id,group_id,person_id,name,email,type\n"-}
        {-$ Q.mergeCodePersonsLecturers objects-}

    -- | all person topics
    writeFile "code_person_lecturers_topicids.csv"
        $ to_csv "service,course_id,group_id,person_id,name,email,type,topic_id\n"
        $ Q.mergeTopicIds objects

    -- | all fact table
    writeFile "code_fact_table.csv"
        $ to_csv "service,course_id,group_id,person_id,name,email,type,topic_id,timestamp,year,month,day,hour,min,sec,SUCCESS,WARNING,FAILURE,ERROR,INFO,TIMEOUT\n"
        $ Q.mergeUnittestResults objects

    {-comparePersonIds objects "code_person_in_unittests_not_in_persons_xml.csv"-}
