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

-- | Select statements | --

selectPersons objects = unique
        $ extract [exService,
                   exAttr "course_id",
                   exAttr "group",
                   exAttr "id",
                   exAttr "name",
                   exAttr "email"]
        $ update [T.upCourseId "course_id"]
        $ select and [hasTag "person"] objects

-- compare person ids occuring in forum entries vs those that occur in persons.xml
comparePersonIds objects filename = do
    let person_objects = unique $ extract [exAttr "id"] $ select and [hasTag "person", inPath "persons.xml"] objects
    let forum_persons = unique $ extract [exAttr "user"] $ select and [hasTag "entry"] $ select and [hasTag "entries"] objects
    writeFile filename $ show $ concat (forum_persons \\ person_objects)

main :: IO()
main = do
    objects <- selectFS and [inService "Forum"]
    -- | all Instances
    writeFile "forum_courses.csv"
        $ to_csv "service,course_id,description\n"
        $ Q.selectCourses objects

    -- | all Persons
    writeFile "forum_persons.csv"
        $ to_csv "service,course_id,group_id,person_id,name,email\n"
        $ Q.selectPersons objects

    -- | all Issues
    writeFile "forum_issues.csv"
        $ to_csv "service,course_id,issue_id,what\n"
        $ Q.selectIssues objects

    -- | all Forum Entries
    writeFile "forum_entries.csv"
        $ to_csv "service,course_id,user,name,nid,id,parent_id,date,subject_length,text_length\n"
        $ Q.selectForumEntries objects

    -- | all forum 99 entries
    writeFile "forum_99_entries.csv"
        $ to_csv "service,course_id,user,nid,id,subject_length,text_length\n"
        $ Q.selectForum99Entries objects

    -- | all code service in forum
    writeFile "forum_unique_users.csv"
        $ to_csv "course_id,username\n"
        $ Q.selectCodeServiceUsersInForum objects

    -- | all forum entries courses
    writeFile "forum_entries_courses.csv"
        $ to_csv "service,course_id,kurs,semester,description,user,name,nid,id,parent_id,date,subject_length,text_length\n"
        $ Q.mergeWithCourses (Q.selectForumEntries objects) objects

    comparePersonIds objects "forum_compare_userids"
