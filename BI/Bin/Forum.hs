module Main where

import BI.Api
import BI.Types
import Data.List
import qualified Data.Map as Map
import System.FilePath
import System.Posix.Files
import System.Time

-- | Update statements | --
--
upPathIndex name idx (Object service path tag theText ttype attrMap attrTMap children) =
    -- do something with path and save that result in the dict
    (Object service path tag theText ttype newMap attrTMap children)
    where newMap  = Map.insert name result attrMap
          newPath = splitDirectories $ path \\ "/home/conrad/Downloads/data/"
          result  = newPath !! idx

-- "Code/{ course_id }/Data/{ group_id }/..."
upCourseId name obj = upPathIndex name 1 obj
upGroupId  name obj = upPathIndex name 3 obj

-- | uses a zipper to update all child "entry" elements from the current object,
--   which lies under "sub" elements, with "parent_id" set as the "id" of the
--   current object
upParentID name x@(Object service path tag theText ttype attrMap attrTMap children) =
    Object service path tag theText ttype attrMap attrTMap newChildren
    where newAttrMap  = attrMap
          parentID    = case Map.lookup "id" attrMap of
                          Just x -> x
                          Nothing -> ""
          -- need the children of element <sub>
          newChildren = map (processSub name parentID) children

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
                       Just x -> map (\idx -> x !! idx) [48,49]
                       Nothing -> ""

-- | helper function that only processes "sub" elements, returns the original object
--   otherwise.
processSub :: String -> String -> Object -> Object
processSub name parentID x@(Object service path tag theText ttype attrMap attrTMap children)
    | (oTag x) == "sub" = Object service path tag theText ttype attrMap attrTMap newChildren
    | otherwise         = x
    where newChildren = map (injectParentID name parentID) children

injectParentID :: String -> String -> Object -> Object
injectParentID name parentID x@(Object service path tag theText ttype attrMap attrTMap children) =
    Object service path tag theText ttype updatedMap attrTMap updatedChildren
    where updatedMap = Map.insert name parentID attrMap
          updatedChildren = oChildren $ upParentID name x

-- | upStringLength calculates the length

-- | Select statements | --

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
        $ select and [hasTag "person"] objects

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
              upCourseId "course_id"]
    $ select and [hasTag "entry"]
    $ select and [hasTag "issues"] objects

-- compare person ids occuring in forum entries vs those that occur in persons.xml
comparePersonIds objects filename = do
    let person_objects = unique $ extract [exAttr "id"] $ select and [hasTag "person", inPath "persons.xml"] objects
    let forum_persons = unique $ extract [exAttr "user"] $ select and [hasTag "entry"] $ select and [hasTag "entries"] objects
    writeFile filename $ show $ concat (forum_persons \\ person_objects)

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
    $ update [
        upLength "subject" "subject_length",
        upLength "text" "text_length",
        pullUp (\o -> concat $ extract [exText] $ select and [hasTag "subject"] $ oChildren o) "subject",
        pullUp (\o -> concat $ extract [exText] $ select and [hasTag "text"] $ oChildren o) "text",
        upCourseId "course_id"]
    $ select and [hasTag "entry"]
    $ update [ upParentID "parent_id" ]
    $ liftChildren and [hasTag "entry"]
    $ update [pushDown "nid" "nid",
              upCourseId "course_id"]
    $ select and [hasTag "entries"] objects

selectForum99Entries objects =
    extract [exService,
             exAttr "course_id",
             exAttr "user",
             exAttr "nid",
             exAttr "id",
             exAttr "subject_length",
             exAttr "text_length"]
    $ update [
        upLength "subject" "subject_length",
        upLength "text" "text_length",
        pullUp (\o -> concat $ extract [exText] $ select and [hasTag "subject"] $ oChildren o) "subject",
        pullUp (\o -> concat $ extract [exText] $ select and [hasTag "text"] $ oChildren o) "text",
        upCourseId "course_id"]
    $ select and [hasTag "entry"]
    $ update [ upParentID "parent_id" ]
    $ select and [attrEq "course_id" "99"]
    $ update [pushDown "nid" "nid",
              upCourseId "course_id"]
    $ select and [hasTag "entries"] objects

selectCodeServiceUsersInForum objects = unique
    $ extract [exAttr "course_id", exAttr "user"]
    $ update [
        upLength "subject" "subject_length",
        upLength "text" "text_length",
        pullUp (\o -> concat $ extract [exText] $ select and [hasTag "subject"] $ oChildren o) "subject",
        pullUp (\o -> concat $ extract [exText] $ select and [hasTag "text"] $ oChildren o) "text",
        upCourseId "course_id"]
    $ select and [hasTag "entry"]
    $ update [ upParentID "parent_id" ]
    $ liftChildren and [hasTag "entry"]
    $ update [pushDown "nid" "nid",
              upCourseId "course_id"]
    $ select and [hasTag "entries"] objects

-- | Merge Statements | --

mergeWithCourses otherSelect objects = leftJoin [0,1] (selectCourses objects) otherSelect []

-- | CSV writing Functions | --

allInstances objects filename = do
    writeFile filename
        $ to_csv "service,course_id,description\n"
        $ selectCourses objects

allPersons objects filename = do
    writeFile filename
        $ to_csv "service,course_id,group_id,person_id,name,email\n"
        $ selectPersons objects

allIssues objects filename = do
    writeFile filename
        $ to_csv "service,course_id,issue_id,what\n"
        $ selectIssues objects

allForumEntries objects filename = do
    writeFile filename
        $ to_csv "service,course_id,user,name,nid,id,parent_id,date,subject_length,text_length\n"
        $ selectForumEntries objects

allForum99Entries objects filename = do
    writeFile filename
        $ to_csv "service,course_id,user,nid,id,subject_length,text_length\n"
        $ selectForum99Entries objects

allCodeServiceInForum objects filename = do
    writeFile filename
        $ to_csv "course_id,username\n"
        $ selectCodeServiceUsersInForum objects

allForumEntriesCourses objects filename = do
    writeFile filename
        $ to_csv "service,course_id,kurs,semester,description,user,name,nid,id,parent_id,date,subject_length,text_length\n"
        $ mergeWithCourses (selectForumEntries objects) objects

main = do
    objects <- selectFS and [inService "Forum"]
    allInstances              objects "forum_courses.csv"
    allPersons                objects "forum_persons.csv"
    allIssues                 objects "forum_issues.csv"
    allForumEntries           objects "forum_entries.csv"
    allForum99Entries         objects "forum_99_entries.csv"
    allCodeServiceInForum     objects "forum_unique_users.csv"
    comparePersonIds          objects "forum_compare_userids"
    allForumEntriesCourses    objects "forum_entries_courses.csv"
