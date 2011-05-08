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
    x
    where newAttrMap  = attrMap
          parentID    = case Map.lookup "id" attrMap of
                          Just x -> x
                          Nothing -> ""
          -- need the children of element <sub>
          {-newChildren = map (injectParentID parentID) $ filter (\x -> (oTag x) == tag) -}
    {-where z = toZipper x-}
          {-Just g1 = down z-}
          {-children = getHole g1 :: Maybe [Object]-}

{-injectParentID parentID x@(Object service path tag theText ttype attrMap attrTMap children) =-}
    {-Nothing-}

-- | upStringLength calculates the length

-- | Select statements | --

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

-- NOTE: user voonly. only occurs in some forum entries but nowhere in
--       persons.xml. who is this?
selectForumEntries objects =
    extract [exService,
             exAttr "nid",
             exAttr "id",
             exAttr "text_length",
             exAttr "subject_length"]
    {-extract [exService,-}
             {-exAttr "course_id",-}
             {-exAttr "user",-}
             {-exAttr "name",-}
             {-exAttr "nid",-}
             {-exAttr "id",-}
             {-exAttr "parent_id",-}
             {-exAttr "date",-}
             {-exAttr "subject_length",-}
             {-exAttr "text_length"]-}
    $ update [
        upLength "subject" "subject_length",
        upLength "text" "text_length",
        pullUp (\children -> concat $ extract [exText] $ select and [hasTag "subject"] children) "subject",
        pullUp (\children -> concat $ extract [exText] $ select and [hasTag "text"] children) "text",
        upParentID "parent_id"]
    $ liftChildren and [hasTag "entry"]
    $ update [pushDown "nid" "nid",
              upCourseId "course_id"]
    $ select and [hasTag "entries"] objects

-- | Merge Statements | --

-- | CSV writing Functions | --

allInstances objects filename = do
    writeFile filename
        $ to_csv "service,course_id,description\n"
        $ unique
        $ extract [exService, exAttr "id", exText]
        $ select and [hasTag "instance"] objects

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
        $ to_csv "whatever\n"
        $ selectForumEntries objects

main = do
    objects <- selectFS and [inService "Forum"]
    allInstances              objects "forum_courses.csv"
    allPersons                objects "forum_persons.csv"
    allIssues                 objects "forum_issues.csv"
    allForumEntries           objects "forum_entries.csv"
