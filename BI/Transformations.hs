module BI.Transformations where

import BI.Api
import BI.Types
import Data.List
import qualified Data.Map as Map
import System.FilePath
import qualified Text.JSON as JSON

upPathIndex name idx (Object service path tag theText ttype attrMap attrTMap children) =
    -- do something with path and save that result in the dict
    (Object service path tag theText ttype newMap attrTMap children)
    where newMap  = Map.insert name result attrMap
          newPath = splitDirectories $ path \\ "/home/conrad/Downloads/data/" -- TODO: why is this path hard-coded?
          result  = newPath !! idx

-- "Code/{ course_id }/Data/{ group_id }/..."
upCourseId name obj = upPathIndex name 1 obj
upGroupId  name obj = upPathIndex name 3 obj
-- "Abgabe/10/Data/a0903586/1/1/xxxXXXxxxxXxx.pdf"
-- "Abgabe/10/Data/{matrikelnr}/{task_id}/{subtask_id}/{filename}"
upAbgabeMatrikelNr name obj = upPathIndex name 3 obj
upAbgabeTaskId     name obj = upPathIndex name 4 obj
upAbgabeSubTaskId  name obj = upPathIndex name 5 obj
upAbgabeFilename   name obj = upPathIndex name 6 obj

upKurs name x@(Object service path tag theText ttype attrMap attrTMap children) =
    Object service path tag theText ttype newMap attrTMap children
    where newMap = Map.insert name result attrMap
          result = case theText of
                       Just x -> map (\idx -> x !! idx) [43,44]
                       Nothing -> ""

upSemester name x@(Object service path tag theText ttype attrMap attrTMap children) =
    Object service path tag theText ttype newMap attrTMap children
    where newMap = Map.insert name result attrMap
          lookup x = map (\idx -> x !! idx)
          result = case theText of
                       Just x -> case lookup x [48,49] of
                                     "/s" -> lookup x [51,52] -- "/ue/se00" -> 00
                                     _    -> lookup x [48,49] -- "/se00" -> 00
                       Nothing -> ""

--
-- Code-specific update functions
--
upUnittestStates (Object service path tag theText ttype attrMap attrTMap children) =
    Object service path tag theText ttype newMap attrTMap children
    where newMap          = Map.union (Map.union attrMap stateMap) defaultStateMap
          defaultStateMap = Map.fromList [("ERROR",      "0"),
                                          ("FAILURE",    "0"),
                                          ("INFO",       "0"),
                                          ("PROCESSING", "0"),
                                          ("SUCCESS",    "0"),
                                          ("TIMEOUT",    "0"),
                                          ("WARNING",    "0")]
          stateMap = Map.fromList $ map (\l@(x:xs) -> (x, show $ length l))
                         $ group
                         $ sort $ concat
                         $ extractAttrs ["state"]
                         $ select and [hasTag "case"] children

--
-- Forum-specific Update code
--

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

-- These are commonly used for transformations, especially for HEP

-- | update an object by setting an attribute key with the specified value
upAttr key value (Object service path tag theText ttype attrMap attrTMap children) =
    Object service path tag theText ttype newMap attrTMap children
    where newMap = Map.insert key value attrMap

-- | update an object by copying attribute key value to another new key
upAttrValue key valueKey (Object service path tag theText ttype attrMap attrTMap children) =
    Object service path tag theText ttype newMap attrTMap children
    where newMap = Map.insert key otherValue attrMap
          otherValue = case Map.lookup valueKey attrMap of
                         Just x -> x
                         Nothing -> ""

-- | update an object by copying the value looked up via 'op' under a new key
upAttrLookup key op x@(Object service path tag theText ttype attrMap attrTMap children) =
    Object service path tag theText ttype newMap attrTMap children
    where newMap = Map.insert key otherValue attrMap
          otherValue = op x

-- | update an object by creating a new attribute with the supplied attribute keys as JSON-encoded object
upJSON key keys x@(Object service path tag theText ttype attrMap attrTMap children) =
    Object service path tag theText ttype newMap attrTMap children
    where newMap = Map.insert key value attrMap
          value  = JSON.encode $ JSON.toJSObject values
          values = map (\key -> (key, exAttr key x)) keys

