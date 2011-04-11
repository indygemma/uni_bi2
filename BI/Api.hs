module BI.Api where

import BI.Types
import BI.Binary
import BI.Common
import BI.Directory
import Data.Binary
import Data.List
import Control.Parallel.Strategies
import Control.Parallel
import Codec.Compression.GZip
import qualified Data.Map as Map
import qualified Data.ByteString.Lazy as L
import System.IO.Unsafe

{-- Data Exploration API / DSL for Objects --}

fetch :: (Object -> Bool) -> Object -> [Object]
fetch f x = case f x of
    True -> [x] ++ (sselect f $ oChildren x)
    False -> sselect f $ oChildren x

sselect :: (Object -> Bool) -> [Object] -> [Object]
sselect f xs = concat $ map (fetch f) xs

applyFuncs :: [(Object -> Bool)] -> Object -> [Bool]
applyFuncs fs y = map (\x -> x y) fs

select :: ([Bool] -> Bool) -> [(Object -> Bool)] -> [Object] -> [Object]
select cond fs = sselect (\x -> cond $ applyFuncs fs x)

selectAll fs = select and fs
selectOr fs  = select or fs

-- pre-defined predicates
hasTag tag x = oTag x == tag
hasAttr attr x = Map.member attr $ oAttributeMap x
inService service x = oService x == service
inPath path x = isInfixOf path $ oPath x
notInPath path x = not $ inPath path x

attr cmp a v x = case Map.lookup a $ oAttributeMap x of
    Just val -> cmp v val
    Nothing  -> False

attrEq a v x = attr (==) a v x
attrGt a v x = attr (>)  a v x
attrLt a v x = attr (<)  a v x
attrContains a v x = attr (isInfixOf) a v x
attrStartsWith a v x = attr (isPrefixOf) a v x
attrEndsWith a v x = attr (isSuffixOf) a v x

isPerson  = hasTag "person"
isPersons = hasTag "persons"
isIssues  = hasTag "issues"
isCustom  = hasTag "custom"

unique x = map head $ (group . sort) x

extractAttr []     x = []
extractAttr (a:as) x = case Map.lookup a $ oAttributeMap x of
    Just v  -> v : extractAttr as x
    Nothing -> extractAttr as x

extractAttrs a xs = map (extractAttr a) xs

-- | Either extract with specific functions, or if a string
--   is supplied, extract the attributes
exService obj = oService obj

exTag = oTag

exPath = oPath

exText obj = case oText obj of
    Just x -> x
    Nothing -> ""

exAttr a obj = case Map.lookup a $ oAttributeMap obj of
    Just x -> x
    Nothing -> ""

performExtract xs obj = map (\f -> f obj ) xs

extract xs = map (performExtract xs)

performUpdate xs obj = foldr (\f y -> f y) obj xs

update xs = map (performUpdate xs)

updateM xs objs = mapM (\x -> return (performUpdate xs x)) objs

-- | Given an object, take the value of the given attribute
--   and "push it down" its children hierarchy by assigning
--   that value as the given key.
performPushDown key value (Object service path tag theText ttype attrMap attrTMap children) =
    Object service path tag theText ttype updatedMap attrTMap updatedChildren
    where updatedMap = Map.insert key value attrMap
          updatedChildren = map (performPushDown key value) children

pushDown a1 a2 (Object service path tag theText ttype attrMap attrTMap children) =
    Object service path tag theText ttype attrMap attrTMap updatedChildren
    where value = case Map.lookup a1 attrMap of
                      Just v -> v
                      Nothing -> ""
          updatedChildren = map (performPushDown a2 value) children
---
sample = do
    objects <- loadObjects "register_objects.raw"
    let person = sselect (hasTag "person") objects
    let bry = sselect (attr (==) "email" "bry.zachanike@unet.univie.ac.at") objects
    return (True)

--- framework-specific functions
loadObjects filename = do
    content <- L.readFile filename
    return (decode (decompress content) :: [Object])

saveObjects objects filename = do
    L.writeFile filename $ compress $ encode objects

-- | We can't hold the whole data structure in memory.
--   Apply the given selection to the objects loaded from
--   individual "raw" files.
selectFS cond fs = do
    objects <- mapM (\x -> selectFSService (select cond fs) (fst x) (snd x)) paths
    return (concat $ parBuffer 4 rwhnf objects)
    where paths = buildPaths "result/" ["Register", "Abgabe", "Forum", "Code"]

parseFile selectfs path = do
    objects <- loadObjects path
    return (selectfs objects)

-- | Force evaluation of a map in order to keep the number of file descriptors
--   low.
unsafeInterleaveMapIO f (x:xs) = unsafeInterleaveIO $ do
    putStrLn $ " Left: " ++ (show $ length xs) ++ " @" ++ x
    y  <- f x
    ys <- unsafeInterleaveMapIO f xs
    return (y : ys)
unsafeInterleaveMapIO _ [] = return []

selectFSService selectfs root output =
    return . concat =<< unsafeInterleaveMapIO (parseFile selectfs) =<< getFilesWithExt root "raw"

to_csv header a = header ++ (unlines $ map (intercalate ",") a)
