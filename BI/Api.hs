module BI.Api where

import BI.Types
import BI.Binary
import BI.Common
import Data.Binary
import Codec.Compression.GZip
import qualified Data.Map as Map
import qualified Data.ByteString.Lazy as L

{-- Data Exploration API / DSL for Objects --}

fetch :: (Object -> Bool) -> Object -> [Object]
fetch f x = case f x of
    True -> [x] ++ (select f $ oChildren x)
    False -> select f $ oChildren x

select :: (Object -> Bool) -> [Object] -> [Object]
select f xs = concat $ map (fetch f) xs

applyFuncs :: [(Object -> Bool)] -> Object -> [Bool]
applyFuncs fs y = map (\x -> x y) fs

selectWith :: ([Bool] -> Bool) -> [(Object -> Bool)] -> [Object] -> [Object]
selectWith cond fs = select (\x -> cond $ applyFuncs fs x)

selectAll fs = selectWith and fs
selectOr fs = selectWith or fs

-- pre-defined predicates
hasTag tag x = oTag x == tag
hasAttr attr x = Map.member attr $ oAttributeMap x

attrEq attr value x = case Map.lookup attr $ oAttributeMap x of
    Just v -> v == value
    Nothing -> False

isPerson  = hasTag "person"
isPersons = hasTag "persons"
isIssues  = hasTag "issues"
isCustom  = hasTag "custom"
---
sample = do
    objects <- loadObjects "register_objects.raw"
    let person = select (hasTag "person") objects
    let bry = select (attrEq "email" "bry.zachanike@unet.univie.ac.at") objects
    return (True)

--- framework-specific functions
loadObjects filename = do
    content <- L.readFile filename
    return (decode (decompress content) :: [Object])

saveObjects objects filename = do
    L.writeFile filename $ compress $ encode objects
