module BI.Api where

import BI.Types
import BI.Binary
import BI.Common
import Data.Binary
import Data.List
import Codec.Compression.GZip
import qualified Data.Map as Map
import qualified Data.ByteString.Lazy as L

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
