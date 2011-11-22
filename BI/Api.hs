module BI.Api where

import BI.Types
import BI.Binary
import BI.Common
import BI.Directory
import Data.Binary
import Data.List
import Data.Maybe
import Control.Parallel.Strategies
import Control.Parallel
import Codec.Compression.GZip
import qualified Data.Map as Map
import qualified Data.ByteString.Lazy as L
import System.IO.Unsafe

{-- Data Exploration API / DSL for Objects --}

fetchOneLevel :: (Object -> Bool) -> Object -> [Object]
fetchOneLevel f x = case f x of
    True -> [x]
    False -> []

fetch :: (Object -> Bool) -> Object -> [Object]
fetch f x = case f x of
    True -> [x] ++ (sselect f $ oChildren x)
    False -> sselect f $ oChildren x

sselectOneLevel :: (Object -> Bool) -> [Object] -> [Object]
sselectOneLevel f xs = concat $ [concat $ map (fetchOneLevel f) (oChildren x) | x <- xs]

sselect :: (Object -> Bool) -> [Object] -> [Object]
sselect f xs = concat $ map (fetch f) xs

applyFuncs :: [(Object -> Bool)] -> Object -> [Bool]
applyFuncs fs y = map (\x -> x y) fs

sselectParent :: (Object -> Bool) -> [Object] -> [Object]
sselectParent f xs = concat $ map (fetchOneLevel f) xs

-- TODO: somehow this returns duplicate entries, remove them...maybe we're traversing an element as child once and later as lone-standing element the second time?
{-select :: ([Bool] -> Bool) -> [(Object -> Bool)] -> [Object] -> [Object]-}
select cond fs = nub . sselect (\x -> cond $ applyFuncs fs x)
{-select cond fs = sselectOneLevel (\x -> cond $ applyFuncs fs x)-}

-- | Lifts up those child objects that match the filters
liftChildren :: ([Bool] -> Bool) -> [(Object -> Bool)] -> [Object] -> [Object]
liftChildren cond fs = sselectOneLevel (\x -> cond $ applyFuncs fs x)

selectAll fs = select and fs
selectOr fs  = select or fs

-- pre-defined predicates
hasTag tag x = oTag x == tag
hasAttr attr x = Map.member attr $ oAttributeMap x
inService service x = oService x == service
inPath path x = isInfixOf path $ oPath x
notInPath path x = not $ inPath path x

attr cmp a v x = maybe False (cmp v) $ Map.lookup a $ oAttributeMap x

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

extractAttr :: [String] -> Object -> [String]
extractAttr xs x = concat $ mapM (\a -> maybeToList $ Map.lookup a $ oAttributeMap x) xs

extractAttrs :: [String] -> [Object] -> [[String]]
extractAttrs a xs = map (extractAttr a) xs

-- | Either extract with specific functions, or if a string
--   is supplied, extract the attributes
exService obj = oService obj

exTag :: Object -> String
exTag = oTag

exPath :: Object -> String
exPath = oPath

exText :: Object -> String
exText obj = maybe "" id $ oText obj

exAttr :: String -> Object -> String
exAttr a obj = exAttrDefault a "" obj

exAttrDefault a defaultValue obj = maybe defaultValue id $ Map.lookup a $ oAttributeMap obj

performExtract :: [Object -> String] -> Object -> [String]
performExtract xs obj = map (\f -> f obj ) xs

extract :: [Object -> String] -> [Object] -> [[String]]
extract xs = map (performExtract xs)

-- | upLength takes the value of the first parameter, calculates its length
--   and stores the result into the table with the key given as the second
--   parameter.
upLength :: String -> String -> Object -> Object
upLength a1 a2 (Object service path tag theText ttype attrMap attrTMap children) =
    Object service path tag theText ttype updatedMap attrTMap children
    where oldValue = maybe "" id $ Map.lookup a1 attrMap
          newValue = length oldValue
          updatedMap = Map.insert a2 (show newValue) attrMap

performUpdate :: [Object -> Object] -> Object -> Object
performUpdate xs obj = foldr (\f y -> f y) obj xs

update :: [Object -> Object] -> [Object] -> [Object]
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
    where value = maybe "" id $ Map.lookup a1 attrMap
          updatedChildren = map (performPushDown a2 value) children

pullUp f attribute x@(Object service path tag theText ttype attrMap attrTMap children) =
    Object service path tag theText ttype updatedMap attrTMap children
    where updatedMap = Map.insert attribute (result!!0) attrMap
          result     = f x
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
    return (concat $ (objects `using` (parBuffer 4 rseq)))
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
    return . concat =<< unsafeInterleaveMapIO (parseFile selectfs) =<< getFilesWithExt root ["raw"]

to_csv header a = header ++ (unlines $ map (intercalate ";") a)

exT1 = [["code", "1", "a0607688", "Conrad Indiono", "indygemma@gmail.com"],
        ["code", "2", "person005", "asdlaskjd", "lkajsdlj@univie.ac.at"]]
exT2 = [["code", "2", "person005", "lecturer"],
        ["code", "2", "person005", "tutor"],
        ["code", "3", "person004", "lecturer"]]
exT3 = [["Code","1","","person001","Conan der Barbar","conan.der.barbar@univie.ac.at"],
        ["Code","3","","person001","Conan der Barbar","conan.der.Barbar@univie.ac.at"]]
exT4 = [["Code","1","1","person001","tutor"],
        ["Code","1","1","person002","lecturer"],
        ["Code","3","1","person001","tutor"]]

joinExample = leftJoin [0,1,3] exT3 exT4 [(6, \key -> "student")]

fetchIndex :: Int -> [String] -> String
fetchIndex idx ls = ls !! idx

fetchIndices :: [Int] -> [String] -> [String]
fetchIndices ids ls = map (\idx -> fetchIndex idx ls) ids

fetchIndicesInv :: [Int] -> [String] -> [String]
fetchIndicesInv ids ls = fetchIndices ([0..length ls - 1] \\ ids) ls

sameKey :: [String] -> [Int] -> [String] -> Bool
sameKey key ids ls = key == fetchIndices ids ls

mappifyIndex :: [Int] -> [[String]] -> Map.Map [String] [[String]]
mappifyIndex ids ls =
    mappifyCollect ids ls startMap
    where startMap = Map.fromList keys
          keys     = [(key, []) | key <- map (fetchIndices ids) ls]

mappifyCollect :: [Int] -> [[String]] -> Map.Map [String] [[String]] -> Map.Map [String] [[String]]
mappifyCollect ids []     theMap = theMap
mappifyCollect ids (x:xs) theMap =
    mappifyCollect ids xs newMap
    where key      = fetchIndices ids x
          curValue = maybe [] id $ Map.lookup key theMap
          newValue = fetchIndicesInv ids x : curValue
          newMap   = if sameKey key ids x then Map.insert key newValue theMap else theMap

leftJoin :: [Int] -> [[String]] -> [[String]] -> [(Int, [String] -> String)] -> [[String]]
leftJoin ids t1 t2 defaultValues =
    leftJoinCollect ids t1 defaultMap indexMap addedRowLength
    where indexMap       = mappifyIndex ids t2
          addedRowLength = length (head t2) - length ids
          defaultMap     = Map.fromList defaultValues

leftJoinCollect :: [Int] -> [[String]] -> Map.Map Int ([String] -> String) -> Map.Map [String] [[String]] -> Int -> [[String]]
leftJoinCollect ids []     defaultMap indexMap addedRowLength = []
leftJoinCollect ids (x:xs) defaultMap indexMap addedRowLength =
    case Map.lookup key indexMap of
        Just values ->
            -- crossproduct with all entries in the indexMap
            -- TODO: ++ is slow
            let newitems = map (\v -> key ++ fetchIndicesInv ids x ++ v) values in
            newitems ++ leftJoinCollect ids xs defaultMap indexMap addedRowLength
        Nothing ->
            -- add the required number of columns and insert the default values if available
            let totalLength = length x + addedRowLength   in
            let flist       = drop (length x) (buildListFromMap defaultMap (\k -> "") totalLength) in
            let newrow = key ++ fetchIndicesInv ids x ++ (map (\f -> f key)) flist in
            newrow : leftJoinCollect ids xs defaultMap indexMap addedRowLength
    where key = fetchIndices ids x

objLeftJoin :: [(Object -> String, Object -> String)]
            -> [Object]
            -> [Object]
            -> [Object]
objLeftJoin mapping t1 [] = t1
objLeftJoin mapping t1 t2 = result
    where result = concat $ map (objLeftJoinCollect t2) t1
          -- do the two objects match anyway?
          compare :: Object -> Object -> Bool
          compare x y = all id $ map (\(f1,f2) -> (f1 x) == (f2 y)) mapping
          -- merge down everything from y to x
          update :: Object -> Object -> Object
          update x@(Object service1 path1 tag1 theText1 ttype1 attrMap1 attrTMap1 children1)
                 y@(Object service2 path2 tag2 theText2 ttype2 attrMap2 attrTMap2 children2) =
                 Object service1 path1 tag1 theText1 ttype1 combinedMap combinedAttrMap children1
                 where combinedMap     = Map.union attrMap1 attrMap2
                       combinedAttrMap = Map.union attrTMap1 attrTMap2
          -- merge the keys only from y to x, leave null as default values
          {-updateKeys :: Object -> Object -> Object-}
          {-updateKeys x y = x-}
          objLeftJoinCollect :: [Object] -> Object -> [Object]
          objLeftJoinCollect ys x = case filter (compare x) ys of
            []    -> [x]
            final -> map (update x) final

createObject attrs =
    Object "Code" "somePath" "a tag" (Just "Some text") (Just "int") (Map.fromList attrs) (Map.fromList []) []

samplePersons = [
    createObject [("P_id", "1"), ("LastName", "Hansen"),   ("FirstName", "Ola"), ("Address", "Timoteivn 10"), ("City", "Sandnes")],
    createObject [("P_id", "2"), ("LastName", "Svendson"), ("FirstName", "Tove"), ("Address", "Borgvn 23"), ("City", "Sandnes")],
    createObject [("P_id", "3"), ("LastName", "Pettersen"), ("FirstName", "Kari"), ("Address", "Storgt 20"), ("City", "Stavanger")]
    ]

sampleOrders = [
    createObject [("O_Id","1"), ("OrderNo", "77895"),  ("P_Id", "3")],
    createObject [("O_Id","2"), ("OrderNo", "44678"),  ("P_Id", "3")],
    createObject [("O_Id","3"), ("OrderNo", "22456"),  ("P_Id", "1")],
    createObject [("O_Id","4"), ("OrderNo", "24562"),  ("P_Id", "1")],
    createObject [("O_Id","5"), ("OrderNo", "347641"), ("P_Id", "5")]
    ]

samplePersonsOrdersJoin = extract [
        exAttr "P_id",
        exAttr "LastName",
        exAttr "FirstName",
        exAttr "Address",
        exAttr "City",
        exAttr "P_Id",
        exAttr "O_Id",
        exAttr "OrderNo"
    ] $
    objLeftJoin [(exAttr "P_id", exAttr "P_Id")] samplePersons sampleOrders

sampleMap :: Map.Map Int ([String] -> String)
sampleMap = Map.fromList [(5, \key -> "student"),(3, \key -> "lol")]

sampleList :: [([String] -> String)]
sampleList = buildListFromMap sampleMap (\key -> "") 10

buildListFromMap :: Map.Map Int ([String] -> String) -> ([String] -> String) -> Int -> [([String] -> String)]
buildListFromMap defaultMap f l = buildListFromMap_ defaultMap f 0 l

buildListFromMap_ :: Map.Map Int ([String] -> String) -> ([String] -> String) -> Int -> Int -> [([String] -> String)]
buildListFromMap_ defaultMap f lmin lmax
    | lmin == lmax = []
    | otherwise    = result : buildListFromMap_ defaultMap f (lmin+1) lmax
                     where result = maybe f id $ Map.lookup lmin defaultMap

{-Code,1,,person001,Conan der Barbar,conan.der.barbar@univie.ac.at-}
{-Code,1,person001,,Conan der Barbar,conan.der.barbar@univie.ac.at,1,tutor-}
{-Code,1,1,person001,tutor-}

{-Code,1,<group>,person001,Conan der Barbar,conan.der.barbar@univie.ac.at-}
{-Code,1,person001,<group>,Conan der Barbar,conan.der.barbar@univie.ac.at,<group>,tutor-}
{-Code,1,<group>,person001,tutor-}

