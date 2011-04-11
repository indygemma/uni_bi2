{-# LANGUAGE Arrows, NoMonomorphismRestriction #-}
-- Sources:
-- http://stackoverflow.com/questions/4619206/parse-xml-in-haskell
-- http://stackoverflow.com/questions/3899365/hxt-what-is-deep
-- http://stackoverflow.com/questions/3897283/haskell-hxt-for-extracting-a-list-of-values
-- http://stackoverflow.com/questions/3901492/running-haskell-hxt-outside-of-io
-- http://stackoverflow.com/questions/5175906/counting-and-filtering-arrow-for-hxt
-- http://www.haskell.org/haskellwiki/Arrow_tutorial
-- http://www.haskell.org/haskellwiki/HXT#copyXML
-- http://www.vex.net/~trebla/haskell/hxt-arrow/lesson-1.xhtml
-- http://www.haskell.org/haskellwiki/HXT/Practical/Simple2
--
-- NOTE: data structures can be persisted and reloaded via the typeclasses: Read, Show
--  --> persisting is done simply via: writeFile filename $ show datastructure
--  --> and loading via: content <- readFile filename; let schema = read content :: DataStructure
--
-- UPDATE NOTE: the read/show combo for serializing is too slow, moved to binary-based serialization
--
module BI.Common where

import Text.XML.HXT.Core

import BI.Types
import BI.Binary

import Control.Arrow
import Control.Monad
import Control.Applicative
import Data.List
import Data.Char
import Data.Maybe
import qualified Data.Map as Map
import System.Directory
import System.FilePath
import System.Environment

toCSV [] = ""
toCSV (x:xs) = elementToCSV x ++ toCSV xs

keys :: [RNGAttribute] -> [String]
keys []     = []
keys (x:xs) = (raName x) : keys xs

types :: [RNGAttribute] -> [String]
types []     = []
types (x:xs) = (raDatatype x) : types xs

toAttributeTypeMap :: [RNGAttribute] -> Map.Map Name Type
toAttributeTypeMap attrs = Map.fromList $ zip (keys attrs) (types attrs)

{---                        ---}
{--- Convert Element to CSV ---}
{---                        ---}
elementToCSV (DescriptionInstance repository id name) =
    show repository ++ ";" ++ show id ++ ";" ++ "\"" ++ name ++ "\"" ++ "\n"

{- end -}

{--- Pretty Print ---}
class PrettyPrint a where
    attributePrettyPrint :: a -> String
    elementPrettyPrint   :: Int -> a -> String

indent 0 mark                     = ""
indent level mark | mark == level = indent (level-1) mark ++ "+"
indent level mark | mark >  level = indent (level-1) mark ++ " "
indent level mark | mark <  level = indent (level-1) mark ++ "-"

prettyPrint [] = ""
prettyPrint (x:xs) = (elementPrettyPrint 0 x) ++ (prettyPrint xs)

instance PrettyPrint RNGAttribute where
    attributePrettyPrint (RNGAttribute name datatype) = "@"++name++":"++datatype
    elementPrettyPrint a = undefined

instance PrettyPrint RNGElement where
    attributePrettyPrint a = undefined
    elementPrettyPrint level (RNGElement theid name thetype attrs optAttrs zeroM oneM children) =
        intercalate "" [element, requiredChildren, zeroOrMore, oneOrMore]
        where element = header ++ attributes ++ optionalAttributes ++ texttype ++ "\n"
              header             = (indent level level) ++ name ++ " "
              attributes         = intercalate " " (map attributePrettyPrint attrs)
              texttype           = " #" ++ show thetype
              optionalAttributes = case (length optAttrs) == 0 of
                True -> ""
                False -> " | " ++ (intercalate " " (map attributePrettyPrint optAttrs))
              zeroOrMore = case (length zeroM) == 0 of
                True  -> ""
                False -> (indent (level+2) level) ++ "zeroOrMore\n" ++
                         (intercalate "" (map (elementPrettyPrint (level+4)) zeroM))
              oneOrMore = case (length oneM) == 0 of
                True  -> ""
                False -> (indent (level+2) level) ++ "oneOrMore\n" ++
                         (intercalate "" (map (elementPrettyPrint (level+4)) oneM))
              requiredChildren = case (length children) == 0 of
                True  -> ""
                False -> (intercalate "" (map (elementPrettyPrint (level+4)) children))

instance PrettyPrint Object where
    attributePrettyPrint a = undefined
    elementPrettyPrint level (Object service path tag theText ttype attrMap attrTMap children) =
        intercalate "" [element, thechildren]
        where element     = header ++ attributes ++ text ++ "\n"
              text        = case theText of
                Nothing -> ""
                Just x  -> "#[" ++ x ++ "]"
              header      = (indent level level) ++ tag ++ " "
              attributes  = intercalate " " (map (\x -> fst x ++ ":" ++ snd x) $ Map.toList attrMap)
              thechildren = case (length children) == 0 of
                True  -> ""
                False -> (intercalate "" (map (elementPrettyPrint (level+4)) children))

{---         ---}
{--- Parsers ---}
{---         ---}
descriptionParser :: (ArrowXml a) => a XmlTree Element
descriptionParser =
    hasName "descriptions" /> hasName "instance" >>> proc x -> do
    repository <- read ^<< getAttrValue "repository" -< x
    id         <- read ^<< getAttrValue "id" -< x
    name       <- getChildren >>> getText -< x
    returnA -< DescriptionInstance repository id name

amanParser :: (ArrowXml a) => a XmlTree Element
amanParser =
    hasName "parts" /> hasName "part" >>> proc x -> do
    schema <- getAttrValue "schema" -< x
    name   <- getText <<< getChildren <<< deep (hasName "xml") -< x
    returnA -< AmanPart { apID=(-1), apXml=name, apSchema=schema }

-- ...
-- <attribute name="name">
--   <data type="string">
-- </attribute>
attributeParser :: (ArrowXml a) => a XmlTree RNGAttribute
attributeParser =
    hasName "attribute" >>> proc x -> do
    name      <- getAttrValue "name" -< x
    datatype  <- getAttrValue "type" <<< deep (hasName "data") -< x
    returnA -< RNGAttribute { raName=name, raDatatype=datatype }

getAttrValues = getAttrl >>> getName &&& (getChildren >>> getText) >>> returnA

extractTextWithAttrs m t = case Map.lookup "name" m of
    Just "CONTAINER_DUMP"    -> Nothing
    Just "CONTAINER_CONTENT" -> Nothing
    Just _                   -> extractText t
    Nothing                  -> extractText t

genericParser service path defaultMap =
    isElem >>> proc x -> do
    name       <- getElemName                                          -< x
    attributes <- listA (getName <<< getAttrl)                         -< x
    attrValues <- listA getAttrValues                                  -< x
    children   <- listA ((genericParser service path defaultMap) <<< getChildren) -< x
    theText    <- listA (getText <<< getChildren)                      -< x
    returnA -< Object {
        oService          = service,
        oPath             = path,
        oTag              = qualifiedName name,
        oText             = extractTextWithAttrs (Map.fromList attrValues) theText,
        oTextType         = Nothing,
        oAttributeMap     = Map.union (Map.fromList attrValues) defaultMap,
        oAttributeTypeMap = Map.fromList [], -- TODO: merge required and optional attributes? optional ones never occur though
        oChildren         = children
    }

-- reuse the previous attributeParser to recursively build an
-- element object
--
-- <element name="person">
--   <zeroOrMore>
--     ... (recursive)
--   </zeroOrMore>
--   ...
--   <oneOrMore>
--     ... (recursive)
--   </oneOrMore>
--   ...
--   (attributes)
-- </element>
-- TODO: handle <optional>, which holds <attribute> elements
-- TODO: handle <data>, which describes the datatype of the text element
atTag tag = deep (isElem >>> hasName tag)

dataElementParser =
    hasName "data" >>> proc x -> do
    datatype <- getAttrValue "type" -< x
    returnA -< (datatype)

elementParser :: (ArrowXml a) => ServiceDataID -> a XmlTree RNGElement
elementParser dataID =
    hasName "element" >>> proc x -> do
    {-datatag    <- atTag "data" -< x-}
    name       <- getAttrValue "name" -< x
    zeroOrMore <- listA ((elementParser dataID) <<< getChildren <<< deep (hasName "zeroOrMore")) -< x
    oneOrMore  <- listA ((elementParser dataID) <<< getChildren <<< deep (hasName "oneOrMore" )) -< x
    children   <- listA ((elementParser dataID) <<< getChildren <<< deep (hasName "element" ))   -< x
    attrs      <- listA (attributeParser <<< getChildren) -< x
    {-optAttrs   <- listA (attributeParser <<< getChildren <<< deep (hasName "optional")) -< x-}
    textdtype  <- listA (dataElementParser <<< getChildren) -< x
    returnA -< RNGElement {
        reID=dataID,
        reName=name,
        reTextType=textdtype,
        reAttributes=attrs,
        reZeroOrMore=zeroOrMore,
        reOneOrMore=oneOrMore,
        reChildElements=children,
        reOptionalAttrs=[]
    }

-- build a composite arrow that is ORed together
--
-- collectAttributes ["id", "admin", "name", "email"] =
-- getAttrValue "id" <+> getAttrValue "admin" <+> getAttrValue "name" <+> getAttrValue "email"
--
collectAttributes []    = constA Nothing
collectAttributes names = (listA $ foldl1 (\x y -> x <+> y) $ map getAttrValue names) >>> arr Just

innerParseSchema service path []       = constA Nothing
innerParseSchema service path children = (listA $ getChildren >>> (foldl1 (\x y -> x <+> y) $ map (parseWithSchema service path) children)) >>> arr Just

maybeToAttributeMap keys values = case values of
    Nothing -> Map.empty
    Just v  -> Map.fromList $ zip keys v

combineChildren a = case a of
    Nothing     -> []
    Just xs     -> xs

combineChildrenM [] = []
combineChildrenM (x:xs) = (combineChildren x) ++ combineChildrenM xs

extractText :: [String] -> Maybe String
extractText text = case (length text) == 0 of
    True -> Nothing
    False -> Just $ head text

extractTextType schema = case (length $ reTextType schema) == 0 of
    True  -> Nothing
    False -> Just $ head $ reTextType schema

parseWithSchema service path schema =
    let attributeKeys = keys $ reAttributes schema in
    hasName (reName schema) >>> proc x -> do
    attributes    <- collectAttributes $ attributeKeys -< x
    children      <- (innerParseSchema service path) $ reChildElements schema -< x
    zeroMChildren <- (innerParseSchema service path) $ reZeroOrMore schema -< x
    oneMChildren  <- (innerParseSchema service path) $ reOneOrMore schema -< x
    theText       <- listA (getText <<< getChildren) -< x
    returnA -< Object {
        oService          = service,
        oPath             = path,
        oTag              = reName schema,
        oText             = extractText theText, -- TODO: extract text from element
        -- TODO: extract text type from element
        oTextType         = extractTextType schema,
        oAttributeMap     = maybeToAttributeMap attributeKeys attributes,
        oAttributeTypeMap = reqAttributeTypes, -- TODO: merge required and optional attributes? optional ones never occur though
        oChildren         = combineChildrenM [children, oneMChildren, zeroMChildren]
    }
    where reqAttributeTypes = toAttributeTypeMap $ reAttributes schema
          optAttributeTypes = toAttributeTypeMap $ reOptionalAttrs schema

loadSchema = do
    schemaString <- readFile "persist.schema"
    let schema = read schemaString :: RNGElement
    print schema

processFile processor filename =
    runX $ readDocument [withValidate no, withRemoveWS yes] filename >>> getChildren >>> processor

collectAman root i =
    let base          = joinPath [root, show (diID i)]
        aman_filename = joinPath [base,"aman.xml"] in
    aman_filename

collectAmanFilenames root instances = map (collectAman root) instances

-- add the directory from a filename to an existing Aman Part
addDir filename (AmanPart theid name schema) = AmanPart newid realName realSchema
    where base       = joinPath $ init $ splitPath filename
          newid      = fst $ head $ reads $ last $ splitPath base -- read the new id from the base path
          realName   = base ++ name
          realSchema = base ++ schema

processAmanFile filename = do
    aman_parts <- processFile amanParser filename
    return (map (addDir filename) aman_parts)

processAmanParts idparts =
    -- [Element] for each id
    mapM (\(part) -> do
        putStrLn ("Extracting Schema from "++apSchema part)
        processFile (elementParser (apID part)) (apSchema part)
    ) idparts

inputPaths :: String -> [String] -> [String]
inputPaths root services = [(++)] <*> [root] <*> services

outputPaths :: [String] -> [String]
outputPaths services = map (\x -> joinPath ["result", x]) services

buildPaths :: String -> [String] -> [(String, String)]
buildPaths root services =
    zip paths output
    where paths = inputPaths root services
          output = outputPaths services
