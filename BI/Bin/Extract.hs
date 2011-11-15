module Main where

import BI.Api (saveObjects)
import BI.Types
import BI.Binary
import BI.Common
import BI.Directory
import BI.MD5
import System.Directory
import System.FilePath
import System.Posix.Files
import System.Time
import System.Time.Utils
import Control.Monad
import Control.Applicative
import Control.Parallel.Strategies
import Control.Parallel
import Data.List
import qualified Data.Map as Map
import Text.Printf

handler e = print e

extractWithSchema root output = do
    let service  = last $ splitPath root
    -- parse the descriptions xml
    instances <- processFile descriptionParser (joinPath [root,"descriptions.xml"])
    -- and write as csv
    writeFile (joinPath [output, "descriptions.csv"]) (toCSV instances)

    -- parse aman.xml files for each instance id
    let amanFilenames = collectAmanFilenames root instances
    amanParts <- mapM processAmanFile amanFilenames
    {-print amanParts-}
    partResults <- mapM processAmanParts amanParts
    {-print $ map prettyPrint partResults-}
    results <- mapM (\(elements) -> do
        {-print $ prettyPrint result)-}
        let theid    = reID $ head $ elements
        let thename  = reName $ head $ elements
        let filename = joinPath [output, "schema", show theid ++ "_" ++ thename ++ ".csv"]
        let content  = prettyPrint elements
        writeFile filename content

        -- use elements as schema to process the corresponding xml file
        let schema              = head elements
        let toParseFilename     = joinPath [root, show theid, thename ++ ".xml"]
        let parseResultFilename = joinPath [output, show theid ++ "_" ++ thename ++ ".csv"]
        putStrLn ("Processing " ++ toParseFilename)
        result <- processFile (parseWithSchema service toParseFilename schema) toParseFilename
        writeFile parseResultFilename $ prettyPrint result
        putStrLn ("Saved to " ++ parseResultFilename)
        return (result)
        )
        (concat partResults)
    return (concat results)

main2 = do
    objects <- mapM (\x -> extractWithSchema (fst x) (snd x)) paths
    saveObjects (concat objects) "extract.raw"
    where paths = buildPaths "/home/conrad/Downloads/data/" ["Register", "Forum", "Code", "Abgabe"]

monthToInt :: Month -> Int
monthToInt month = case month of
    January   -> 1
    February  -> 2
    March     -> 3
    April     -> 4
    May       -> 5
    June      -> 6
    July      -> 7
    August    -> 8
    September -> 9
    October   -> 10
    November  -> 11
    December  -> 12

clockTimeToISO :: CalendarTime -> String
clockTimeToISO ct = printf "%d-%d-%dT%d:%d:%d+00:00" year month day h m s
    where year  = ctYear ct
          month = monthToInt $ ctMonth ct
          day   = ctDay ct
          h     = ctHour ct
          m     = ctMin ct
          s     = ctSec ct

doProcessWithContainerTimestamp service path containerFile = do
    filestatus     <- getFileStatus containerFile
    let timestamp  = modificationTime filestatus
    ct             <- toCalendarTime $ epochToClockTime $ timestamp
    let datetime   = clockTimeToISO ct
    let defaultMap = Map.fromList [("timestamp", show timestamp),
                                   ("iso_date",  clockTimeToISO ct),
                                   ("year",      show $ ctYear ct),
                                   ("month",     show $ ctMonth ct),
                                   ("day",       show $ ctDay ct),
                                   ("hour",      show $ ctHour ct),
                                   ("min",       show $ ctMin ct),
                                   ("sec",       show $ ctSec ct)]
    result <- processFile (genericParser service path defaultMap) path
    return (result)

doProcessPDF service path = do
    filestatus <- getFileStatus path
    let timestamp = modificationTime filestatus
    ct <- toCalendarTime $ epochToClockTime $ timestamp
    let defaultMap = Map.fromList [("timestamp", show timestamp), ("iso_datetime", clockTimeToISO ct)]
    let result = Object {
        oService          = service,
        oPath             = path,
        oTag              = "pdf",
        oText             = Nothing,
        oTextType         = Nothing,
        oAttributeMap     = defaultMap,
        oAttributeTypeMap = Map.fromList [],
        oChildren         = []
    }
    return (result)

doNormalProcess service path = do
    let defaultMap = Map.fromList []
    result <- processFile (genericParser service path defaultMap) path
    return (result)

doProcess service path
    | isInfixOf "resUnit.xml" path == True = do
        let file = fst $ splitFileName path
        let containerFile = file ++ "CONTAINERTYPE.h"
        fileExists <- doesFileExist containerFile
        if fileExists then do
            result <- doProcessWithContainerTimestamp service path containerFile
            return (result)
            else doNormalProcess service path
    | isSuffixOf ".pdf" path == True = do
        -- get the timestamp of the file, then create a Bogus Object with tag "pdf" for later selection
        result <- doProcessPDF service path
        return ([result])
    | otherwise = doNormalProcess service path

extractGeneric root output fileExts = do
    let service = last $ splitPath root
    paths <- getFilesWithExt root fileExts
    objects <- forM paths $ \path -> do
        let index = case findIndex (\x -> x == path) paths of
                    Just i  -> i
                    Nothing -> -1
        let outputPath = joinPath [output, "raw", (md5 path) ++ ".raw"]
        putStrLn $ "("++ show index ++ "/"++ (show $ length paths) ++")" ++ " processing path " ++ path
        result <- doProcess service path
        putStrLn $ "saving to " ++ outputPath
        saveObjects result outputPath
        return (result)
    {-print $ map (\x -> (oTag x, oAttributeMap x, oChildren x)) $ concat result-}
    return (concat objects)

main = do
    let exts = ["xml", "pdf"]
    results <- mapM (\x -> extractGeneric (fst x) (snd x) exts) paths
    let results2 = results `using` parBuffer 4 rwhnf
    saveObjects (concat results2) "all_extracted.raw"
    print "done"
    where paths = buildPaths "/home/conrad/Downloads/data/" ["Register", "Forum", "Abgabe", "Code"]
