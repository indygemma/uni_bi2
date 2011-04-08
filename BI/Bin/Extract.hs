module Main where

import BI.Api (saveObjects)
import BI.Types
import BI.Binary
import BI.Common
import BI.Directory
import System.Directory
import System.FilePath
import Control.Monad
import Control.Applicative
import Control.Parallel.Strategies
import Control.Parallel

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

extractGeneric root output = do
    let service = last $ splitPath root
    paths <- getFilesWithExt root "xml"
    result <- forM paths $ \path -> do
        putStrLn $ "processing path " ++ path
        result <- processFile (genericParser service path) path
        return (result)
    {-print $ map (\x -> (oTag x, oAttributeMap x, oChildren x)) $ concat result-}
    return (concat result)

main = do
    results <- mapM (\x -> extractGeneric (fst x) (snd x)) paths
    let results2 = parBuffer 4 rwhnf results
    saveObjects results2 "extract_code.raw"
    print "done"
    where paths = buildPaths "/home/conrad/Downloads/data/" ["Code"]
