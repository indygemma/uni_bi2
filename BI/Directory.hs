module BI.Directory (
    getRecursiveContents,
    getFilesWithExt
) where

import Control.Monad
import System.Directory (doesDirectoryExist, getDirectoryContents)
import System.FilePath
import Data.List

getRecursiveContents :: FilePath -> IO [FilePath]
getRecursiveContents topdir = do
    names <- getDirectoryContents topdir
    let properNames = filter (`notElem` [".", ".."]) names
    paths <- forM properNames $ \name -> do
        let path = topdir </> name
        isDirectory <- doesDirectoryExist path
        if isDirectory
            then getRecursiveContents path
            else return [path]
    return (concat paths)

getFilesWithExt :: FilePath -> [String] -> IO [FilePath]
getFilesWithExt topdir exts = do
    paths <- getRecursiveContents topdir
    return (filter (\path -> any id $ suffixResults path) paths)
    where suffixResults path = map (flip isSuffixOf path) exts

