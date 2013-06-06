{-# LANGUAGE PackageImports #-}

module Main where

{-import "base" Prelude-}
import Text.XML.HXT.Core
{-import System-}
import System.Environment

main = do
  [filename] <- getArgs
  runX (readDocument [withValidate no, withRemoveWS yes] filename >>> putXmlTree "-")
