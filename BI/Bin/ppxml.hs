module Main where

import Text.XML.HXT.Core
import System
import System.Environment

main = do
  [filename] <- getArgs
  runX (readDocument [withValidate no, withRemoveWS yes] filename >>> putXmlTree "-")
