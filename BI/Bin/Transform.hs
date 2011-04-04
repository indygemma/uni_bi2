module Main where

import BI.Api
import BI.Explore.Person (personIds)

-- Transform
main = do
    objects <- loadObjects "extract.raw"
    result <- personIds objects
    print result
