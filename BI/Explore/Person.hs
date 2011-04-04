module BI.Explore.Person where

import BI.Api
import BI.Types
import BI.Common
import Data.List
import qualified Data.Map as M

personIds objects = do
    let persons = selectAll [hasTag "person"] objects
    {-let persons = selectAll [hasTag "person", attrEq "email" "bry.zachanike@unet.univie.ac.at"] objects-}
    {-let unique_groups = group $ sort $ map (\x -> M.lookup "id" $ oAttributeMap x) persons-}
    let unique_groups = group $ sort persons
    return (unique_groups)

