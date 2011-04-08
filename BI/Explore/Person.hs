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

validMatrikelNr "" = False
validMatrikelNr x = and [ (head x) `elem` ['a'..'w'], length x == 8 ]

extractMatrikelNrYear x =
    if year <= "40" then "20"++year else "19"++year
    where year = fst $ splitAt 2 $ tail x

-- extract the year from the matrikel nummer if its a valid one
-- and save it as "inscription_year" in the attributemap
transformMatrikelNr object =
    case M.lookup "id" originalMap of
        Just v -> if validMatrikelNr v then M.insert "inscription_year" (extractMatrikelNrYear v) originalMap
                                       else originalMap
        Nothing -> originalMap
    where originalMap = oAttributeMap object

{-transformPerson object =-}

{-transformAllPerson objects = map transformPerson objects-}
