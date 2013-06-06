module BI.Filters where

import BI.Api (exAttr)

data Filter = FID | FDBSExercise | FDBSMilestone deriving Show

filterAbgabeDBSExerciseCourse = filter isExerciseEvent
    where isExerciseEvent o | exAttr "course_id" o == "1"  = True
                            | exAttr "course_id" o == "9"  = True
                            | exAttr "course_id" o == "16" = True
                            | otherwise                    = False

filterAbgabeDBSMilestoneCourse = filter isMilestoneEvent
    where isMilestoneEvent o | exAttr "course_id" o == "2"  = True
                             | exAttr "course_id" o == "10" = True
                             | exAttr "course_id" o == "17" = True
                             | otherwise                    = False

evalFilter FID = id
evalFilter FDBSExercise = filterAbgabeDBSExerciseCourse
evalFilter FDBSMilestone = filterAbgabeDBSMilestoneCourse
