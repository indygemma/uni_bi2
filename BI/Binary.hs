{-# LANGUAGE TemplateHaskell #-}
module BI.Binary where

import Data.DeriveTH
import Data.Binary
import BI.Types

$( derive makeEq     ''Object )
$( derive makeOrd    ''Object )
$( derive makeShow   ''Object )
$( derive makeBinary ''Object )
