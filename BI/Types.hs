module BI.Types (
    Element(..),
    RNGAttribute(..),
    RNGElement(..),
    Object(..),

    ServiceDataID,
    RepositoryID,
    InstanceID,
    ServiceDataID,
    Name,
    Value,
    Type,
    AttributeMap,
    AttributeTypeMap
) where

import qualified Data.Map as Map

data Element = DescriptionInstance {
                 diRepository :: RepositoryID,
                 diID         :: InstanceID,
                 diName       :: String
               }
             -- AmanPart holds the file paths for each rng/xml pair for a service under the Data
             -- folder.
             | AmanPart {
                apID     :: ServiceDataID,
                apXml    :: String,
                apSchema :: String
             } deriving (Show)

data RNGAttribute = RNGAttribute {
                    raName     :: String,
                    raDatatype :: String
                  } deriving (Read, Show)

data RNGElement = RNGElement {
                    reID            :: ServiceDataID,
                    reName          :: String,
                    reTextType      :: [Type],
                    reAttributes    :: [RNGAttribute], -- specifically, RNGAttribute
                    reOptionalAttrs :: [RNGAttribute],
                    reZeroOrMore    :: [RNGElement],
                    reOneOrMore     :: [RNGElement],
                    reChildElements :: [RNGElement]
                } deriving (Read, Show)

type RepositoryID  = Int
type InstanceID    = Int
type ServiceDataID = Int

data Object = Object {
                oService          :: String,
                oTag              :: Name,
                oText             :: Maybe String,
                oTextType         :: Maybe Type,
                oAttributeMap     :: AttributeMap,
                oAttributeTypeMap :: AttributeTypeMap,
                oChildren         :: [Object]
            } deriving ({-! Eq, Ord, Show, Binary !-})

type Name             = String
type Value            = String
type Type             = String
type AttributeMap     = Map.Map Name Value
type AttributeTypeMap = Map.Map Name Type

