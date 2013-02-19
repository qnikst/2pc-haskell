{-# LANGUAGE TypeFamilies #-}
module Control.Concurrent.STM.Split.TVar
  ( SpTVar
  , splitTVar
  , readSpTVar
  , writeSpTVar
  ) where

import Control.Concurrent.STM
import Control.Concurrent.STM.Split.Class

newtype SpTVar dir a = SpTVar (TVar a)

splitTVar :: TVar a -> (SpTVar In a, SpTVar Out a)
splitTVar x = (SpTVar x, SpTVar x)

readSpTVar :: SpTVar Out a -> STM a
readSpTVar (SpTVar x) = readTVar x

writeSpTVar :: SpTVar In a -> a -> STM ()
writeSpTVar (SpTVar x) v = writeTVar x v 
