{-# LANGUAGE TypeFamilies #-}
module Network.TwoPhase.Class 
  where

import Control.Applicative
import Control.Concurrent.STM
import Data.ByteString
import Data.Map (Map)
import qualified Data.Map as M

import Network.TwoPhase

type Message = (Addr STMNetwork, ByteString, Addr STMNetwork)

data STMNetwork = STMNetwork (Map ByteString (TChan Message)) (Storage STMNetwork)

instance TPNetwork STMNetwork where
  type Addr STMNetwork = ByteString
  send (STMNetwork s _) from m to = 
    case M.lookup to s of
      Nothing -> return ()
      Just x -> atomically $ writeTChan x (from,m,to)
instance TPStorage STMNetwork where
  getStore (STMNetwork _ s) = s  

mkNetwork :: [ByteString] -> IO STMNetwork
mkNetwork bs = STMNetwork <$> (M.fromList <$> mapM (\x -> (,) x <$> newTChanIO) bs)
                          <*> (Storage <$> newTVarIO M.empty
                                       <*> newTVarIO M.empty)

cloneNetwork :: STMNetwork -> IO STMNetwork
cloneNetwork (STMNetwork a _) = STMNetwork a <$> (Storage <$> newTVarIO M.empty
                                                          <*> newTVarIO M.empty)

extractCh :: STMNetwork -> ByteString -> Maybe (TChan Message)
extractCh (STMNetwork a _) b = M.lookup b a
