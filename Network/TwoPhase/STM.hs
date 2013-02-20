{-# LANGUAGE TypeFamilies #-}
module Network.TwoPhase.STM
  where

import Control.Applicative
import Control.Concurrent.STM
import Data.ByteString
import Data.Map (Map)
import qualified Data.Map as M

import Network.TwoPhase

type Message = (Addr STMNetwork, ByteString, Addr STMNetwork)

data STMNetwork = STMNetwork (ByteString) (Map ByteString (TChan Message)) (Storage STMNetwork)

instance TPNetwork STMNetwork where
  type Addr STMNetwork = ByteString
  send (STMNetwork from s _) m to = 
    case M.lookup to s of
      Nothing -> return ()
      Just x -> atomically $ writeTChan x (from,m,to)

instance TPStorage STMNetwork where
  getStore (STMNetwork _ _ s) = s  

mkNetwork :: ByteString -> [ByteString] -> IO STMNetwork
mkNetwork n bs = STMNetwork n <$> (M.fromList <$> mapM (\x -> (,) x <$> newTChanIO) (n:bs))
                              <*> mkStorage

cloneNetwork :: STMNetwork -> ByteString -> IO STMNetwork
cloneNetwork (STMNetwork _ a _) f = STMNetwork f a <$> mkStorage

extractCh :: STMNetwork -> ByteString -> Maybe (TChan Message)
extractCh (STMNetwork _ a _) b = M.lookup b a

