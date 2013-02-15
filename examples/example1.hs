{-# LANGUAGE OverloadedStrings #-}


import Test.Framework
import Test.Framework (Test, testGroup)
import Test.Framework.Providers.HUnit (testCase)
import Test.HUnit

import Control.Monad
import Control.Concurrent
import Control.Concurrent.STM
import qualified Data.Map as M


import Network.TwoPhase.Class
import Network.TwoPhase

si :: String
si = "!"


case01 = do
  let lst = ["a","b","c","d"]
  net <- mkNetwork lst 
  forM lst $ \l -> forkIO $ do
    net' <- if l=="a" then return net else cloneNetwork net
    let mch = extractCh net' l
    case mch of
      Nothing -> error "no such network"
      Just ch -> forever $ do
                   (s, m, r) <- atomically $ readTChan ch
                   withInput net' s m r $ \e -> 
                       case e of
                         EventNew b -> do
                           -- putStrLn $ unwords ["host:",show l,show b]
                           return . Just $ Accept (putStrLn "commit") (putStrLn "rollback")
                         EventRollback b -> undefined
                         EventRollbackE b e -> undefined
  tid <- register net "a" si ["b","c","d"] 
  waitResult net tid
        
        


