{-# LANGUAGE OverloadedStrings #-}
module Main
  ( main )
  where


import Test.Framework
import Test.Framework (Test, testGroup)
import Test.Framework.Providers.HUnit (testCase)
import Test.HUnit

import Control.Monad (when)
import Control.Concurrent.STM
import qualified Data.Map as M


import Network.TwoPhase.Class
import Network.TwoPhase

main :: IO ()
main = defaultMain [tests]

tests = testGroup "2pc"
  [ testCase "case01 (register)" case01 
  , testCase "case02 (input)" case02
--  , testCase "case03 (voting)" case03
  ]


si :: String
si = "!"

case01 = do
  net@(STMNetwork st s) <- mkNetwork ["a","b","c"]
  tid <- register net "a" si ["b"] (\_ _ -> return ())
  let (Just ch) = M.lookup "b" st
  checkTChan [("a",encode' $ PNewTransaction tid (encode' si),"b")] ch
  let (Just ch) = M.lookup "c" st
  checkTChan [] ch

case02 = do
  net@(STMNetwork st s) <- mkNetwork ["a","b","c"]
  tid <- register net "a" si ["b"] (\_ _ -> return ())
  let (Just ch) = M.lookup "b" st
  (s,m,r) <- atomically . readTChan $ ch
  v <- newTVarIO False
  withInput net s m r (\e -> 
    case e of
      EventNew b -> when (b == encode' si) (atomically $ writeTVar v True) >> return (Nothing::Maybe (Action STMNetwork ()))
      _ -> return Nothing)
  assertBool "correct encoded message" =<< readTVarIO v 
  tid <- register net "a" si ["b","c"] (\_ _ -> return ())
  v <- newTVarIO 0
  (s,m,r) <- atomically . readTChan $ ch
  withInput net s m r (\e -> 
    case e of
      EventNew b -> when (b == encode' si) (atomically $ modifyTVar v (+1)) >> return (Nothing::Maybe (Action STMNetwork ()))
      _ -> return Nothing)
  let (Just ch) = M.lookup "c" st
  (s,m,r) <- atomically . readTChan $ ch
  withInput net s m r (\e -> 
    case e of
      EventNew b -> when (b == encode' si) (atomically $ modifyTVar v (+1)) >> return (Nothing::Maybe (Action STMNetwork ()))
      _ -> return Nothing)
  assertEqual "correct encoded message" 2 =<< readTVarIO v 


checkTChan :: (Show a, Eq a) => [a] -> TChan a -> Assertion
checkTChan [] c = do
  r <- atomically (tryReadTChan c) 
  assertEqual "empty channel" Nothing r
checkTChan (x:xs) c = do
  r <- atomically (tryReadTChan c)
  assertEqual "value in channel" (Just x) r
  checkTChan xs c
