{-# LANGUAGE OverloadedStrings #-}
module Main
  ( main )
  where


import Test.Framework
import Test.Framework (Test, testGroup)
import Test.Framework.Providers.HUnit (testCase)
import Test.HUnit

import Control.Applicative
import Control.Monad
import Control.Concurrent
import Control.Concurrent.STM
import qualified Data.Map as M
import Data.Binary
import Data.ByteString (ByteString)
import Data.List
import qualified Data.ByteString as S
import qualified Data.ByteString.Lazy as SL
import Data.Maybe


import Network.TwoPhase.STM
import Network.TwoPhase

main :: IO ()
main = do 
    forM_ exps $ \e -> do
        putStrLn $ "running: " ++ show e
        xs <- mapM mkDesc e
        r <- experiment xs
        mapM_ (checkD (chS1 e) (chS2 e)) xs
        forM_ xs $ \(D _ _ x1 x2 x3) -> do
           v1 <- readTVarIO x1
           v2 <- readTVarIO x2
           v3 <- readTVarIO x3
           putStrLn $ intercalate ":" $ map show [v1,v2,v3]
          
        case (fromJust r) of
          Left _  | chAll e -> putStrLn "error"
                  | otherwise   -> putStrLn "ok"
          Right _ | chAll e -> putStrLn "ok"
                  | otherwise  -> putStrLn "error"
  where 
    chAll :: [[Bool]] -> Bool
    chAll = all (all id) 
    chS1 :: [[Bool]] -> Bool
    chS1 = all head
    chS2 :: [[Bool]] -> Bool
    chS2 = all (head.tail)

    checkD a b (D t1 t2 x1 x2 x3) = do
       v1 <- readTVarIO x1
       v2 <- readTVarIO x2
       v3 <- readTVarIO x3
       unless (v1 == Just t1) $ print "error in 1"
       if a then return () -- TODO
            else do
              unless (v2 == Nothing) $ putStrLn "error in 2"
              unless (v3 == Nothing) $ putStrLn "error in 3"
    exps = [ [[True,True], [True,True]]
           , [[True,False], [True,True]]
           , [[True,False], [True,False]]
           , [[True,True], [True,False]]
           ]
                  
          
      

type MB = TVar (Maybe Bool)
data Description = D Bool Bool MB MB MB

mkDesc [a1, a2] = D a1 a2 <$> newTVarIO Nothing 
                          <*> newTVarIO Nothing
                          <*> newTVarIO Nothing
      

runState :: STMNetwork -> S.ByteString -> Description -> IO ()
runState net n (D s1 s2 r1 r2 r3) = 
    case extractCh net n of
      Nothing -> error "!!"
      Just ch  -> forever $ do
        (s,m,r) <- atomically $ readTChan ch
        withInput net s m r $ \x -> 
            case x of
              EventNew _ | s1 -> accept
                         | otherwise  -> decline
              _   -> error "!"
  where
    accept = atomically (writeTVar r1 (Just True)) >> return (Just (Accept commit rollback))
    decline = atomically (writeTVar r1 (Just False)) >> return (Just (Decline "!!"))
    commit | s2 = atomically (writeTVar r2 (Just True))
           | otherwise = atomically (writeTVar r2 (Just False)) >> error "!!!"
    rollback = atomically (writeTVar r3 (Just True))
    

experiment :: [Description] -> IO (Maybe (Either [ByteString] ()))
experiment ds = do
    let es = zip ns ds
    net <- mkNetwork "main" (map fst es)
    forM_ es $ \(n,d) -> forkIO $ do
        net' <- cloneNetwork net n
        runState net' n d
    t <- transaction net ("!"::S.ByteString) (map fst es)
    forkIO $ do
      case extractCh net "main" of
        Nothing -> error "no ch"
        Just ch  -> forever $ do
          (s,m,r) <- atomically (readTChan ch)
          withInput net s m r (const $ return Nothing)
    waitResult net t
  where ns = [S.singleton x | x <-[0..]]

encode' :: Binary b => b -> S.ByteString
encode' = S.concat . SL.toChunks . encode
{-# INLINE encode' #-}
