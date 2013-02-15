{-# LANGUAGE OverloadedStrings #-}
module Main
  ( main )
  where


import Test.Framework
import Test.Framework (Test, testGroup)
import Test.Framework.Providers.HUnit (testCase)
import Test.HUnit

import Control.Monad (when, forM_)
import Control.Concurrent.STM
import qualified Data.Map as M
import Data.Binary
import qualified Data.ByteString as S
import qualified Data.ByteString.Lazy as SL


import Network.TwoPhase.STM
import Network.TwoPhase

main :: IO ()
main = defaultMain [tests]

tests = undefined

data Description = Data { step1 :: Bool
                        , r1 :: TVar (Maybe Bool)
                        , step2 :: Bool
                        , step3 :: Bool
                        }

runState :: STMNetwork -> S.ByteString -> Description -> IO ()
runState net n (Description s1 r1 s2 s3) = forever $ do
    (s,m,r) <- atomically (readTChan ch)
    withInput s m r $ \x -> 
        case x of
          NewEvent _ -> if s1 then accept
                              else decline
          _   -> error "!"
  where
    accept = atomically (putTVar r1 (Just True)) >> return $ Accept commit rollback
    decline = atomically (putTVar r1 (Just False)) >> return $ Decline "!!" 
    commit  = if s2 then atomically (putTVar r2 (Just True))
                    else atomically (putTVar r2 (Just False))
    rollback = if s3 then atomically (putTVar r3 (Just True))
                  s3 then atomically (putTVar r3 (Just False))
    

experiment :: STMNetwork -> S.ByteString -> [Description] -> IO ()
experiment ds = do
    let es = zip ns ds
    net <- mkNetwork "main" (map fst es)
    forM_ es $ \(n,d) -> forkIO $ do
        net' <- cloneNetwork n net
        runState net' n d
    t <- transaction net "!" (map first es)
    forkIO $ do
      case extractCh net "main" of
        Nothing -> error "no ch"
        Just x  -> forever $ do
          (s,m,r) <- atomically (readTChan ch)
          withInput net m r (const Nothing)
      
  where ns = [S.singleton x | x <-[0..]]

encode' :: Binary b => b -> S.ByteString
encode' = S.concat . SL.toChunks . encode
{-# INLINE encode' #-}
