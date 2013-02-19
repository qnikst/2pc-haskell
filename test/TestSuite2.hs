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
import Data.ByteString (ByteString)
import qualified Data.ByteString as S
import Data.Maybe

import Network.TwoPhase.STM
import Network.TwoPhase

main :: IO ()
main = defaultMain tests

tests :: [Test.Framework.Test]
tests = [ testGroup "one host"
            [ testCase "True,True"   $ testTransaction [[True,True]] 
            , testCase "True,False"  $ testTransaction [[True,False]]
            , testCase "False,True"  $ testTransaction [[False,True]]
            , testCase "False,False" $ testTransaction [[False,False]]
            ]
        , testGroup "2 hosts" $
              map (\x -> testCase (show x) $ testTransaction x)
                [ [[True,True],[True,True]]
                , [[True,False],[True,True]]
                , [[False,True],[True,True]]
                , [[True,True],[False,True]]
                , [[False,False],[True,True]]
                , [[True,False],[True,False]]
                , [[False,True],[False,True]]
                ] 
        , testGroup "3 hosts" $
              map (\x -> testCase (show x) $ testTransaction x)
                [ [[True,True],[True,True],[True,True]]
                , [[True,False],[True,True],[True,True]]
                , [[False,True],[True,True],[True,True]]
                , [[True,True],[False,True],[True,True]]
                , [[False,False],[True,True],[True,True]]
                , [[True,False],[True,False],[True,False]]
                , [[False,True],[False,True],[True,True]]
                ] 
        , testGroup "timeout" $
                [ testCase "timeout (before)" $ testTimeout True
                , testCase "timeout (fail)" $ testTimeout False
                ]
        ]


testTransaction :: [[Bool]] -> IO ()
testTransaction e = do
    xs <- mapM mkDesc e
    r <- experiment xs
    let tAll = all (all id) e
        r' = fromJust r
        res  = if tAll then isRight r'
                       else isLeft r'
    assertBool "got matching result" res
    mapM_ (checkD (chS1 e) (chS2 e)) xs
  where 
    chS1 :: [[Bool]] -> Bool
    chS1 = all head
    chS2 :: [[Bool]] -> Bool
    chS2 = all (head.tail)

    checkD a b d@(D t1 t2 x1 x2 x3) = do
       v1 <- readTVarIO x1
       v2 <- readTVarIO x2
       v3 <- readTVarIO x3
       assertEqual "first step met" (Just t1) v1
       if a then do
              assertEqual "2nd step met" (Just t2) v2
              unless b (assertEqual "rolled back" (Just True) v3)
            else do
              assertEqual "nobody reach step2" Nothing v2
              if t1 then assertEqual (show d ++ ": should rollback") (Just True) v3
                    else assertEqual (show d ++ ": no rollback should be called") Nothing v3
          
      

type MB = TVar (Maybe Bool)
data Description = D Bool Bool MB MB MB
instance Show Description where
  show (D b1 b2 _ _ _) = "D{"++show b1++","++show b2++"} "

mkDesc :: [Bool] -> IO Description
mkDesc [a1, a2] = D a1 a2 <$> newTVarIO Nothing 
                          <*> newTVarIO Nothing
                          <*> newTVarIO Nothing
mkDesc _ = error "!"
      

runState :: STMNetwork -> S.ByteString -> Description -> IO ()
runState net n (D s1 s2 r1 r2 r3) = 
    case extractCh net n of
      Nothing -> error "!!"
      Just ch  -> forever $ do
        (s,m,_) <- atomically $ readTChan ch
        withInput net s m $ \x -> 
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
    ls <- forM es $ \(n,d) -> forkIO $ do
            net' <- cloneNetwork net n
            runState net' n d
    t <- transaction net ("!"::S.ByteString) (map fst es)
    _ <- forkIO $ do
           case extractCh net "main" of
             Nothing -> error "no ch"
             Just ch  -> forever $ do
                (s,m,_) <- atomically (readTChan ch)
                withInput net s m (const $ return Nothing)
    x <- waitResult net t
    mapM_ (const yield) ls
    mapM_ killThread ls
    return x
  where ns = [S.singleton x | x <-[0..]]


testTimeout ret = do
  com <- mkNetwork "main" ["a","b"]
  a <- transaction com ("1"::ByteString) ["b"]
  when ret $ void . forkIO $ do com' <- cloneNetwork com "b" 
                                case extractCh com' "b" of
                                        Nothing -> return ()
                                        Just ch -> forever $ do
                                          (s,m,_) <- atomically $ readTChan ch
                                          withInput com' s m (const . return . Just $ Accept (return ()) (return ()))
  t <- registerDelay 500000
  mf <- stmResult com a
  _ <- forkIO $ do
         case extractCh com "main" of
             Nothing -> return ()
             Just ch  -> forever $ do
                (s,m,_) <- atomically (readTChan ch)
                withInput com s m (const $ return Nothing)
  r <- case mf of
            Nothing -> return $ Left ["no reply"] 
            Just f  -> atomically $ f `orElse`
                          (readTVar t 
                           >>= flip unless retry 
                           >>  return (Left ["timeout"]))
  if ret 
      then assertEqual "transaction finished" (Right ()) r
      else assertEqual "transaction timeout"  (Left ["timeout"]) r
    

isLeft :: Either a b -> Bool
isLeft (Left _) = True
isLeft _ = False

isRight :: Either a b -> Bool
isRight (Right _) = True
isRight _ = False
