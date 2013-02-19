{-# LANGUAGE RankNTypes, TypeFamilies, FlexibleContexts, OverloadedStrings #-}
module Network.TwoPhase 
  ( TPNetwork(..)
  , TPStorage(..)
  , withInput
  , Action(..)
  , Storage(..)
  , Event(..)
  -- ** functions
  , mkStorage 
  , transaction
  , waitResult
  , stmResult
  , toEvent
  ) where

import Prelude hiding (sequence, mapM_)
import Control.Applicative
import Control.Monad (void, forM_, replicateM)
import Control.Concurrent.STM
import Control.Exception
import Data.Binary
import Data.Binary.Get
import Data.ByteString (ByteString)
import qualified Data.ByteString as S (concat, pack)
import qualified Data.ByteString.Char8 as S8
import qualified Data.ByteString.Lazy as SL
import Data.Foldable (mapM_)
import Data.Traversable as T
import Data.Map (Map)
import qualified Data.Map as M
import Data.Set (Set)
import qualified Data.Set as S
import qualified System.Random.MWC as MWC

-- | TNetwork class abstracts network logic and
-- need to be defined to bind ThoPhase controller 
-- to the real network and can be sent over arbitrary
-- protocol
class TPNetwork a where
  type Addr a
  send :: a -> ByteString -> Addr a -> IO ()

-- | Simple protocol
data Protocol = PNewTransaction TID ByteString                                      -- ^ new transaction message
              | PCommit  TID                                                        -- ^ commit message
              | PRollback TID -- ^ rollback message
              | PAck TID (Either ByteString ())   -- ^ ack message
              deriving (Show)

ackOk :: TID -> Protocol
ackOk t = PAck t (Right ())
ackNo :: Binary b => TID -> b -> Protocol
ackNo t f = PAck t (Left (encode' f))


instance Binary Protocol where
  put (PNewTransaction t b) = putWord8 0 >> put t >> put b
  put (PCommit t) = putWord8 1 >> put t
  put (PRollback t) = putWord8 2 >> put t
  put (PAck t v) = putWord8 3 >> put t >> put v
  get = do t <- get :: Get Word8
           case t of
             0 -> PNewTransaction <$> get <*> get
             1 -> PCommit <$> get
             2 -> PRollback <$> get
             3 -> PAck <$> get <*> get
             _ -> error "Protocol.get"

-- | Transaction ID
type TID = ByteString

class (TPNetwork a) => TPStorage a where
  getStore :: a -> Storage a

-- | Transactional storage
data Storage a = Storage 
        { storageLeader :: TVar (Map TID (TServerInfo a))
        , storageCohort :: TVar (Map TID (TClientInfo a))
        }

mkStorage :: IO (Storage a)
mkStorage = Storage <$> newTVarIO M.empty 
                    <*> newTVarIO M.empty

type TRollback = IO ()
type TCommit   = IO ()
type TEvent  a = Event a -> IO (Maybe Action)

-- | Transaction state
data TState = TVote
            | TRollingback
            | TRollback
            | TCommiting
            | TCommited
            deriving (Eq, Show)

data TServerInfo a = TServerInfo
      { tstate       :: TState
      , tparty       :: Set (Addr a)
      , tawait       :: Set (Addr a)
      , tdata        :: ByteString
      , tresult      :: [ByteString]
      , result       :: TMVar (Either [ByteString] ())
      }

data TClientInfo a = TClientInfo 
      { tclientState :: TState                                -- ^ transaction state
      , tcommit      :: TCommit                               -- ^ commit action
      , trollback    :: TRollback                             -- ^ rollback action
      , tcsender     :: Addr a
      }

-- | UI Events
data Event b = EventNew b                            -- ^ new transaction received
             | EventRollback TID                     -- ^ transaction without corresponding information rolled back
             | EventRollbackE TID SomeException      -- ^ rollback failed

-- | User actions
data Action = Decline ByteString
            | Accept  TCommit TRollback


withInput :: (TPNetwork a, TPStorage a, Ord (Addr a), Show (Addr a))
          => a 
          -> Addr a
          -> ByteString 
          -> TEvent ByteString
          -> IO ()
withInput a s b f = 
    let ev = pushEndOfInput $ pushChunk (runGetIncremental get) b
    in case ev of
         Done _ _  v -> go v
         _ -> return ()
  where
    st = storageCohort . getStore $ a
    ct = storageLeader . getStore $ a
    reply b' = send a (encode' b') s
    go (PNewTransaction t b1) = goInitialize t b1
    go (PCommit t) = do
        mv <- atomically $ M.lookup t <$> readTVar st
        case mv of
          Nothing -> reply (PAck t (Left "transaction-expired"))
          Just info -> 
            case tclientState info of
              TVote -> goCommiting t info
              TCommited  -> reply (ackOk t)
              TCommiting -> return () -- another thread will notify server it there are problems
              _ -> reply (PAck t (Left "illegal-state"))
    go (PRollback t) = do
        mv <- atomically $ M.lookup t <$> readTVar st
        case mv of
          Nothing -> reply $ ackOk t
          Just info -> do
            case tclientState info of
              TVote -> goRollingBack t info
              TCommited  -> goRollingBack t info
              TRollback -> reply (ackOk t)
              _ -> return () {- ? -}
    go (PAck tid retData) = do
        xv <- atomically $ M.lookup tid <$> readTVar ct
        case xv of
          Nothing   -> reply (PRollback tid)
          Just info_ -> f' info_{tawait = aw, tresult = rs'}
              where
                f' = case tstate info_ of
                            TVote -> goVote
                            TCommiting -> inCommiting
                            TRollingback -> inRollingBack
                            _ -> error "illegal server state"
                aw = s `S.delete` tawait info_
                (ok,rs') = either (\e -> (False,e:tresult info_)) 
                                  (const $ (True, tresult info_)) retData
                inVote info | S.null (tparty info) {- nobody to responce -} = goFinalize info
                            | S.null (tawait info) {- next state -} = goStep2 info 
                            | otherwise = save info
                inCommiting info | not ok = goRollingBack info{tparty = s `S.delete` tparty info}
                                 | S.null (tawait info) = goFinalize info
                                 | otherwise = save info 
                inRollingBack info | S.null (tawait info) = goFinalize info
                                   | otherwise = save info 
                goVote info  | not ok = inVote info{tparty = s `S.delete` tparty info}
                             | otherwise = inVote info
                goStep2 info | null (tresult info) {- no errors -} = goCommiting info
                             | otherwise = goRollingBack info
                goCommiting info = runStep2 True info
                goRollingBack info | S.null (tparty info) = goFinalize info
                                   | otherwise = runStep2 False info

                goFinalize info = let r = if null (tresult info)
                                                then Right ()
                                                else Left (tresult info)
                                  in atomically $ do putTMVar (result info) r
                                                     modifyTVar ct (M.delete tid)
                save info = atomically $ modifyTVar ct (M.insert tid info)
                runStep2 st i = 
                  let (m,s) = if st then (PCommit, TCommiting) 
                                    else (PRollback, TRollingback)
                  in do atomically $ modifyTVar ct (M.insert tid i{tstate = s, tawait = tparty i})
                        mapM_ (send a (encode' $ m tid)) (tparty i)


    -- State blocks
    goInitialize tid dat_ = do
        ev <- hack <$> (try . f $! EventNew dat_)
        case ev of
          Nothing -> return () -- just ignore
          Just (Decline e) -> reply (ackNo tid e)
          Just (Accept commit rollback) -> do
            atomically $ modifyTVar st (M.insert tid (TClientInfo TVote commit rollback s))
            reply $! ackOk tid
    -- client rolling back block
    goRollingBack tid info = do 
        atomically $ modifyTVar st (M.insert tid info{tclientState = TRollingback}) -- TODO adjust (?)
        ret <- trySome $ trollback info
        case ret of
          Left ex -> void . trySome . f $ EventRollbackE tid ex
          Right _ -> return ()
        atomically $ modifyTVar st (M.insert tid info{tclientState = TRollback})
        reply $! ackOk tid
        goClean tid
    -- client clean block
    goCommiting tid info = do
        -- TODO: update persistent storage
        atomically $ modifyTVar st (M.insert tid info{tclientState = TCommiting})
        ret <- trySome (tcommit info)
        case ret of
          Left e -> do reply (PAck tid . Left . S8.pack $ show e)
                       goRollingBack tid info
          Right e -> do reply (ackOk tid)
                        atomically $ modifyTVar st (M.insert tid info{tclientState = TCommited})
    goClean tid = atomically $ modifyTVar st (M.delete tid)

            
transaction :: (TPStorage a, TPNetwork a, Binary d, Ord (Addr a)) => a -> d -> [Addr a] -> IO TID
transaction a d rs = do
    tid <- generateTID
    box <- newEmptyTMVarIO
    let info = TServerInfo TVote (S.fromList rs) (S.fromList rs) db [] box
    atomically $ modifyTVar st (M.insert tid info)
    forM_ rs $ \r -> send a (encode' (PNewTransaction tid db)) r
    return tid
  where db = encode' d
        st = storageLeader . getStore $ a


waitResult :: (TPStorage a) => a -> TID -> IO (Maybe (Either [ByteString] ()))
waitResult a t = do 
    mx <- atomically $ M.lookup t <$> readTVar st
    case mx of
      Nothing -> return Nothing
      Just x  -> atomically $ Just <$> readTMVar (result x)
  where st = storageLeader. getStore $ a

-- | get an STM function that will either read transaction result or retries.
-- This function is usable if you want to add timeout to transaction: 
--
-- @
-- a <- transaction com Tran1 hosts
-- t <- registerDelay 1000000
-- mf <- stmResult com a
-- print =<< case mf of
--             Nothing -> return ()
--             Just f  -> atomically $ f `orElse`
--                            (readTVar t >>= flip unless retry >>= return ["timeout"])
-- @
stmResult :: (TPStorage a) => a -> TID -> IO (Maybe (STM (Either [ByteString] ())))
stmResult a t = do
    x <- atomically $ M.lookup t <$> readTVar ct
    return $ case x of
               Nothing -> Nothing
               Just t  -> Just . readTMVar $ result t
  where ct = storageLeader . getStore $ a
  

toEvent :: (Binary b) => TEvent b -> TEvent ByteString
toEvent f = \x -> f (fromBS x) 
  where
    fromBS :: (Binary b) => Event ByteString -> Event b
    fromBS (EventNew b) = EventNew (decode' b)
    fromBS (EventRollback t) = EventRollback t
    fromBS (EventRollbackE t e) = EventRollbackE t e


hack :: Either SomeException (Maybe Action) -> Maybe Action
hack (Left e) = Just (Decline . S8.pack $! show e)
hack (Right Nothing) = Nothing
hack (Right (Just x)) = Just x

encode' :: forall b . Binary b => b -> ByteString
encode' = S.concat . SL.toChunks . encode
{-# INLINE encode' #-}

trySome :: IO a -> IO (Either SomeException a)
trySome = try

generateTID :: IO ByteString
generateTID = MWC.withSystemRandom . MWC.asGenIO $ \gen ->
                    S.pack <$> replicateM 20 (MWC.uniform gen)

decode' :: (Binary b) => ByteString -> b
decode' b = 
  case (pushEndOfInput $ pushChunk (runGetIncremental get) b) of
    Done _ _ v -> v
    _ -> error "no parse"

{-
decodeMay' :: (Binary b) => ByteString -> Maybe b
decodeMay' b = 
  case (pushEndOfInput $ pushChunk (runGetIncremental get) b) of
    Done _ _ v -> return v
    _ -> fail "no parse"
-}
