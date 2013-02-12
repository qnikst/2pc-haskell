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
          -> Addr a
          -> TEvent ByteString
          -> IO ()
withInput a s b r f = 
    let ev = pushEndOfInput $ pushChunk (runGetIncremental get) b
    in case ev of
         Fail{}    -> return () -- TODO log?
         Partial{} -> return () -- TODO log?
         Done _ _  v -> go v
  where
    st = storageCohort . getStore $ a
    ct = storageLeader . getStore $ a
    reply b' = send a (encode' b') s
    go (PNewTransaction t b1) = do
        ev <- hack <$> (try . f $! EventNew b1)
        case ev of
          Nothing -> return () -- just ignore
          Just (Decline e) -> reply $ PAck t (Left (encode' e))
          Just (Accept  commit rollback) -> do
            let info = TClientInfo TVote commit rollback s
            atomically $ modifyTVar st (M.insert t info)
            reply $ PAck t (Right ())
    go (PCommit t) = do
        mv <- atomically $ M.lookup t <$> readTVar (storageCohort . getStore $ a)
        case mv of
          Nothing -> reply (PAck t (Left "transaction-expired"))
          Just info -> 
            case tclientState info of
              TVote -> do
                let info' = info {tclientState = TCommiting}
                -- TODO: update persistent storage
                atomically $ modifyTVar st (M.insert t info')
                (x, s') <- either (\e -> (PAck t . Left . S8.pack $ show e, TRollback))
                                  (const (PAck t $ Right (), TCommited))
                                  <$> trySome (tcommit info)
                -- TODO: update persistent storage
                atomically $ modifyTVar st
                                        (case s' of
                                           TRollback -> M.delete t
                                           o -> M.insert t (info'{tclientState = o}))
                reply x
              TCommited  -> reply $ PAck t (Right ())
              TCommiting -> return () {- ? -}
              _ -> reply (PAck t (Left "illegal-state"))
    go (PRollback t) = do
        mv <- atomically $ M.lookup t <$> readTVar st
        case mv of
          Nothing -> void . trySome . f $ EventRollback t
          Just info -> do
            case tclientState info of
              TCommited  -> do
                  atomically $ modifyTVar st (M.insert t info{tclientState=TRollingback})
                  ret <- trySome $ trollback info
                  case ret of
                    Left ex -> void . trySome . f $ EventRollbackE t ex
                    Right _ -> return ()
              TCommiting -> return () {- ? -}
              _ -> return ()
            atomically $ modifyTVar st (M.delete t)
    go (PAck t x) = atomically (M.lookup t <$> readTVar ct) >>= \xv ->
      case xv of
        Nothing -> reply (PRollback t) -- send a r (encode' (PRollback t )) s
        Just info -> 
          let aw = s `S.delete` tawait info
              ps = tparty info
              rs = tresult info
              (ok,rs') = case x of 
                           Left  e -> (False,e:rs)
                           Right _ -> (True,rs)
          in case tstate info of
              TVote | S.null aw -> let (msg,st') = if null rs' then (PCommit t, TCommiting)
                                                              else (PRollback t, TRollingback)
                                   in do atomically $ modifyTVar ct (M.insert t info{tstate=st',
                                                                                     tresult=rs'
                                                                                    })
                                         mapM_ (send a (encode' msg)) ps
                    | otherwise -> atomically $ modifyTVar ct (M.insert t info{tawait=aw
                                                                              ,tresult=rs'})
              TCommiting | not ok -> let ps' = s `S.delete` ps
                                     in do atomically $ modifyTVar ct (M.insert t info{tstate=TRollingback
                                                                                      ,tawait=ps'
                                                                                      ,tresult=rs'
                                                                                      })
                                           mapM_ (send a (encode' $ PRollback t)) ps'
                         | S.null aw -> atomically $ do
                                            modifyTVar ct (M.delete t)
                                            putTMVar (result info) (Right ())
                         | otherwise -> atomically $ modifyTVar ct (M.insert t info{tawait=aw})
              TRollingback | S.null aw -> atomically $ do
                                              modifyTVar ct (M.delete t)
                                              putTMVar (result info) (Left rs')
                           | otherwise -> atomically $ modifyTVar ct (M.insert t info{tawait=aw
                                                                                     ,tresult=rs'})
              TCommited -> error "illegal server state"
              TRollback -> error "illegal server state"


            
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
waitResult a t = atomically $ do
    mx <- M.lookup t <$> readTVar st
    case mx of
      Nothing -> return Nothing
      Just x  -> Just <$> readTMVar (result x)
  where st = storageLeader. getStore $ a
  

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
