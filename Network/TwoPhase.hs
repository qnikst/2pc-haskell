{-# LANGUAGE RankNTypes, TypeFamilies, FlexibleContexts, OverloadedStrings, DeriveDataTypeable #-}
module Network.TwoPhase 
  ( -- * Datatypes
    TPNetwork(..)
  , TPStorage(..)
  , Storage(..)
  , mkStorage 
  -- * API
  -- ** Basic API
  -- $api
  , withInput
  -- *** Client
  , Action(..)
  , TEvent
  , TCommit
  , TRollback
  -- ** Server
  -- $server
  , THandler
  , TransactionResult
  , transaction
  , waitResult
  , stmResult
  , cancel
  , timeout
  , runServer
  -- ** asynchronous api
  -- ** utils
  , decoding
  -- , accept
  -- , decline
  --, rollback
  , module Control.Concurrent.STM.Split.TVar
  ) where

import Prelude hiding (sequence, mapM_)
import Control.Applicative
import Control.Concurrent.STM
import Control.Concurrent.STM.Split.TVar
import Control.Concurrent.STM.Split.Class
import Control.Exception
import Control.Monad (replicateM, (<=<), unless, forever)
import Control.Monad.Error (Error())
import Control.Monad.Trans
import Control.Monad.Trans.Either
import Data.Binary
import Data.Binary.Get
import Data.ByteString (ByteString)
import qualified Data.ByteString as S (concat, pack)
import qualified Data.ByteString.Char8 as S8
import qualified Data.ByteString.Lazy as SL
import Data.Foldable (mapM_, forM_) 
import Data.Typeable
import Data.Map (Map)
import qualified Data.Map as M
import Data.Set (Set)
import qualified Data.Set as S
import qualified System.Random.MWC as MWC

-- | TNetwork class that abstracts network logic. You need to
-- to define a 'send' function TwoPhase protocol message over
-- you protocol.
--
-- Example of TPNetwork you may find in 'Network.TwoPhase.STMNetwork' module.
class TPNetwork a where
  type Addr a
  -- ^ Address type, it can be any name or IP address depending on your protocol
  send :: a -- ^ Controller
       -> ByteString        -- ^ message data
       -> Addr a            -- ^ recipient address
       -> IO ()


data TwoPhaseException = ERollback TID
                       | ERollbackE TID SomeException
                       | ETransactionNotFound TID
                       deriving (Typeable, Show)

instance Error TwoPhaseException
instance Exception TwoPhaseException

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
type TransactionResult = Either TErrors ()
type TRollback = IO ()
type TCommit   = IO ()
type TEvent  a = a -> IO (Maybe Action)
type TErrors   = [ByteString]
type TResult x = SpTVar x (Maybe TransactionResult)

-- | Transaction handler is used to control transaction flow on server
data THandler = THandler !TID !(TResult Out)

-- | Internal storage class internal storage should collect
-- information about pending transactions avaliable at runtime
class TPNetwork a => TPStorage a where
  getStore :: a -> Storage a

-- | Transactional storage
data Storage a = Storage 
        { storageLeader :: TVar (Map TID (TServerInfo a))
        , storageCohort :: TVar (Map TID (TClientInfo a))
        }

-- | initialize default STM storage
mkStorage :: IO (Storage a)
mkStorage = Storage <$> newTVarIO M.empty 
                    <*> newTVarIO M.empty


-- | Simple protocol
-- | Simple protocol
-- | Transaction state
data TState = TVote
            | TRollingback
            | TRollback
            | TCommiting
            | TCommited
            deriving (Eq, Show)

data TServerInfo a = TServerInfo
      { tstate       :: !TState
      , tparty       :: Set (Addr a)
      , tawait       :: Set (Addr a)
      , tdata        :: !ByteString
      , tresult      :: ![ByteString]
      , result       :: TResult In
      }

data TClientInfo a = TClientInfo 
      { tclientState :: TState                                -- ^ transaction state
      , tcommit      :: TCommit                               -- ^ commit action
      , trollback    :: TRollback                             -- ^ rollback action
      , tcsender     :: Addr a
      }

-- | User actions
data Action = Decline ByteString                        -- ^ cancel transaction with error message 
            | Accept  TCommit TRollback                 -- ^ accept transaction and set commit and rollback messages


-- | low level interface function. 
--
-- N.B. withInput is synchronous function untill you'll make it asynchronous
-- explicitly see Asynchonous API section.
withInput :: (TPNetwork a, TPStorage a, Ord (Addr a))
          => a                                          -- ^ controller
          -> Addr a                                     -- ^ sender address
          -> ByteString                                 -- ^ message
          -> TEvent ByteString                          -- ^ callback
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
          Nothing -> do
              reply $ ackOk t
              throw (ERollback t)
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
                inCommiting info | not ok = goRollingBackS info{tparty = s `S.delete` tparty info}
                                 | S.null (tawait info) = goFinalize info
                                 | otherwise = save info 
                inRollingBack info | S.null (tawait info) = goFinalize info
                                   | otherwise = save info 
                goVote info  | not ok = inVote info{tparty = s `S.delete` tparty info}
                             | otherwise = inVote info
                goStep2 info | null (tresult info) {- no errors -} = goCommitingS info
                             | otherwise = goRollingBackS info
                goCommitingS info = runStep2 True info
                goRollingBackS info | S.null (tparty info) = goFinalize info
                                    | otherwise = runStep2 False info
                goFinalize info = let r = if null (tresult info)
                                                then Right ()
                                                else Left (tresult info)
                                  in atomically $ do writeSpTVar (result info) (Just r)
                                                     modifyTVar ct (M.delete tid)
                save info = atomically $ modifyTVar ct (M.insert tid info)
                runStep2 state i = 
                  let (m,s) = if state then (PCommit, TCommiting) 
                                       else (PRollback, TRollingback)
                  in do atomically $ modifyTVar ct (M.insert tid i{tstate = s, tawait = tparty i})
                        mapM_ (send a (encode' $ m tid)) (tparty i)


    -- State blocks
    goInitialize tid dat_ = do
        ev <- hack <$> (try $! f dat_)
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
        atomically $ modifyTVar st (M.insert tid info{tclientState = TRollback})
        reply $! ackOk tid
        goClean tid
        case ret of
          Left ex -> throw ex
          Right _ -> return ()
    -- client clean block
    goCommiting tid info = do
        atomically $ modifyTVar st (M.insert tid info{tclientState = TCommiting})
        ret <- trySome (tcommit info)
        case ret of
          Left e -> do reply (PAck tid . Left . S8.pack $ show e)
                       goRollingBack tid info
          Right _ -> do reply (ackOk tid)
                        atomically $ modifyTVar st (M.insert tid info{tclientState = TCommited})
    goClean tid = atomically $ modifyTVar st (M.delete tid)

            
-- | Start asynchonous transaction
transaction :: (TPStorage a, TPNetwork a, Binary d, Ord (Addr a)) 
            => a -- ^ transaction controller 
            -> d -- ^ event 
            -> [Addr a]  -- ^ list of partipitiants
            -> IO THandler
transaction a d rs = do
    tid <- generateTID
    (inBox, outBox) <- splitTVar <$> newTVarIO Nothing
    let info = TServerInfo TVote (S.fromList rs) (S.fromList rs) db [] inBox
    atomically $ modifyTVar st (M.insert tid info)
    forM_ rs $ \r -> send a (encode' (PNewTransaction tid db)) r
    return (THandler tid outBox)
  where db = encode' d
        st = storageLeader . getStore $ a

-- | Lock until transaction is finished
waitResult :: THandler -> IO TransactionResult
waitResult (THandler _ r) = atomically $ maybe retry return =<< readSpTVar r

-- | get an STM function that will either read transaction result or retries.
-- This function is usable if you want to add timeout to transaction: 
--
-- @
-- a <- transaction com Tran1 hosts
-- t <- registerDelay 1000000
-- print =<< atomically $ (stmResult t) `orElse`
--                            (readTVar t >>= flip unless retry >>= return ["timeout"])
-- @
stmResult :: THandler -> STM TransactionResult
stmResult (THandler _ x) = maybe retry return =<< readSpTVar x
  
-- | cancels non finished transaction. 
-- This functon sets status to Rolling back and sends rollback events. You need to 
-- wait result to guarantee that transaction is finished
cancel :: (TPStorage a, TPNetwork a) => a -> THandler -> IO (Either TwoPhaseException ())
cancel a (THandler t _) = runEitherT $ do -- TODO use goRollbackingS function
    x <- hoistEither <=< liftIO . atomically $ do
        mx <- M.lookup t <$> readTVar ct
        case mx of
          Nothing -> return $ Left (ETransactionNotFound t)
          Just x  -> do modifyTVar ct (M.adjust (\i -> i{tstate = TRollingback, tawait= tparty i}) t) 
                        return $ Right x
    lift $ do 
      forM_ (tparty x) $ \r -> send a (encode' (PRollback t)) r
  where ct = storageLeader . getStore $ a

-- | create transaction with timeout, this is operation will be locked untill it completes
-- note require -threaded, see 'Control.Concurrent.STM.TVar' registerDelay
timeout :: (TPStorage a, TPNetwork a, Binary d, Ord (Addr a)) 
        => Int 
        -> a 
        -> d 
        -> [Addr a] 
        -> IO (Maybe TransactionResult)
timeout t a b c = do
  h <- transaction a b c
  d <- registerDelay t
  mr <- atomically $ (Just <$> stmResult h)
           `orElse` (readTVar d >>= flip unless retry >> return Nothing)
  case mr of
    Nothing -> cancel a h >> return Nothing
    _ -> return mr

-- | Helper to run leader
-- you can use this function is you can guarantee that this controller will always be a leader.
runServer :: (TPStorage a, TPNetwork a, Ord (Addr a))
          => a
          -> (IO (Addr a, ByteString)) 
          -> IO ()
runServer c feeder = forever $ feeder >>= \(a,b) -> withInput c a b (const . return . Just $ Decline "server")


{-
asynchonously = TID -> IO Action -> IO ()
asynchonously t f = do
  forkIO $ (do
    x <- f
    case x of
      Accept -> accept t
      Decline x -> decline t)
    `onException` (\(e::SomeException) -> decline (S8.pack $ show e) >> 
                                          throw e)
  return Nothing
-}

-- $api 
-- To use TwoPase in your program you need to capture messages as always
-- and if you get TwoPhase Protocol message you need to pass it into 
-- 'withInput' function, it's a general interface. 
--


-- $server
-- Transaction leader can control transaction flow only by high level API:
-- that gives an ability to create and cancel transactions. 

-- $client
--
-- On client you will have 2 transaction steps first is voting step, where you 
-- need to prepare transaction and either accept it or decline. If you declining
-- transaction than you need manually undo all preparation steps, if you accepting
-- you need to pass finalisation (commit) function and rollback function, rollback
-- should support rollbacking after prepare when commit was not called, and rollback
-- after commit.
--
-- For client by default all transactions a sequencial i.e. when you preparing 
-- or commiting transaction thread that calls 'withInput' will be locked. To run
-- transaction preparation asynchronously you need to call asynchronously.
--







hack :: Either SomeException (Maybe Action) -> Maybe Action
hack (Left e) = Just (Decline . S8.pack $! show e)
hack (Right Nothing) = Nothing
hack (Right (Just x)) = Just x

trySome :: IO a -> IO (Either SomeException a)
trySome = try

generateTID :: IO ByteString
generateTID = MWC.withSystemRandom . MWC.asGenIO $ \gen ->
                    S.pack <$> replicateM 20 (MWC.uniform gen)

decoding :: (Binary b) => (b -> IO (Maybe Action)) -> ByteString -> IO (Maybe Action)
decoding f = maybe (return . Just $ Decline "unknown message") f . decodeMay' 

encode' :: Binary b => b -> ByteString
encode' = S.concat . SL.toChunks . encode
{-# INLINE encode' #-}

decodeMay' :: (Binary a) => ByteString -> Maybe a
decodeMay' b =
    case pushEndOfInput $ pushChunk (runGetIncremental get) b of
      Done _ _  v -> return v
      _ -> fail "no parse"

