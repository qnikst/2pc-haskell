{-# LANGUAGE RankNTypes, TypeFamilies, FlexibleContexts, OverloadedStrings, DeriveDataTypeable, ScopedTypeVariables #-}
module Network.TwoPhase 
  ( 
  -- $intro
  -- * Network
    TPNetwork(..)
  , TPStorage(..)
  , Storage(..)
  , mkStorage 
  -- * API
  -- ** Basic API
  -- $api
  , withInput
  -- *** Client
  , VoteHandler
  , TCommit
  , TRollback
  , accept
  , decline
  , cleanup
  -- *** Server
  -- $server
  , transaction
  , THandler
  , TransactionResult
  , waitResult
  , stmResult
  , cancel
  , timeout
  , cleanupS
  , runTxServer
  -- ** utils
  , decoding
  --, rollback
  , module Control.Concurrent.STM.Split.TVar
  , module Control.Monad.Trans.Resource
  ) where

import Prelude hiding (sequence, mapM_)
import Control.Applicative
import Control.Concurrent.STM
import Control.Concurrent.STM.Split.TVar
import Control.Concurrent.STM.Split.Class
import Control.Exception.Lifted
import Control.Monad (replicateM, (<=<), unless, forever, when)
import Control.Monad.Error (Error())
import Control.Monad.Trans
import Control.Monad.Trans.Resource
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
import Data.Maybe (isJust)
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
  send :: a                 -- ^ Network controller
       -> ByteString        -- ^ Message data
       -> Addr a            -- ^ Recipient address
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
              | PCleanup TID
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
  put (PCleanup t) = putWord8 4 >> put t
  get = do t <- get :: Get Word8
           case t of
             0 -> PNewTransaction <$> get <*> get
             1 -> PCommit <$> get
             2 -> PRollback <$> get
             3 -> PAck <$> get <*> get
             4 -> PCleanup <$> get
             _ -> error "Protocol.get"

-- | Transaction ID
type TID = ByteString
type TransactionResult = Either TErrors ()
type TRollback = IO ()
type TCommit   = IO ()
type TErrors   = [ByteString]
type TResult x = SpTVar x (Maybe TransactionResult)

-- | Transaction handler is used to control transaction flow on server
data THandler = THandler !TID !(TResult Out)

-- | Transaction handler that used to either accept or rollback 
-- transaction.
data VoteHandler = VoteHandler !(TMVar Bool) 
                               !(TCommit -> TRollback -> IO ())
                               !(ByteString -> IO ())

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
      , action       :: Bool -> IO ()
      }

data TClientInfo a = TClientInfo 
      { tclientState :: TState                                -- ^ transaction state
      , tcommit      :: TCommit                               -- ^ commit action
      , trollback    :: TRollback                             -- ^ rollback action
      , tcsender     :: Addr a
      , tcdata       :: ByteString
      }

-- | low level interface function. 
--
-- Callback client is a function that will be called once new transaction will
-- arive (i.e. server doesn't need it). When user work with function he should
-- either 'accept' or 'decline' transaction, if none will be done trasaction 
-- will be declined, if you want to run that function asynchonously you should
-- run it in 'resourceForkIO', that will prevent transaction from declining until
-- this thread will be finished.
withInput :: (TPNetwork a, TPStorage a, Ord (Addr a), Binary b)
          => a                                          -- ^ Network controller
          -> Addr a                                     -- ^ Sender address
          -> ByteString                                 -- ^ Message
          -> ((ReleaseKey, VoteHandler) -> b -> ResourceT IO ())      -- ^ Callback
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
          Nothing -> throw (ERollback t)
          Just info -> do
            case tclientState info of
              TVote -> goRollingBack t info
              TCommited  -> goRollingBack t info
              TCommiting -> atomically $ modifyTVar st  (M.insert t info{tclientState = TRollingback})
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
                        action i state
    go (PCleanup t) = atomically $ do
          mtr <- M.lookup t <$> readTVar st
          case mtr of
            Nothing -> return ()
            Just _ -> modifyTVar st (M.delete t)

    -- State blocks
    goInitialize tid dat_ = 
        case decodeMay' dat_ of
          Nothing -> reply (ackNo tid ("no-parse"::ByteString))
          Just d  -> runResourceT $ do
               lock <- liftIO $ newTMVarIO False
               res  <- allocate (return $ VoteHandler lock
                                          (\cm rb -> do 
                                              atomically $ modifyTVar st (M.insert tid (TClientInfo TVote cm rb s dat_))
                                              reply $! ackOk tid)
                                          (reply . (ackNo tid)))
                                (releaseTHandler)
               ev <- try $! f res d
               case ev of
                  Left (e::SomeException)  -> decline (snd res) (S8.pack $ show e)
                  Right _ -> return ()

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
        minfo <- M.lookup tid <$> readTVarIO st
        case tclientState <$> minfo of 
          Nothing -> do 
            reply (PAck tid . Left . S8.pack $ "incorrect state")
            goRollingBack tid info
          Just TCommiting ->
            case ret of
              Left e -> do 
                reply (PAck tid . Left . S8.pack $ show e)
                goRollingBack tid info
              Right _ -> do 
                reply (ackOk tid)
                atomically $ modifyTVar st (M.insert tid info{tclientState = TCommited})
          Just TRollingback -> goRollingBack tid info
    goClean tid = atomically $ modifyTVar st (M.delete tid)

            
-- | Start asynchonous transaction
transaction :: (TPStorage a, TPNetwork a, Binary d, Ord (Addr a)) 
            => a -- ^ transaction controller 
            -> d -- ^ event 
            -> (Bool -> IO ())   -- ^ event to perform when all partipitians accepted transaction
            -> [Addr a]  -- ^ list of partipitiants
            -> IO THandler
transaction a d f rs = do
    tid <- generateTID
    (inBox, outBox) <- splitTVar <$> newTVarIO Nothing
    let info = TServerInfo TVote (S.fromList rs) (S.fromList rs) db [] inBox f
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

-- | Send a command to clean up current transaction
-- transaction should be finished otherwise nothing will be done
cleanupS :: (TPNetwork a) => a -> THandler -> [Addr a] -> IO Bool
cleanupS a (THandler t s) adx = do
    x <- atomically (readSpTVar s)
    when (isJust x) (mapM_ (send a (encode' (PCleanup t))) adx)
    return $! isJust x

-- | Cleanup on client
-- N.B. this function doesn't perform any changes on transaction
cleanup :: (TPNetwork a, TPStorage a, Binary b) => a -> b -> IO ()
cleanup a b = atomically $ modifyTVar st (M.filter (((encode' b)==).tcdata) )
  where st = storageCohort (getStore a)

-- | create transaction with timeout, this is operation will be locked untill it completes
-- note require -threaded, see 'Control.Concurrent.STM.TVar' registerDelay
timeout :: (TPStorage a, TPNetwork a, Binary d, Ord (Addr a)) 
        => Int 
        -> a 
        -> d 
        -> (Bool -> IO ())
        -> [Addr a] 
        -> IO (Maybe TransactionResult)
timeout t a b f c = do
  h <- transaction a b f c
  d <- registerDelay t
  mr <- atomically $ (Just <$> stmResult h)
           `orElse` (readTVar d >>= flip unless retry >> return Nothing)
  case mr of
    Nothing -> cancel a h >> return Nothing
    _ -> return mr

-- | Helper to run leader
-- you can use this function is you can guarantee that this controller will always be a leader.
runTxServer :: (TPStorage a, TPNetwork a, Ord (Addr a))
          => a
          -> IO (TChan (Addr a, ByteString), IO ())
runTxServer c = do
  ch <- newTChanIO
  return (ch, forever $ (atomically $ readTChan ch) >>= \(a,b) -> withInput c a b (\_ (_::ByteString) -> return ()))


-- $intro
-- Two phase commit protocol (2PC) is an atomic commitement protocol. It uses 
-- distributed algorithm that allow to run atomic trasactions on a different
-- hosts.
--
-- Algorithm consists of two parts: 
--
--    * voting phase
--    
--    * commit phase
--
-- In voting phase every node is notified about incomming transaction and each
-- node should prepare transaction: i.e. change it state, check if transaction 
-- is able to run and then either accept or decline transaction.  When node is
-- accepting transaction it should provide commit and rollback actoins that will
-- be automatically triggered when transaction will change state.
--
-- When all nodes sent their vote leader either commit transaction (if everybody
-- accepted) or send a rollback event. So when client receive a new event it 
-- triggeres action. And then either commit if there was no exceptions or rollback
-- transaction. So transaction can be rolled back even after commit message.
--
-- Problems:
--
--    1. If transaction succesfully aplied then node will never know that every other nodes
--    finished also finished this transaction, so you have to store rollback function
--    forever, and it is a possible memleak. To prevent system from a memleak there
--    is a cleanup API: 'cleanupS' for transaction leader and 'cleanup' for client.
--
--    2. The library itself doesn't support methods checking concurrent transaction, i.e. 
--    library can't say if current recipient is in transaction now. This is needed because
--    each node can has many indenendent transactions. So user should provide state check
--    in prepare function (in vote phase)
--
--    3. There is a time perion when system is not in consistent state: when done finishes
--    transaction it doesn't know if all transaction is finished, and if request need
--    all nodes to be in consistent state you need to either call it in 2pc transaction
--    or introduce mechanism to notify nodes about transaction finish.
--
-- This library is network agnosting this means that you can use 2pc over your
-- protocol my writing a "wrapper" with specified 'send' function.

-- $api 
-- To use TwoPase in your program you need to capture messages message outside of
-- 2pc and then feed it into 'withInput' a general library interface. This should
-- be done as for server (leader only node) and for clients.
--
-- WithInput is not asynchronous, so process will be locked until internal action 
-- (i.e. phase action) will be finished. So it can be a bottleneck, however you
-- can use resourceForkIO to jump form the action. This should be done carefully
-- as if you will try to commit asynchronously you can receive rollback message
-- while you are commiting. (TODO fix it)


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

trySome :: IO a -> IO (Either SomeException a)
trySome = try

generateTID :: IO ByteString
generateTID = MWC.withSystemRandom . MWC.asGenIO $ \gen ->
                    S.pack <$> replicateM 20 (MWC.uniform gen)

decoding :: (MonadIO m, Binary b) => (VoteHandler -> b -> m ()) -> VoteHandler -> ByteString -> m ()
decoding f v = maybe (decline v "unknown message") (f v) . decodeMay' 

encode' :: Binary b => b -> ByteString
encode' = S.concat . SL.toChunks . encode
{-# INLINE encode' #-}

decodeMay' :: (Binary a) => ByteString -> Maybe a
decodeMay' b =
    case pushEndOfInput $ pushChunk (runGetIncremental get) b of
      Done _ _  v -> return v
      _ -> fail "no parse"

releaseTHandler :: VoteHandler -> IO ()
releaseTHandler (VoteHandler lock _apply c) = do
    lc <- atomically $ takeTMVar lock
    unless lc  $ c "No response"
    atomically $ putTMVar lock True

decline :: MonadIO m 
        => VoteHandler    -- ^ transaction 
        -> ByteString     -- ^ message
        -> m ()
decline (VoteHandler lock _apply cancel') bs = liftIO $ do
    lc <- atomically $ takeTMVar lock
    unless lc (cancel' bs)
    atomically (putTMVar lock True)
-- | accept transaction 
accept :: MonadIO m 
        => VoteHandler -- ^ transaction handler
        -> TCommit     -- ^ commit action
        -> TRollback   -- ^ rollback action
        -> m ()
accept (VoteHandler lock apply _cancel) commit rollback = liftIO $ do
    lc <- atomically $ takeTMVar lock
    unless lc (apply commit rollback)
    atomically (putTMVar lock True)
