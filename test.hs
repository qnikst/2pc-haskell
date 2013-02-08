import Control.Applicative (Applicative)
import Control.Monad.Reader (ReaderT, ask, runReaderT)
import Control.Monad.Trans (MonadIO, liftIO)

class Contains a s where
  wrap :: a s
  unwrap :: s -> Maybe a

data a :+: b = L a | R b  deriving (Show)

infix 5 :+:

instance Contains a (a :+: b) where
  wrap  = L 
  unwrap (L x) = Just x
  unwrap _     = Nothing

instance Contains b (a :+: b) where
  wrap = R
  unwrap (R x) = Just x
  unwrap _     = Nothing

instance Contains a s => Contains a (b :+: s) where



