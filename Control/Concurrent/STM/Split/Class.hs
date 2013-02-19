module Control.Concurrent.STM.Split.Class
  ( In(..)
  , Out(..)
--  , Split(..)
  ) where

data In = In
data Out = Out

{-
class Split a where
  type GSplit v a 
  split :: a -> (GSplit a In, GSplit a Out)
-}


