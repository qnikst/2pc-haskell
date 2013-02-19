# 2pc is a two phase transaction implementation.

2pc is abstracted over the communication type to integrate
2pc into your application you need to:

  * define a TPNetwork class instance, that describes 2pc 
    can send message over the network.

  * write function that will feed controller with new events

As an example you can see Network.TwoPhase.STM


