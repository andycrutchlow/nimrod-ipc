nimrod-ipc
==========

Simple interprocess communication for java using ZeroMQ.

Covers two major use-cases :

1) Remote method invocation : A thread in running in one jvm (client) can call any method in any class running in another jvm (server).
The calling thread is blocked until a response object is returned or an exception is thrown.
Any number of calling threads in client can be in progress whilst server is 

2) Publish/Subscribe : 


