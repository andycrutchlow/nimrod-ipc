nimrod-ipc
==========

A simple API for interprocess communication for java using ZeroMQ.

The API covers two major use-cases whilst requiring minimal configuration and code to use it :

1) Remote method invocation : A thread in running in one jvm (client process) can call any method in any class passing any arguments running in another jvm (server process). The calling application thread is blocked until a response object is returned or an exception is thrown. Any number of calling threads in the client jvm can be in progress whilst the server jvm is servicing any number of client calls. The calls are multiplexed from the client process over one pre-existing transport connection to the server and dispatched to and handled by descrete worker threads in server to maximise concurrency. The size of thread pools are configurable. The response is not dictated by FIFO to the server but rather how long the specific call takes to complete in the server. A jvm process can be a client to any number of server processes or a server to any number of different clients processes or a mixture of both. A server process can make services (methods) available on one or more transports. Applicable ZeroMQ Transports covered are "ipc" for processes co-located on same physical server or "tcp" if processes are running on different physical servers. Leveraging ZeroMQ features it does not matter the order that the processes are instantiated. A client process will connect to a server process when the server process becomes available. This might be immediately if the server process is already running or later if the server process is yet to start. Appropriate exceptions will be thrown during time that server process is unavailable.

2) Publish/Subscribe : yet to write this up...but its good!

A further optional extension or mode is available. By running a supplied agent process two extra functions become available :

1) Latest value re-publish on initial subscription. So when a subject is initially subscribed to by a subscriber process the publisher of that subject will re-publish its most recent value so the subscriber process immediately sees the current value rather than having to wait for the next change.

2) Any start or stop of either a RMI service or a Publisher service will be communicated to all other processes running the API which are configured to connect to the agent and have registered callbacks expressing an interest in a particular services start/stop event.


