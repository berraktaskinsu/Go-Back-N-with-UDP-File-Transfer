# Go-Back-N-with-UDP-File-Transfer
A program consisting of two modules Sender.java and receiver.py which when run at the same time, makes a file transfer to the current directory, using Go Back N protocol with UDP.

The Sender.java module functions as the sender module of a end-end connection where Go Back N Protocol and UDP is used. The file includes the implementation of both the Application and Transport layers.
When run in a second console after running the receiver.py program, which acts as the receiver in the connection, the program makes a file transfer using GBN Protocol.

In order for the program to function properly, Sender.java module must be run after receiver.py module.

In order to compile and run Sender.java, the following commands are used:

	javac Sender.java

	java Sender <file_path> <receiver_port_number> <window_size> <retransmission_timeout>

Here, the receiver port number must be the same as the port number provided as argument to the receiver.py module. An example command for running the Sender module is provided below:

	java Sender  C:\Users\berrak\OneDrive\Desktop\pa2v2\image.png 3000 20 120

Note that if the file path contains any white space, it must be enclosed with quotation marks, i.e "C:\Users\berrak\OneDrive\Desktop\pa2v2\image.png".
