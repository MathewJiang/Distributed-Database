# Distributed Database System Project
***
## Project Overview
This project is to implement a distributed database system which stores data on multiple servers, and handle user’s data requests. This project is written in Java.

* It allows the user to store data(i.e. key-value pairs) onto the server by a simple user-server interface, and get the data when they requested so.
* It allows the administrator to add server instances up to the number of maximum threads one could create within the Operating System.
* It allows the administrator to remove server instances.
* It handles unexpected server failures and could recover the data under the help of data replications. 
(Note: this is a (2n + 1) model, where it could handle at most n concurrent server failures when the total number of server being (2n + 1); the replication factor is 3.)
* It allows the user to set alerts for certain data, and be notified when the designated data has been altered; the system will also report what type of modification this is (e.g. deletion, change of content).

(Note: once the data has been deleted the alarm will no longer be triggered unless the user reset the alert.)

***
## Milestone 1
Objective: To implement a storage server which persists data(i.e. key-value pairs) to disk.

* Implemented the communication mechanisms between the client and the server using java sockets; this allows multiple users to add/request data into/from the server while maintaining the consistency within data.
* Optimized the system runtime by using cache, and flushing the data onto disk after the user disconnect from the server.
* Implemented FIFO, LRU, and LFU cache replacement policies when the cache reaches its preset capacities.
* Tested all the functionalities with JUnit tests.
* Result: All JUnit tests passed.


***
## Milestone 2
Objective: To improve the data storage system in Milestone 1 into a distributed database system.

* Implemented the coordination unit (i.e. ECS, ???? Configuration Service) to coordinate actions among servers, using Zookeeper.
* Implemented the communication logic between the servers and the ECS, and between the client and the server.
(Note: the communication logic between the client and server becomes different when the system is now distributed; we hold the assumption that the client knows at least one of the servers, and the server will do the job of distributing the data internally (i.e. invisible to the user) to other servers if it finds itself is not responsible for that piece of data.)
* Implemented the add_server functionality.
* Implemented the remove_server functionality.
* Implemented the data distribution logic based on Consistent Hash: i.e. which server is responsible for the incoming data.
* Implemented data migration functionality to properly migrated the data when server has been added/removed/shutdown; during migration, no user request will be handled; a message will be sent to user to ask him/her try the request again later.
* Tested all the functionalities with JUnit tests.
* Result: All JUnit tests passed. 


***
## Milestone 3
Objective: To improve the data storage system in Milestone 2 to handle server failures.

* Implemented the replication mechanism with a replication factor of 3; the backup data are stored in the next two consecutive (i.e.sequence determined by Consistent Hash) servers.
* Modified add_server to cope with the replication logic.
* Modified remove_server to cope with the replication logic.
* Modified data_migration to cope with the replication logic.
* Implemented failure detection on the ECS end, by the Watcher class within Zookeeper.
* Implemented data recovery logic after failure has been detected; the backups will find its new primary owner and try to replicate itself.
* Result: all JUnit tests passed.

(Note: this is a (2n + 1) model, where the system could handle at most n concurrent server failures without data loss when the total number of server being (2n + 1).)


***
## Milestone 4 (Optional project expansion)
Objective: To improve the storage system in Milestone 3 to set custom alert systems
* Implemented the alert systems which allows user to set watchers on the data they like; the system will notify the user once the data has been modifited and indication the type of modification this is (e.g. change of content, deletion).
* (Some side work) Hacked into the Zookeeper from where allows us to see all the node information from other groups; this happens because one only need the correct port number to connect with the Zookeeper within the same Virtual Machine cluster.
* Result: all JUnit tests passed.



