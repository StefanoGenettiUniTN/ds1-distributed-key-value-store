# Distributed Key-Value Store with Data Partitioning and Replication
## Distributed System 1 course project

### AUTHORS: Carlin Nicola and Stefano Genetti 
### YEAR: 2023

### REPORT: [ds1_project_report](https://github.com/StefanoGenettiUniTN/ds1-distributed-key-value-store/blob/main/ds1_project_report.pdf)

### Abstract
This project represents a distributed system which implements a distributed hash table based peer-to-peer key-value storage service inspired by Amazon Dynamo. The architecture is composed by two Akka actor classes whose instances are referred to as nodes and clients which interacts by means of exchange of messages. In order to simulate the behaviour of a real computer network, we add random delays before each transmission. The distributed hash table is composed by multiple peer nodes interconnected together. The stored data is partitioned among these nodes in order to balance the load. Symmetrically, several nodes record the same items for reliability and accessibility. The partitioning is based on the keys that are associated with both the stored items and the nodes. We consider only unsigned integers as keys, which are logically arranged in a circular space, such that the largest key value wraps around to the smallest key value like minutes on analog clocks. On the other hand, the clients support data services which consist of two commands: i. update(key,value); ii. get(key)→value; which are used respectively to insert and read from the DHT. Any storage node in the network is able to fulfill both requests regardless of the key, forwarding data to/from appropriate nodes. Although multiple clients can read and write on the data structure in parallel, we assume that each client performs read and write operations sequentially one at a time The overlay ring network topology of nodes which constitutes the distributed hash table, is not static; on the contrary nodes can dynamically join, leave, crash and recover one at a time and only when there are no ongoing operations. We assume that operations might resume while one or more nodes are still in crashed state. In order to handle these functionalities, each node supports management services which can be accessed by means of dedicated messages. When nodes leave or join the network, the system repartitions the data items accordingly.
