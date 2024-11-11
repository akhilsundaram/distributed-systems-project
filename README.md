# G59

**CS425 Project** 

| Akhil Sundaram  | Anurag Choudhary  |
| ------ | ------ |


**Machine Problem 1** - **Distributed Log Parser**

(Created a new repo with the correct group name and slug link)

A simple parser for querying log files Distributed over a set of machines in a network. Written in Golang.


**Project Structure**

>**Parser** :
> This contains the log parser that greps across multiple machines.

>**Listener** : 
> This container socket listener code that will be running on all machines part of the log system.

>**LogGen** : 
> This contains code to generate logs for unit tests.


**Code Instructions**


- **Start the listeners**: In every machine to be queried, start the listener.

>`>$ cd Listener/`
>
>`>$ nohup go run . &`

- **Update machine list**: Once we have the listeners running in every machine to be grepped. Update the list of active machines in `/Parser/cmd/grepper/configs/machines.txt/`.

- **Check log file path**: Make sure your log files within the machines are in the correct path and name. The Distributed parser expects the logs to be in `/home/logs/machine.i.log` where i is the machine number ranging from 01-10.

-**Run the parser - grep**: Run the grep command through the Parser package. 

> `>$ cd Parser`
>
>`>$ go run . -cmd "grep" -o "" -s "pattern"`

**cmd** is the command input, "grep" is the only valid input.

**o** is the options you can pass to grep.

**s** is the expected pattern/regex.


**Machine Problem 2** - **Distributed Group Membership and Failure Detection**


Created a group membership system which detects failures and maintains a real-time updated membership list at every node using the SWIMs algorithm. This implementation closely follows the same, with a few differences and modifications. 


**Project Structure**


>**Buffer** :
> This contains the buffer logic that will be piggybacked on every ping and pin-ack message.


>**Introducer** : 
> This contains the code for the "introducer" node, and this can be any VM (as long as the configuration inside introducer.go is set accordingly).


>**Membership** : 
> This contains code to maintain , update and modify the membership list at every node. It also contains the suspicion logic for enabling PingAck-Sus mechanism.


>**Ping** :
>This contains code to ping all the nodes in the system in a randomized manner from every other node. Integrates all the other mechanisms we have specified.


>**Utility** :
>This contains some utility methods that are used across all files.


**Code Instructions**


- **Start the Introducer**: Whichever machine is chosen to be the introducer, perform following on that node (by default, VM Node 1)


>`>$ cd MP2/`
>
>`>$ go run .`


**Start the other Nodes to join the system**: Run the same command on other nodes, and use the cli prompts to see the membership list at any node, at any time !


>`>$ cd MP2/`
>
>`>$ go run .`



**Enabling PingAck-Sus**: To enable the PingAck-Sus mechanism, we make use of MP1 code to distribute and signal all the machines to set Suspicion to True. 


> `>$ cd MP1/`
> `>$ cd Parser`
>
>`>$ go run . -cmd "sus" -o "" -s "test"`



>**PS** : This will toggle the suspicion flag, running this again will disable the Suspicion mechanism.


>**PS** : Before running this, ensure that Listeners are up in all the machines ! 


**Machine Problem 3** - **Distributed File System (built upon Machine Problem 2 - distributed group membership and failure detection** 


**Code Instructions**


- **Start the Introducer**: Whichever machine is chosen to be the introducer, perform following on that node (by default, VM Node 1)


>`>$ cd MP3/`
>
>`>$ go run .`


**Start the other Nodes to join the system**: Run the same command on other nodes, and use the cli prompts to see the membership list at any node, at any time !


>`>$ cd MP3/`
>
>`>$ go run .`

** Follow the prompts when entering each command in the CLI (create, get, append, merge, ls, store) and input the values in the format that it expects. **

**Project Structure**


>**File Transfer Package** :
> This contains the file transfer logic between nodes in the distributed file system.


>**Ring Package** : 
> This contains the code for the Chord ring structure, and replication of data upon node rejoin / leave.


>**Cache Package** : 
> This contains code for the caching mechanism for the files which have been previously fetched to the local file system of the node.






