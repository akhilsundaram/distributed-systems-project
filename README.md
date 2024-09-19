# G59

**CS425 Project** - **Distributed Log Parser**


| Akhil Sundaram  | Anurag Choudhary  |
| ------ | ------ |

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
