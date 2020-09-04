# Map-Reduce-Program
This program contains Map-Reduce Program.


Input File
  1. Generate HDFS input file from the python file. It will generate a file of about size 1.3GB.
  
Input FIle Format:
 txid: p1 p2 p3 where txid is transaction id and pi denotes price.
 

Mapper Function- WordCount
  1. Generates 5 partitions based on word count.
  2. Partition done on the basis of Total Price. For e.g. : bin 1 contains transaction ids along with total price, total number of items 
  s.t. the item count is between 1-10, bin2 for 11-20, and so on till bin5 having 41-50 item count
  
Reducer:
  1. The Reduce task will add up the values from output of Mapper function. Once the reduce tasks are executed parallelly, we combine the results 
to get our output file. Each partion has differnet reducers. In this case, we have 5 reducers.


Result:
We have our output file with the word count for each partition.
 
  
