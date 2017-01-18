==============================================================================================================================
CSCI54100 : CONCURENCY CONTROL TECHNIQUES
HADOOP DISTRIBUTED FILE SYSTEM
SRC : StrictTwoPhaseLocking.java
AUTHORS : KARTIK SOOJI, KUMUD BHAT, CURAM SUNDARAM NARASIMHAN 
==============================================================================================================================

Few classes and methods are implemented in the process. 

1) class StrictTwoPhaseLocking
2) void populateMaps()
3) class StrictTwoPhaseLockingMapper extends MapReduceBase implements Mapper
4) String readTeamData()
5) class StrictTwoPhaseLockingReducer extends MapReduceBase implements Mapper
6) void main()

============================
class StrictTwoPhaseLocking:
============================

	This Class file is the main class that encapsulates the rest of the implemenatation. The class contains hashmap variables 
for the data processing for the multiple trasactions 

====================
void populateMaps():
====================
	
	The method takes in the input from Input.csv where the data is read line by line and converted in the format that can 
be easily accepted by the mapper class for further processing of data.

=========================================================================
class StrictTwoPhaseLockingMapper extends MapReduceBase implements Mapper
=========================================================================
	One of the major part of the hadoop file system that extends the Master Mapper class where the data is read from the 
input and calls the reaTeamData method for the rest of the data for the trasaction for processing based on the team_id request
by the indivdual object. The returned value is then passed on to the reducer for updation of the data.

===================
String readTeamData():
===================

	This method accepts team_id as key. The team_id trasaction data needs to be fetched. However, the input file is open 
for all the other objects to access hence Readlock needs to be acquired before the data is read from the file. Here is a part
of the strict 2PL algorithm for locking on the data row. After the row is retrived the same is returned to mapper class. 

===============================================================================
class StrictTwoPhaseLockingReducer extends MapReduceBase implements Reducer ():
===============================================================================

	Another major class for the hadoop file system that extends the Master Reducer Class where the input sent for the mapper 
class as the <key,value> pairs is accepted and worked on the same. Here the other part of the strict 2PL algorithm is implemented
as the rank data is updated randomly based on the sequence of the Exclusive locks acquired.The results are updated in the output 
file internally by the hadoop framework.

============
void main():
============

	The main function has a minimum functionality here it just configures the job, Input, Output File path for the hadoop 
framework.

================
EXECUTION STEPS:
================

1) Install Hadoop2.6.0
2) Run ssh localhost
3) Run the DataNodes
4) Load the Data in DFS
5) Compile the Source from ~>/usr/local/hadoop

