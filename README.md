# Description
This is a project that imports a csv file into a SQLite databse.
The data in excel file is processed using pandas to match the data structure inside the database.
The system is designed for the max processing efficiency to process a large amount of data.

## System Design
* the system is designed to have two modules:
	1. a module to process and compare the csv file
	2. a module to import the data into the database
* due to a time constraint, there are still some modules missing:
	1. unit test module. 
		* Although the code is manually tested upon writing, a unit test file is necessary to be added for further testing
	2. log module.
		* A system log function is not added.
* there are still work left to do, for example, it doesn't check for the duplicated entries inside the CSV file. However, the goal of this project is to show the power of the Big data tools and a way for getting a better processing efficiency based on system architecture