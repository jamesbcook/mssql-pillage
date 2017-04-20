Purpose:

Connect to mssql server, list all databases.  Search each database looking for
columns that contain the following:
        pass
        ssn
        routing
        rtn
        address
        credit
        card
        cvv

If a column is matched a second query is executed, performing a count on the
amount of rows that exist in that table.  If the number of rows is greater than 0
it will reported and written to a file.

USAGE:

        ./mssql-pillage -server 127.0.0.1 -user sa -pass hunter2

        or

        ./mssql-pillage -inputFile hosts.txt -user Administrator -domain ACME -pass acme123 -output some-folder

Hosts need to be line seperated as in:
        127.0.0.1
        4.2.2.2
        8.8.8.8

Things that don't work

ARGS:
       threads
