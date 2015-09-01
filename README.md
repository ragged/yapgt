# yapgt (the g is silent)
Yet Another PostgreSQL Tool

**This is not yet production ready, the master branch is even not giving any output**

### Introduction
Ever wanted to **easily** understand why your last database change or the last new query just blow up your database performance? Well, try yapgt (the g is silent).
Yapgt is an easy to use, htop like interface, to understand what is going on under the hood in PostgreSQL. Don't mess with too many different queries, don't mess with comparing different selects of statistic tables, you might never heard of before.
In my past years as Linux Ad

### Views
What does it offer?

#### Index / Sequential Reads (seq_idx)
* Getting the amount of index / sequential scans initiated for each table
* Getting the amount of rows received through index / sequentials scans (**Hello missing indexes**) for each table

#### Inserts Updates Deletes (ins_upd_del)
* How much **inserts** are going on which table
* How much **updates** are going on which table
* How much **deletes** are going on which table

#### Table IO (table_io) #TODO maybe wrong name
* How much do you get for each table out of the memory or from disc

#### Specific Index usage (table_idx)
* Which idx of a table gets read


### Requirements
* python
* python-urwid
* python-psycopg2
