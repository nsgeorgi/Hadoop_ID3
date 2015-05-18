# Hadoop_ID3
A distributed implementation of ID3 classification algorithm using Hadoop.

The repository has two separate projects. ID3 Cars and ID3 Wines. The former is based on Cars Data Set [1] while the latter on Wines Data Set [2].

The difference between these two is that Cars contain categorical attributes while Wines contain real-valued attributes, hence the need for discretization for ID3 to work (used binary discretization). The implementation executes a ten-fold cross validation. The input and test files are included in each sub-project.

The projects are ready to be imported into Eclipse. Hadoop 1.2.1 [3] was used. You may need to include the jar libraries of the hadoop package in the build options.

When the project is set up, press run and the results (precision, recall, f-measure) will be printed in the console.

[1] http://archive.ics.uci.edu/ml/datasets/Car+Evaluation 
[2] http://archive.ics.uci.edu/ml/datasets/Wine+Quality 
[3] http://apache.tsl.gr/hadoop/common/hadoop-1.2.1

Hadoop ID3 
Copyright (C) 2013 Nick Georgiadis
