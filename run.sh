hadoop jar MapReduceFinal.jar /data/task2/people_name_list.txt /data/task2/novels task1_out

hadoop jar MapReduceFinal.jar task1_out task2_out

hadoop jar MapReduceFinal.jar task2_out task3_out

hadoop jar MapReduceFinal.jar task3_out task4_inter task4_out

hadoop jar MapReduceFinal.jar /data/task2/people_name_list.txt task3_out task5_inter task5_out

hadoop jar MapReduceFinal.jar task4_out task6_1_out task6_1_partition task6_1_inter

hadoop jar MapReduceFinal.jar task5_out task6_2_out