Please find attached in the root of the project the jar newday_2.10-0.0.1.jar, with has been run in clouderas oficial virtual machine with belows command (The command is the basic one, without any complication).

spark-submit --class com.test.newday.core.Driver \
--master yarn \
newday_2.10-0.0.1.jar \
/user/cloudera/newday/movies.dat \
/user/cloudera/newday/ratings.dat \
hdfs:///user/cloudera/newday/parquet/movies/ \
hdfs:///user/cloudera/newday/parquet/ratings/ \
hdfs:///user/cloudera/newday/parquet/moviesRatings/ \
hdfs:///user/cloudera/newday/parquet/usersWith3Films/

Notice the arguments:
      - Source data for movies.
      - Source data for ratings.
      - Output for movies (exercise 1)
      - Output for ratings (exercise 1)
      - Output for movies with their average ratings (exercise 2)
      - Output for users with the 3 films that are rated best. (exercise 3)
      
****************************************
****************************************
OLD NOTES BELOW
****************************************
****************************************
      
Being an exercise of hour, I assume that there was not important the quality of the code and I did in an easiest way that I could.

Point of improvement.

- Parametrize the spark configuration in order to execute in local or cluster mode as required. Or modify the configuration parameters with the content of the configuration file. Currently always is executed in local. As in the code the parameter of Spark is overwritten.
- Remove duplicate code by creating a correct architecure with a trait for the exercise one, that has duplicated code to create three tables, providing a functional code.
- Split different responsabilities in diferent classes.
- Create some test with a little sample of data.
- Find a way in exercise 3, of selecting the 3 items needed, without ordering all the list. Although I have found that the logical plan, from spark 1.5, has this situation under control and only select the first 3 item.

Lets speak in the interview about this issues.
