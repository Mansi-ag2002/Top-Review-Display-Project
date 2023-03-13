
//Creating a text file containing movies reviews. (Can have any other domain) 

//Now, move the file to user directory from home directory. 

hdfs dfs -put /home/cloudera/movies.txt   /user/cloudera/movies.txt 

//Creating another text containing stop words and moving the file from user directory to home directory. 

hdfs dfs -put /home/cloudera/stopwords.txt   /user/cloudera/stopwords.txt

//Run the spark shell on cloudera. 

//On scala terminal running the following commands. 


//uploading the text file for further processing 
1. var rdd= sc.textFile("/user/cloudera/movies.txt") 

// mapping the words to get converted to lowercase. 
2. var rdd1= rdd.map(s=>s.toLowerCase)

// splitting each word using flatmap 
3. var rdd2= rdd1.flatMap(s=>s.split(" "))

//mapping the each word frequency to 1
4. var rdd3= rdd2.map(s=>(s,1))

// applying the reduceByKey function on rdd to display the frequency of each word in the document. 
5. var rdd4=rdd3.reduceByKey((u,v)=>(u+v))

// uploading the stop words textfile 
6. var rdd5= sc.textFile("/user/cloudera/stopwords.txt") 

7. var rdd6= rdd5.map(s=>s.toLowerCase)
8. var rdd7= rdd6.flatMap(s=>s.split(" "))
9. var rdd8= rdd7.map(s=>(s,1))
10. var rdd9=rdd8.reduceByKey((u,v)=>(u+v))

// subtracting the stop words from the movies review document to have the useful words in the document. 
11. var rdd10=rdd3.subtract(rdd9)

// now applying the reduceByKey function again to the frequency count.
12. var rdd11=rdd10.reduceByKey((u,v)=>(u+v))

//Now, applying the swap function to swap the place of word, it's frequency.
13. var rdd12= rdd11.map(_.swap)

// Applying the sortByKey function by setting the value to false such that they are sorted in descending order.
14. var rdd13=rdd12.sortByKey(false)

//Finally, displaying the top 5 reviews of movie. 
15. rdd13.top(5)
