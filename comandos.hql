pscp -i "C:\Users\ROG\Downloads\llave-cluster.ppk" "C:\Users\ROG\Desktop\big data\indice invertido\secundarios\output_part_0.txt" hadoop@ec2-54-166-255-147.compute-1.amazonaws.com:/home/hadoop/


hdfs dfs -mkdir -p /user/hadoop/wordcount

hdfs dfs -put output_part_0.txt /user/hadoop/wordcount/

hive



CREATE EXTERNAL TABLE word_lines (
  line STRING
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '\n'
STORED AS TEXTFILE
LOCATION '/user/hadoop/wordcount/';


SELECT word, COUNT(*) as total
FROM (
  SELECT explode(split(line, ' ')) as word
  FROM word_lines
) tmp
GROUP BY word
ORDER BY total DESC
LIMIT 20;
