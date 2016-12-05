# ACMCup
ACM SigSpatial GISCup 2016

Steps to run the code:

1. Run a spark shell from /bin supplying the given jar as a dependency using the --jars command.


Example:
./spark-shell --master spark://master:7077 --jars /home/user/group33_phase3.jar --executor-memory 2g

2. Copy paste the following line and then press enter to create an object of the class to run the program:

val temp = new GISCup(sc,"INPUT_PATH","OUTPUT_PATH");



Notes:

1. The input path can be either HDFS location or local file.
2. The input file MUST NOT contain headers in the first line.
3. The output path SHOULD BE a local path since we are using the Java FileWriter method to write the output to a file. (Apologize for inconvenience)
4. The GitHub link to our project is : https://github.com/sujayv/ACMCup
