/*
 * Dian.com Inc.
 * Copyright (c) 2004-2017 All Rights Reserved.
 */
package flink;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.DataSink;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;

/**
 *
 * @author ${wanyan}
 * @version $Id: HadoopWordCount.java, v 0.1 2017年08月04日
 */
public class HadoopWordCount {

    private final static String HADOOP_ADDRESS = "hdfs://101.201.122.113:9000/";

    public static void main(String[] args) throws Exception {

        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        //ExecutionEnvironment env = ExecutionEnvironment.createRemoteEnvironment("101.201.122.113", 6123);

        // read text file from a HDFS running at nnHost:nnPort
        DataSet<String> text = env.readTextFile(HADOOP_ADDRESS + "test_1.txt");

        System.out.println(text.toString());

        DataSet<Tuple2<String, Integer>> counts =
                // split up the lines in pairs (2-tuples) containing: (word,1)
                text.flatMap(new LineSplitter())
                        // group by the tuple field "0" and sum up tuple field "1"
                        .groupBy(0)
                        .sum(1);

        // execute and print result

        counts.writeAsText(HADOOP_ADDRESS + "hadoop_out");

        env.execute("read from hadoop");

    }

    /**
     * Implements the string tokenizer that splits sentences into words as a user-defined
     * FlatMapFunction. The function takes a line (String) and splits it into
     * multiple pairs in the form of "(word,1)" (Tuple2<String, Integer>).
     */
    public static final class LineSplitter implements FlatMapFunction<String, Tuple2<String, Integer>> {

        @Override
        public void flatMap(String value, Collector<Tuple2<String, Integer>> out) {
            // normalize and split the line
            String[] tokens = value.toLowerCase().split("\\W+");

            // emit the pairs
            for (String token : tokens) {
                if (token.length() > 0) {
                    out.collect(new Tuple2<String, Integer>(token, 1));
                }
            }
        }
    }
}