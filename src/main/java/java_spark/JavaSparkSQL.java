
package java_spark;

import java.util.ArrayList;
import java.util.List;
import java.util.Arrays;
import java.util.Collections;
import java.io.Serializable;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;

import org.apache.spark.api.java.function.MapFunction;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

import org.apache.spark.sql.Encoder;
import org.apache.spark.sql.Encoders;

import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;

import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import spark03.sql.SparkProperties;

import static org.apache.spark.sql.functions.col;

public class JavaSparkSQL {
    public static class Person implements Serializable {
        private String name;
        private int age;

        public String getName() {
            return name;
        }

        public void setName(String name) {
            this.name = name;
        }

        public int getAge() {
            return age;
        }

        public void setAge(int age) {
            this.age = age;
        }
    }

    public static void main(String[] args) {
        SparkSession spark = SparkSession
                .builder()
                .appName("Java Spark SQL ")
                .master("local[*]")
                .config(SparkProperties.warehouse(), SparkProperties.warehouseDir())
                .getOrCreate();

//        testBasicDF(spark);

//        testDataSetCreation(spark);

//        runInferSchema(spark);

        runProgrammaticSchemaExample(spark);

        spark.stop();
    }

    /**
     * Test basic DataFrame.
     * Read json file.
     *
     * @param spark SparkSession
     */
    private static void testBasicDF(SparkSession spark) {
        Dataset<Row> df = spark.read().json("file:\\D:\\mycode1\\BigData\\hello_bigdata\\data\\people.json");

        df.show();

        df.printSchema();

        df.select("name").show();

        df.select(col("name"), col("age").plus(1)).show();

        df.groupBy("age").count().show();

        df.createOrReplaceTempView("people");

        Dataset<Row> sqlDF = spark.sql("select * from people");
        sqlDF.show();
    }

    private static void testDataSetCreation(SparkSession spark) {
        Person person = new Person();
        person.setName("Andy");
        person.setAge(32);

        Encoder<Person> personEncoder = Encoders.bean(Person.class);
        Dataset<Person> javaBeanDS = spark.createDataset(
                Collections.singletonList(person),
                personEncoder
        );
        javaBeanDS.show();


        Encoder<Integer> integerEncoder = Encoders.INT();
        Dataset<Integer> primitiveDS = spark.createDataset(
                Arrays.asList(1, 2, 3),
                integerEncoder
        );
        Dataset<Integer> transformedDS = primitiveDS.map(
                new MapFunction<Integer, Integer>() {
                    @Override
                    public Integer call(Integer value) throws Exception {
                        return value + 1;
                    }
                }, integerEncoder);

        transformedDS.collect();

        String path = "file:\\D:\\mycode1\\BigData\\hello_bigdata\\data\\people.json";
        Dataset<Person> peopleDS = spark.read().json(path).as(personEncoder);
        peopleDS.show();


    }

    private static void runInferSchema(SparkSession spark) {
        JavaRDD<Person> personJavaRDD = spark.read()
                .textFile("file:\\D:\\mycode1\\BigData\\hello_bigdata\\data\\people.txt")
                .javaRDD()
                .map(new Function<String, Person>() {
                    @Override
                    public Person call(String line) throws Exception {
                        String[] parts = line.split(",");
                        Person person = new Person();
                        person.setName(parts[0]);
                        person.setAge(Integer.parseInt(parts[1].trim()));
                        return person;
                    }
                });

        Dataset<Row> personDF = spark.createDataFrame(personJavaRDD, Person.class);

        personDF.createOrReplaceTempView("people");

        Dataset<Row> teenagersDF = spark.sql("select name from people where age between 13 and 19");

        Encoder<String> stringEncoder = Encoders.STRING();
        Dataset<String> teenagerNamesByIndexDF = teenagersDF.map(
                new MapFunction<Row, String>() {
                    @Override
                    public String call(Row row) throws Exception {
                        return "Name:" + row.getString(0);
                    }
                }, stringEncoder);

        teenagerNamesByIndexDF.show();

        Dataset<String> teenagerNamesByFiledDF = teenagersDF.map(
                new MapFunction<Row, String>() {
                    @Override
                    public String call(Row value) throws Exception {
                        return "Name :" + value.<String>getAs("name");
                    }
                }, stringEncoder);

        teenagerNamesByFiledDF.show();
    }


    private static void runProgrammaticSchemaExample(SparkSession spark) {
        JavaRDD<String> peopleRDD = spark.sparkContext()
                .textFile("file:\\D:\\mycode1\\BigData\\hello_bigdata\\data\\people.txt", 1)
                .toJavaRDD();

        String schemaString = "name age";

        List<StructField> fields = new ArrayList<>();

        for (String fieldName : schemaString.split(" ")) {
            StructField field = DataTypes.createStructField(fieldName, DataTypes.StringType, true);
            fields.add(field);
        }
        StructType schema = DataTypes.createStructType(fields);

        JavaRDD<Row> rowJavaRDD = peopleRDD.map(
                new Function<String, Row>() {
                    @Override
                    public Row call(String record) throws Exception {
                        String[] attributes = record.split(",");
                        return RowFactory.create(attributes[0], attributes[1].trim());
                    }
                }
        );

        Dataset<Row> peopleDataFrame = spark.createDataFrame(rowJavaRDD, schema);

        peopleDataFrame.createOrReplaceTempView("people");

        Dataset<Row> results = spark.sql("select name from people");

        Dataset<String> namesDS = results.map(new MapFunction<Row, String>() {
            @Override
            public String call(Row row) throws Exception {
                return "Name : " + row.getString(0);
            }
        }, Encoders.STRING());
        namesDS.show();

    }

}










