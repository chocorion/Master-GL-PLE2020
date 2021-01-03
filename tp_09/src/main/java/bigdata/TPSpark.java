package bigdata;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import org.apache.log4j.Logger;
import org.apache.log4j.Level;
import scala.Tuple2;
import scala.Tuple3;


public class TPSpark {
	public static void firstPart(JavaSparkContext context) {
		JavaRDD<String> cities = context.textFile("/raw_data/worldcitiespop.txt");
		System.out.println("Le nombre de partition est : " + cities.getNumPartitions());

		int numberOfExecutors = 5;
		System.out.println("There is " + numberOfExecutors + " executors !");
		cities = cities.repartition(numberOfExecutors);
		System.out.println("Le nombre de partition est : " + cities.getNumPartitions());

		JavaRDD<Tuple2<String, Integer>> pop = cities.map(line -> {
			String name = line.split(",")[1];
			String population = line.split(",")[4];

			return new Tuple2<String, Integer>(name, (population.length() == 0 || population.equals("Population"))? -1 : Integer.valueOf(population));
		});

		pop = pop.filter(tuple -> tuple._2() != -1);
		JavaPairRDD<Integer, Integer> categoryTuple = pop.mapToPair(tuple -> {
			new Tuple2<Integer, Integer>((int) Math.floor(Math.log10(tuple._2())) ,1);
		});
		categoryTuple = categoryTuple.reduceByKey((v1, v2) -> v1 + v2).sortByKey();

		for (Tuple2<Integer, Integer> value : categoryTuple.collect()) {
			System.out.printf("%5d -> %5d\n", value._1(), value._2());
		}
	}

	public static void secondPart(JavaSparkContext context) {
			//region_codes.csv
		JavaRDD<String> cities = context.textFile("/raw_data/worldcitiespop.txt");
		JavaRDD<String> regions = context.textFile("/raw_data/region_codes.csv");

		JavaPairRDD<String, String> citiesMapped = cities.mapToPair(line -> {
			String[] splitted = line.split(",");

			return new Tuple2<>(
					splitted[0].toLowerCase() + splitted[3],
					splitted[1]
			);
		});

		JavaPairRDD<String, String> regionsMapped = regions.mapToPair(line -> {
			String[] splitted = line.split(",");

			return new Tuple2<>(
					splitted[0].toLowerCase() + splitted[1],
					splitted[2].replaceAll("\"", "")
			);
		});

		citiesMapped.leftOuterJoin(regionsMapped);

	}

	// Country,City,AccentCity,Region,Population,Latitude,Longitude
	// zw,zvishavane,Zvishavane,07,79876,-20.3333333,30.0333333
	//ZW,10,"Harare"
	public static void main(String[] args) {
		Logger.getLogger("org").setLevel(Level.OFF);
		Logger.getLogger("akka").setLevel(Level.OFF);
		
		SparkConf conf = new SparkConf().setAppName("TP Spark");
		JavaSparkContext context = new JavaSparkContext(conf);




	}
	
}