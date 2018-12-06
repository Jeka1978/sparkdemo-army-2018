package taxi;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.storage.StorageLevel;
import scala.Tuple2;

import java.util.List;

/**
 * @author Evgeny Borisov
 */
public class TaxiMainJava {

    public static final String BOSTON = "boston";

    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setAppName("taxi java")
                .setMaster("local[*]");
        JavaSparkContext sc = new JavaSparkContext(conf);
        JavaRDD<String> rddLines = sc.textFile("data/taxi_order.txt");
        JavaRDD<taxi.TaxiOrder> orderRdd = rddLines
                .map(String::toLowerCase)
                .map(TaxiUtil::convertFromLine);

        orderRdd.persist(StorageLevel.MEMORY_AND_DISK());
        System.out.println("number of object: " + orderRdd.count());
        long bostonLongTripsAmount = orderRdd.filter(order -> order.getCity().equals(BOSTON))
                .filter(order -> order.getKm() > 10)
                .count();
        System.out.println("bostonLongTripsAmount = " + bostonLongTripsAmount);

        Double totalBostonKm = orderRdd
                .filter(order -> order.getCity().equals(BOSTON))
//                .map(TaxiOrder::getKm).reduce(Integer::sum)
                .mapToDouble(TaxiOrder::getKm).sum();
        System.out.println("totalBostonKm = " + totalBostonKm);

        JavaPairRDD<Integer, Integer> totalKmPerDriverIdRdd = orderRdd.mapToPair(order -> new Tuple2<>(order.getId(), order.getKm()))
                .reduceByKey(Integer::sum);
        JavaPairRDD<Integer, String> driversRdd = sc.textFile("data/drivers.txt").mapToPair(line -> {
            String[] data = line.split(", ");
            return new Tuple2<>(Integer.parseInt(data[0]), data[1]);
        });
        List<Tuple2<Integer, String>> results = driversRdd.join(totalKmPerDriverIdRdd)
                .mapToPair(tuple -> new Tuple2<>(tuple._2._2, tuple._2._1))
                .sortByKey(false)
                .take(3);
        System.out.println("The winners are:");
        results.forEach(tuple->{
            System.out.print("name: "+tuple._2()+" ");
            System.out.println("km: "+tuple._1());
        });


    }

}





