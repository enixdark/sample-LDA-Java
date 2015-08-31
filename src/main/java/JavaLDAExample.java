import scala.Function1;
import scala.Tuple1;
import scala.Tuple2;
import scala.collection.Seq;
import scala.collection.mutable.ArrayBuffer;
import scala.collection.mutable.ListBuffer;
import scala.collection.immutable.Map;
import org.apache.spark.api.java.*;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.mllib.clustering.DistributedLDAModel;
import org.apache.spark.mllib.clustering.LDA;
import org.apache.spark.mllib.clustering.LDAModel;
import org.apache.spark.mllib.linalg.Matrix;
import org.apache.spark.mllib.linalg.Vector;
import org.apache.spark.mllib.linalg.Vectors;
import java.util.List;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import org.apache.spark.SparkConf;
import java.util.*;
import scala.collection.JavaConverters;
import scala.collection.JavaConversions;


class Content implements Function<String, List<String>>, Serializable {
	public List<String> call(String content) throws Exception {
        String[] string_arrays = content.toLowerCase().split("\\s");
        return Arrays.asList(string_arrays);
    }
}




public class JavaLDAExample {
  public static void main(String[] args) {
	   // Override the default port
	  // Create a SparkContext with this configuration
    SparkConf conf = new SparkConf().setAppName("LDA Example");
    conf.set("spark.app.name", "My Spark App");
	conf.set("spark.master", "local[2]");
	conf.set("spark.ui.port", "36000");
    JavaSparkContext sc = new JavaSparkContext(conf);
    // Load and parse the data
//    String path = "/home/thuy/sample.txt";
    String path = "/home/thuy/mini_newsgroups/sci.space/60191.txt";
    JavaRDD<String> data = sc.wholeTextFiles(path).map(
    		new Function<Tuple2<String,String>, String>() {
				@Override
				public String call(Tuple2<String, String> t) throws Exception {
					// TODO Auto-generated method stub
					return t._2;
				}
			}
    );
    

    
    JavaRDD<List<String>> corpus = data.map(new Content());
    corpus.cache();
    ArrayList<List<String>> lists = new ArrayList<List<String>>();
    
    for(List<String> item: corpus.collect()){
    	if( item.size() > 3){
    		List<String> tmp = sc.parallelize(item).filter(new Function<String, Boolean>() {
				@Override
				public Boolean call(String s) throws Exception {
					// TODO Auto-generated method stub
					return s.length() > 3 && s.matches("[A-za-z]+");
				}
			}).collect();
	    	lists.add(tmp);
    	}
    }
    

//    
    JavaRDD<List<String>> corpuss = sc.parallelize(lists);
    List<Tuple2<String, Long>> termCounts = corpuss.flatMap(
    	new FlatMapFunction<List<String>, String>() {
    		public Iterable<String> call(List<String> list) {
    			return list;
        }
    })
    .mapToPair(
    	new PairFunction<String, String, Long>() {
    		public Tuple2<String, Long> call(String s) {
    			return new Tuple2<String, Long>(s,1L);
    		}
    })
    .reduceByKey(
    	new Function2<Long, Long, Long>() {
	    public Long call(Long i1, Long i2) {
	      return i1 + i2;
	    }
	})
	.collect();
    
    Collections.sort(termCounts, new Comparator<Tuple2<String, Long>>(){
		@Override
		public int compare(Tuple2<String, Long> v1, Tuple2<String, Long> v2) {
			// TODO Auto-generated method stub
			return (int)(v2._2 - v1._2);
	}});

    
    
    
    
    int numStopwords = 20;
    List<String> vocabArray = new ArrayList<String>();
    for(int i = termCounts.size() - 1 ; i >  numStopwords - 1; i-- ){
    	vocabArray.add(termCounts.get(i)._1);
    }
    
//    
    final HashMap<String, Long> vocab = new HashMap<String, Long>();
    for(Tuple2<String,Long> item: sc.parallelize(vocabArray).zipWithIndex().collect()){
    	vocab.put(item._1, item._2);
    }
    

//    
    JavaRDD<Tuple2<Long, Vector>> documents = corpuss.zipWithIndex().map(
		new Function<Tuple2<List<String>,Long> , Tuple2<Long,Vector>>() {
			@SuppressWarnings("unchecked")
			@Override
			public Tuple2<Long,Vector> call(Tuple2<List<String>, Long> t) throws Exception {
				HashMap<Long, Double> counts = new HashMap<Long, Double>(0);
				for(String item: t._1){
					if (vocab.containsKey(item)) {
				        Long idx = vocab.get(item);
				        if(!counts.containsKey(idx)){
				        	counts.put(idx,0.0);
				        }
				        counts.put(idx,counts.get(idx) + 1.0);
				    }
				}
				
				
//				JavaRDD<Vector<Tuple2<Integer,Double>>> parsedData 
						
				int[] key = new int[counts.size()+1];
				double[] value = new double[counts.size()+1];
				
				int i = 0;
				ArrayList<Tuple2<Integer,Double>> c = new ArrayList<>();
				for(Long item: counts.keySet()){
//					c.add(new Tuple2(item.intValue(), counts.get(item)));
					key[i] = item.intValue();
					value[i] = counts.get(item);
					i++;
					
				}
				return new Tuple2(t._2, Vectors.sparse(vocab.size(), key, value));
			}
		}	
    );
    
    System.out.println(documents.collect());

//    
    JavaPairRDD<Long, Vector> cor = JavaPairRDD.fromJavaRDD(documents);
    
    int numTopics = 10;

    DistributedLDAModel ldaModel = (DistributedLDAModel) new LDA().setK(10).run(cor);
    double avgLogLikelihood = ldaModel.logLikelihood() / documents.count();
    Matrix topics = ldaModel.topicsMatrix();
    int maxTermsPerTopic = 10;
    Tuple2<int[], double[]>[] topicIndices = ldaModel.describeTopics(maxTermsPerTopic);

	for(Tuple2<int[], double[]> topic: topicIndices){
		System.out.println("TOPIC:");
		int[] terms = topic._1;
		double[] termWeights = topic._2;
		for(int i = 0 ; i < terms.length; i++){
			System.out.println(vocabArray.get(terms[i]) + "\t" + termWeights[i]);
		}
	}
    
  }
}

