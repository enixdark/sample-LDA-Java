package lda;

import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SQLContext;


public class LDACSV extends SQLContext {
	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	private String filename;
	
	
	
	

	public LDACSV(JavaSparkContext sparkContext) {
		super(sparkContext);
		// TODO Auto-generated constructor stub
		
	}
	
	public LDACSV(JavaSparkContext sparkContext, String filename) {
		super(sparkContext);
		// TODO Auto-generated constructor stub
		this.filename = filename;
		
	}
	
	
	
}
