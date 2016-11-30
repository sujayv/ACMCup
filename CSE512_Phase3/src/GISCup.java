import java.io.*;
import java.math.RoundingMode;
import java.net.URL;
import java.text.DecimalFormat;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.fs.Path;
import org.apache.spark.api.java.*;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFunction;

import com.univocity.parsers.common.*;
import com.univocity.parsers.common.processor.*;
import com.univocity.parsers.common.record.*;
import com.univocity.parsers.conversions.*;
import com.univocity.parsers.csv.*;
import java.util.*;

import org.apache.spark.SparkConf;

import scala.Tuple2;
public class GISCup implements Serializable{
	
	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	
	private JavaRDD<ArrayList<String>> fetchedrecords = null;
	private ArrayList<String> header = null; 
	private JavaSparkContext sc;
	
	public GISCup(JavaSparkContext sc)
	{
		this.sc = sc;
	}
	
	private static Function mapfunc = new Function<String, ArrayList<String>>() {
		
		/**
		 * 
		 */
		private static final long serialVersionUID = 1L;

		@Override
    	public ArrayList<String> call(String line) throws Exception {
    		String[] fields = line.split(",");
    		ArrayList<String> temp = new ArrayList<>();
    		temp.add(fields[1]);
    		temp.add(fields[5]);
    		temp.add(fields[6]);
    		return temp;
    	}
    };
	
	
	public void readFile()
	{
	    JavaRDD<ArrayList<String>> records = sc.textFile("hdfs://master:54310/yellow_tripdata_2015-01_shortened.csv").map(mapfunc);
	    System.out.println("Done loading csv file");
	    //System.out.println("The number of lines are"+records.count());
	    System.out.println("Press enter");
	    fetchedrecords = records.cache();
	    /*for(int i=0;i<fetchedrecords.count();i+=100000)
	    {
	    	for(ArrayList<String> tp : fetchedrecords.take(i))
    		{
	    		for(int j=0;j<tp.size();j++)
	    		{
	    			System.out.print(tp.get(j)+"--");
	    		}
	    		System.out.println();
    		}
	    }*/
	}
	
	double X_avg = 0.0;
	double X_avg_square = 0.0;
	double n = 55 * 40 * 31;
	double X_bar = 0.0;
	double S = 0.0;
	public void sortRecords()
	{
		//header = fetchedrecords.first();
		JavaRDD<ArrayList<String>> resultRows = fetchedrecords;
		JavaPairRDD<ArrayList<Integer>, ArrayList<String>> mapped_data = resultRows.mapToPair(transform);
		JavaPairRDD<ArrayList<Integer>, ArrayList<String>> filtered_data = mapped_data.filter(filterNullVal);
		JavaPairRDD<ArrayList<Integer>, Iterable<ArrayList<String>>> grouped_data = filtered_data.groupByKey();
		JavaPairRDD<ArrayList<Integer>, Integer> attribute_data = grouped_data.mapToPair(calculateAttribute);
		List<Tuple2<ArrayList<Integer>, Integer>> list = attribute_data.collect();
		HashMap<ArrayList<Integer>,Integer> keyvalue = new HashMap<>();
		for(Tuple2<ArrayList<Integer>, Integer> row : list)
		{
			keyvalue.put(row._1,row._2);
		}
		long attributesize = attribute_data.count();
		for(Tuple2<ArrayList<Integer>, Integer> temp : attribute_data.take((int)attributesize))
		{
			X_avg += temp._2;
			X_avg_square += X_avg * X_avg;
		}
		X_bar = X_avg / n;
		S = Math.sqrt((X_avg_square / n) - (X_bar * X_bar));
		TreeMap<Double,ArrayList<Integer>> zscore = new TreeMap<>();
		for(Tuple2<ArrayList<Integer>,Integer> tp: list)
		{
			double Gstar = 0.0;
			double sum = 0.0;
			int count = 0;
			for(int i=-1;i<2;i++)
			{
				for(int j=-1;j<2;j++)
				{
					for(int k=-1;k<2;k++)
					{
						ArrayList<Integer> tp1 = new ArrayList<>();
						tp1.add(tp._1.get(0)+i);
						tp1.add(tp._1.get(1)+j);
						tp1.add(tp._1.get(2)+k);
						if(keyvalue.containsKey(tp1))
						{
							sum += tp._2;
							count++;
						}
						else
						{
							int timestep = tp._1.get(0) + i;
							int longitude = tp._1.get(1) + j;
							int latitude = tp._1.get(2) + k;
							if(!(timestep < 1 && timestep > 31 && latitude < 0 && latitude > 40 && longitude < 0 && longitude > 55))
							{
								count++;
							}
						}
					}
				}
			}
			double num = sum - X_bar * count;
			double den = S * Math.sqrt((n * count - count * count) / (n-1));
			Gstar = num / den;
			zscore.put(Gstar, tp._1);
		}
		Map<Double,ArrayList<Integer>> sortedzscore = zscore.descendingMap();
		for(Double value : sortedzscore.keySet())
		{
			System.out.print("Key: ");
			ArrayList<Integer> temporary = sortedzscore.get(value);
			System.out.println(temporary.get(0)+"--"+temporary.get(1)+"--"+temporary.get(2));
			System.out.println("Value: "+value);
			System.out.println("******************");
			
		}
		/*for(Tuple2<ArrayList<Integer>, Integer> temp : attribute_data.take(1000))
	    {
	    	System.out.println(temp._1.get(0) + "--"+ temp._1.get(1) + "--"+ temp._1().get(2));
	    	System.out.println("Size of the cell is "+temp._2);
	    	
	    }*/	
	}
	
	
    
    // Filter Header
    /*static Function<ArrayList<String>, Boolean> filterFirstRow = new Function<ArrayList<String>, Boolean>() {
    	public Boolean call(ArrayList<String> line) {
    		      return (!line.get(1).equalsIgnoreCase(header.get(1)));
    	}
    };*/
    
    
    
    static PairFunction<ArrayList<String>, ArrayList<Integer>, ArrayList<String>> transform = new PairFunction<ArrayList<String> , ArrayList<Integer>, ArrayList<String>>() {
		  public Tuple2<ArrayList<Integer>, ArrayList<String>> call(ArrayList<String> row) {
			    int timeStep, longitude_cell, latitude_cell;
			    Double longitude_conv, latitude_conv;
			    ArrayList<Integer> transformed_row = new ArrayList<Integer>();
			  
				//TimeStep
				timeStep = Integer.parseInt(row.get(0).split(" ")[0].split("-")[2]);
				
				//Longitude cell
				longitude_conv =  Math.floor ((Double.parseDouble(row.get(1)) * 100.0)) / 100.0;
				longitude_cell = (int)((longitude_conv + 74.25)/0.01);
				
				//Latitude cell
				latitude_conv =  Math.floor ((Double.parseDouble(row.get(2)) * 100.0)) / 100.0;
				latitude_cell = (int)((latitude_conv - 40.50)/0.01);
				
				
				if (timeStep < 1 || longitude_cell < 0 || longitude_cell > 55 ||
						latitude_cell < 0 || latitude_cell > 40)
						return null;
			
				else{
					transformed_row.add(timeStep);
					transformed_row.add(longitude_cell);
					transformed_row.add(latitude_cell);

					return new Tuple2(transformed_row, row);
				}
		  }
	};
		
	
    // Filter null transformed row
	static Function<Tuple2<ArrayList<Integer>, ArrayList<String>>, Boolean> filterNullVal = new Function<Tuple2<ArrayList<Integer>, ArrayList<String>>, Boolean>() {
		public Boolean call(Tuple2<ArrayList<Integer>, ArrayList<String>> row) throws Exception {
	    	   	return row != null;
	    	   }
    };
    
    static PairFunction calculateAttribute = new PairFunction<Tuple2<ArrayList<Integer>, Iterable<ArrayList<String>>>,ArrayList<Integer>, Integer>() {

		@Override
		public Tuple2<ArrayList<Integer>, Integer> call(Tuple2<ArrayList<Integer>, Iterable<ArrayList<String>>> row)throws Exception 
		{
	    	Iterator<ArrayList<String>> it = row._2.iterator();
	    	int count = 0;
	    	while(it.hasNext())
	    	{
	    		it.next();
	    		count += 1;
	    	}
	    	return new Tuple2(row._1,count);
		}
    };
    

}

