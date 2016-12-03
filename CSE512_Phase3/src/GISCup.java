import java.io.*;
import java.util.Map.Entry;

import org.apache.spark.api.java.*;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import java.util.*;

import scala.Tuple2;
public class GISCup implements Serializable{
	
	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	private JavaSparkContext sc;
	private JavaRDD<ArrayList<String>> records = null;
	
	public GISCup(JavaSparkContext sc, String inputFile, String outputFile) throws FileNotFoundException, UnsupportedEncodingException
	{
		this.sc = sc;
		this.readFile(inputFile);
		this.sortRecords(outputFile);
		//this.results(outputFile);
	}
	
	private static Function mapfunc = new Function<String, ArrayList<String>>() {

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
	
	
	public void readFile(String inputFile)
	{
	    records = sc.textFile(inputFile).map(mapfunc);
	}
	
	double X_avg = 0.0;
	double X_avg_square = 0.0;
	double n = 55 * 40 * 31;
	double X_bar = 0.0;
	double S = 0.0;
	public void sortRecords(String outputFile) throws FileNotFoundException, UnsupportedEncodingException
	{
		JavaPairRDD<String, Integer> mapped_data = records.mapToPair(normalize);
		JavaPairRDD<String, Integer> filtered_data = mapped_data.filter(filterNullVal);
		//System.out.println("Count of filtered_data is : "+filtered_data.count());
		Map<String, Integer> grouped_data1 = filtered_data.reduceByKeyLocally(reducingMethod);
		HashMap<String,Integer> grouped_data = new HashMap<>(grouped_data1);
		//System.out.println("Count of grouped_data is : "+grouped_data.size());
		for(Entry<String,Integer> entry : grouped_data.entrySet())
		{
			int temp = entry.getValue();
			X_avg += temp;
			X_avg_square += temp * temp;
		}
		X_bar = (double) X_avg / n;
		S = Math.sqrt((X_avg_square / n) - (X_bar * X_bar));
		TreeMap<Double,String> zscore = new TreeMap<>();
		int a=0,b=0,c=0,d=0;
		for(Entry<String,Integer> entry : grouped_data.entrySet())
		{
			String key = entry.getKey();
			double Gstar = 0.0;
			double sum = 0.0;
			int count = 0;
			for(int i=-1;i<2;i++)
			{
				for(int j=-1;j<2;j++)
				{
					for(int k=-1;k<2;k++)
					{
						String parts[] = key.split(",");
						int newdate = Integer.parseInt(parts[0]) + i;
						int newlat = Integer.parseInt(parts[1]) + j;
						int newlong = Integer.parseInt(parts[2]) + k;
						String newkey = newdate + "," + newlat + "," + newlong;
						if(grouped_data.containsKey(newkey))
						{
							sum += (int)grouped_data.get(newkey);
							count++;
						}
						else
						{
							if(!((newdate < 1 || newdate > 31) || (newlat < 4050 || newlat > 4090) || (newlong < 7370 || newlong > 7425)))
							{
								count++;
							}
						}
					}
				}
			}
			if(count == 27)
			{
				a++;		
			}
			else if(count == 18)
			{
				b++;		
			}
			else if(count == 12)
			{
				c++;		
			}
			else if(count == 8)
			{
				d++;		
			}
			double num = sum - X_bar * count;
			double den = S * Math.sqrt((n * count - count * count) / (n-1));
			Gstar = num / den;
			zscore.put(Gstar, key);
		}
		//System.out.println("The number of 27 neighbored cells are: "+a);
		//System.out.println("The number of 18 neighbored cells are: "+b);
		//System.out.println("The number of 12 neighbored cells are: "+c);
		//System.out.println("The number of 8 neighbored cells are: "+d);
		Map<Double,String> sortedzscore = zscore.descendingMap();
		int ct = 0;
		PrintWriter writer = new PrintWriter(outputFile, "UTF-8");
		Set<Double> sortedkeys = sortedzscore.keySet();
		for(Double value : sortedkeys)
		{
			if(++ct == 51)
			{
				break;
			}
			String temporary = sortedzscore.get(value);
			String temp[] = temporary.split(",");
			int date = Integer.parseInt(temp[0]);
			int latitude = Integer.parseInt(temp[1]);
			int longitude = Integer.parseInt(temp[2]);
			System.out.print("Key: ");
			System.out.println(date+"--"+latitude+"--"+longitude);
			System.out.println("Value: "+value);
			System.out.println("******************");
			double final_latitude = latitude / 100.00;
			double final_longitude = longitude / 100.00;
			writer.print(final_latitude+","+(0-final_longitude)+","+date+","+value+"\n");
		}
		writer.close();
		/*for(Tuple2<ArrayList<Integer>, Integer> temp : attribute_data.take(1000))
	    {
	    	System.out.println(temp._1.get(0) + "--"+ temp._1.get(1) + "--"+ temp._1().get(2));
	    	System.out.println("Size of the cell is "+temp._2);
	    	
	    }*/			
	}    
    
    
	static Function2 reducingMethod = new Function2<Integer, Integer, Integer>() {

		@Override
		public Integer call(Integer arg0, Integer arg1) {
			// TODO Auto-generated method stub
			return arg0 + arg1;
		}
		
	};
	
	
	
    static PairFunction<ArrayList<String>, String, Integer> normalize = new PairFunction<ArrayList<String> , String, Integer>() {
		  public Tuple2<String, Integer> call(ArrayList<String> row) {
			    int date, longitude_cell, latitude_cell;
			    double longitude_conv, latitude_conv;
			    String transformed_row = null;
			  
				//date
				date = Integer.parseInt(row.get(0).split(" ")[0].split("-")[2]);
				
				//Longitude cell
				longitude_conv =  Math.ceil ((Double.parseDouble(row.get(1)) * 100.0));
				longitude_cell = Math.abs((int)longitude_conv);
				
				//Latitude cell
				latitude_conv =  Math.floor ((Double.parseDouble(row.get(2)) * 100.0));
				latitude_cell = Math.abs((int)latitude_conv);
				
				
				if (date < 1 || date > 31 || longitude_cell < 7370 || longitude_cell > 7425 ||
						latitude_cell < 4050 || latitude_cell > 4090)
						return null;
			
				else{
					transformed_row = date + "," + latitude_cell + "," + longitude_cell;

					return new Tuple2(new String(transformed_row), 1);
				}
		  }
	};
		
	
    // Filter null transformed row
	static Function<Tuple2<String, Integer>, Boolean> filterNullVal = new Function<Tuple2<String, Integer>, Boolean>() {
		public Boolean call(Tuple2<String, Integer> row) throws Exception {
	    	   	return row != null;
	    	   }
    };
}

