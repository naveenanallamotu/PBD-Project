import java.io.IOException;
import javax.servlet.ServletException;
import javax.servlet.annotation.WebServlet;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.net.URL;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.api.java.JavaSQLContext;
import org.apache.spark.sql.api.java.JavaSchemaRDD;
import org.apache.spark.sql.api.java.Row;
import com.ibm.watson.developer_cloud.alchemy.v1.AlchemyLanguage;
import com.ibm.watson.developer_cloud.alchemy.v1.model.DocumentSentiment;

/**
 * Servlet implementation class TwitterAnalysis
 */
public class TwitterAnalysis extends HttpServlet {
	private static final long serialVersionUID = 1L;
       
    /**
     * @see HttpServlet#HttpServlet()
     */
	URL url = getClass().getResource("tweet_output.txt");
    String pathToFile = url.toString();
    
    public TwitterAnalysis() {
        super();
        // TODO Auto-generated constructor stub
    }

	/**
	 * @see HttpServlet#doGet(HttpServletRequest request, HttpServletResponse response)

	 */
	protected void doGet(HttpServletRequest request, HttpServletResponse response) throws ServletException, IOException {
		// TODO Auto-generated method stub
		int option = Integer.parseInt(request.getParameter("option"));

		switch(option)
			{
			case 1: 
					query1();
					response.sendRedirect("query5.html");
					break;
			case 2: query2();
					response.sendRedirect("q2.html");
					break;
			case 3: query3();
					response.sendRedirect("q3.html");
					break;
			case 4: query4();
					response.sendRedirect("q5.html");
					break;
			case 5: query5();
					response.sendRedirect("q6.html");
					break;
			default: //JOptionPane.showMessageDialog(null, "Invalid Option");
					query1();
					response.sendRedirect("q4.html");
					break;
			}
	}

	/**
	 * @see HttpServlet#doPost(HttpServletRequest request, HttpServletResponse response)
	 */
	protected void doPost(HttpServletRequest request, HttpServletResponse response) throws ServletException, IOException {
		// TODO Auto-generated method stub
		int option = Integer.parseInt(request.getParameter("option"));

		  switch(option)
			{
			case 1: query1();		       
					response.sendRedirect("query5.html");
					break;
					
			case 2: query2();
					response.sendRedirect("q2.html");
					break;
			
			case 3: query3();
					response.sendRedirect("q3.html");
					break;
					
			case 4: query4();
					response.sendRedirect("q5.html");
					break;
			
			case 5: query5();
					response.sendRedirect("q6.html");
					break;
	
			default: //JOptionPane.showMessageDialog(null, "Invalid Option");
					query1();
					response.sendRedirect("q4.html");
					break;
			}
	}
	
	
	public void query1() {
		// TODO Auto-generated method stub
		 SparkConf conf = new SparkConf().setAppName("User mining").setMaster("local[*]");
		 conf.set("spark.driver.allowMultipleContexts", "true");
		 JavaSparkContext sc = new JavaSparkContext(conf);
		 JavaSQLContext sqlContext = new JavaSQLContext(sc);
		 JavaSchemaRDD tweets = sqlContext.jsonFile(pathToFile);
		 tweets.registerAsTable("tweetTable");
		 tweets.printSchema();
		 query1PosNeg(sqlContext);
		 sc.stop();
		
	}
	
	protected void query1PosNeg(JavaSQLContext sqlContext) {
		// TODO Auto-generated method stub
	    try
		{

	    		File outputFile = new File(getServletContext().getRealPath("/")
			            + "/q10.JSON");
	    		FileWriter fw= new FileWriter(outputFile);
	    		JavaSchemaRDD tweetdata = sqlContext.sql("SELECT DISTINCT text FROM tweetTable ");
	    		String textTweet = tweetdata.toString();
	    		System.out.println(textTweet);
	    		AlchemyLanguage service = new AlchemyLanguage();
	    		service.setApiKey("29131b737a57f19004e3728cb2c16900bd598919");
	    		Map<String,Object> params = new HashMap<String, Object>();
	    		params.put(AlchemyLanguage.TEXT, textTweet);
	    		DocumentSentiment sentiment = service.getSentiment(params).execute();
	    		System.out.println(sentiment);
	    		String sentimentData = sentiment.toString();
	    		fw.write(sentimentData);
	    		fw.close();
		}
		catch (Exception exp)
		{
		  }
	
		
	}	
	

	
	public void query2()
	{
		SparkConf conf = new SparkConf().setAppName("User mining").setMaster("local[*]");

        JavaSparkContext sc = new JavaSparkContext(conf);

        JavaSQLContext sqlContext = new JavaSQLContext(sc);

        JavaSchemaRDD tweets = sqlContext.jsonFile(pathToFile);

        tweets.registerAsTable("tweetTable");

        tweets.printSchema();

        query2PopularUser(sqlContext);

       sc.stop();
		
	}
	private void query2PopularUser(JavaSQLContext sqlContext) {
		  
		  try
		  {
			  File outputFile = new File(getServletContext().getRealPath("/")+ "/q2.csv");
			
			  FileWriter fw= new FileWriter(outputFile);
	   
			  JavaSchemaRDD count = sqlContext.sql("SELECT DISTINCT  user.screen_name, user.followers_count AS c FROM tweetTable " +
	                                         "ORDER BY c");
	    
			  List<org.apache.spark.sql.api.java.Row> rows = count.collect(); 

	       		Collections.reverse(rows);
		    
	       		String rows123=rows.toString();
	    
	       		String[] array = rows123.split("],"); 
	
	       	fw.append("Name");
			fw.append(',');
			fw.append("Count");
			fw.append("\n");

			
			for(int i = 0; i < 8; i++)
			{
				if(i==0)
						{
						fw.append(array[0].substring(2));
						fw.append(',');
						fw.append("\n");
						System.out.println(array[0].substring(2));
						}
				else {
						fw.append(array[i].substring(2));
						fw.append(',');
		    			fw.append("\n");
		    			System.out.println(array[i].substring(2));
					}
				}
		
				fw.close();
		
		
		  	}
		  catch (Exception exp)
		  {
		  }
		  

	  }
	
	
	
	
	public void query3()
	{
		SparkConf conf = new SparkConf().setAppName("User mining").setMaster("local[*]");
    
    	JavaSparkContext sc = new JavaSparkContext(conf);
   
    	JavaSQLContext sqlContext = new JavaSQLContext(sc);
   
    	JavaSchemaRDD tweets = sqlContext.jsonFile(pathToFile);

    	tweets.registerAsTable("tweetTable");

    	tweets.printSchema();

    	query3MostActiveUser(sqlContext);

    	sc.stop();
 
	}

	
	private void query3MostActiveUser(JavaSQLContext sqlContext) {
		// TODO Auto-generated method stub
		 try
		 {
			 
			 File outputFile = new File(getServletContext().getRealPath("/")+"/q3.csv");
			
			 FileWriter fw= new FileWriter(outputFile);
	   
			 JavaSchemaRDD count = sqlContext.sql("SELECT DISTINCT user.name,user.statuses_count AS c FROM tweetTable " +
	    		                             "ORDER BY c");
	    
	    
			 List<org.apache.spark.sql.api.java.Row> rows = count.collect(); 

			 Collections.reverse(rows);
	    
			 String rows123=rows.toString();
	    
			 String[] array = rows123.split("],"); 
	    
			 fw.append("Name");
			 fw.append(',');
			 fw.append("Count");
			 fw.append("\n");
		
		

		for(int i = 0; i < 8; i++)
		{
			if(i==0)
			{
				fw.append(array[0].substring(2));
				fw.append(',');
				fw.append("\n");
			}
			else {
			fw.append(array[i].substring(2));
			fw.append(',');
			fw.append("\n");
			System.out.println(array[i].substring(2));
			}
		}
		
		fw.close();
		
	 }
		  catch (Exception exp)
		  {
		  }


	}
	
	
	
	public void query4()
	{
		 SparkConf conf = new SparkConf().setAppName("User mining").setMaster("local[*]");

         JavaSparkContext sc = new JavaSparkContext(conf);
         JavaSQLContext sqlContext = new JavaSQLContext(sc);

         JavaSchemaRDD tweets = sqlContext.jsonFile(pathToFile);

         tweets.registerAsTable("tweetTable");

         tweets.printSchema();

         query4TweetTimes(sqlContext);

         sc.stop();
	}
	
	private void query4TweetTimes(JavaSQLContext sqlContext) 
	 {		  
		  try
		  {
			  File outputFile = new File(getServletContext().getRealPath("/")
			            + "/q5.csv");
			
			  FileWriter fw= new FileWriter(outputFile);
	   
			  JavaSchemaRDD count = sqlContext.sql("SELECT created_at, COUNT(*) AS c FROM tweetTable " +
	    										"Group By created_at " +
	    		                                  "order by c" );
	    	   
			  List<org.apache.spark.sql.api.java.Row> rows = count.collect(); 
	    
			  Collections.reverse(rows);
	    
			  String rows123=rows.toString();
	    
			  String[] array = rows123.split("],"); 
	    
			  System.out.println(rows123);
	    
			  fw.append("Time");
			  fw.append(',');
			  fw.append("Count");
			  fw.append("\n");
		
		

			  for(int i = 0; i < 9; i++)
			  {
				  if(i==0)
				  {
					  continue;
				  }
				  else if(i == array.length-1)
				  {
					  fw.append(array[i].substring(2,array[i].length()-2));
					  fw.append(',');
					  fw.append("\n");
				  }
				  else {
					  fw.append(array[i].substring(2));
					  fw.append(',');
					  fw.append("\n");
				  }
		}
		
		fw.close();
		
		
	 }
		  catch (Exception exp)
		  {
		  }
		  

	  }

	
	public void query5()
	{
		 SparkConf conf = new SparkConf().setAppName("User mining").setMaster("local[*]");

         JavaSparkContext sc = new JavaSparkContext(conf);

         JavaSQLContext sqlContext = new JavaSQLContext(sc);

         JavaSchemaRDD tweets = sqlContext.jsonFile(pathToFile);

         tweets.registerAsTable("tweetTable");

         tweets.printSchema();

          query5TopLanguage(sqlContext);

          sc.stop();
	}
	
	 private void query5TopLanguage(JavaSQLContext sqlContext) {
		  
		  try
		  {
			  File outputFile = new File(getServletContext().getRealPath("/")
			            + "/q7.csv");
			
		FileWriter fw= new FileWriter(outputFile);
	    JavaSchemaRDD count = sqlContext.sql("SELECT lang, COUNT(*) AS c FROM tweetTable " +
	    										"Group By lang " +
	    		                                  "order by c"); 
	    List<org.apache.spark.sql.api.java.Row> rows = count.collect(); 
	    Collections.reverse(rows);
	    String rows123=rows.toString();
	    String[] array = rows123.split("],"); 
	    System.out.println(rows123);
	    fw.append("Language");
		fw.append(',');
		fw.append("Count");
		fw.append("\n");

		for(int i = 0; i < 9; i++)
		{
			if(i==0)
			{
				continue;
			}
			else if(i == array.length-1)
			{
				fw.append(array[i].substring(2,array[i].length()-2));
				fw.append(',');
				fw.append("\n");
			}
			else {
			fw.append(array[i].substring(2));
			fw.append(',');
			fw.append("\n");
			}
		}
		
		fw.close();
		
		
		  }
		  catch (Exception exp)
		  {
		  }
		  
	  }
	
	 
}
