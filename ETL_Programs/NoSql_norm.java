//package Assignment2;
/** Program Description
 *   -   Provide a Java process to extract the previous data from MySQL 
 *   into one or more collections in MongoDB.
 *  Normalized
 *  Author : Ritvik Joshi
 */
import java.sql.*;
import com.mongodb.MongoClient;
import com.mongodb.MongoException;
import com.mongodb.WriteConcern;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.BasicDBObject;
import com.mongodb.DBObject;
import com.mongodb.DBCursor;
import com.mongodb.ServerAddress;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.*;

public class NoSql_norm {

	public static void main(String args[]){
		try{
			
	         // To connect to mongodb server
	         MongoClient mongoClient = new MongoClient( "localhost");
				
	         // Now connect to your databases
	         DB db = mongoClient.getDB( "test" );
	         System.out.println("Connect to database successfully");
	         
	         
	         
	         DBCollection coll = db.getCollection("imdb");
	         DBCollection coll2 = db.getCollection("imdb1");
	         System.out.println("Collection imdb created and selected successfully");
	         coll.drop();
	         coll = db.getCollection("imdb");
	         coll2 = db.getCollection("imdb1");
	        
	         ResultSet actor_data=fetch_actor_data();
	         
	         while(actor_data.next()){
	        	 String movies = actor_data.getString(5);
	        	 String buffer[] = movies.split(",");
	        	 
		         BasicDBObject doc = new BasicDBObject("title", "Actor").
		                 append("actor_id",actor_data.getInt("id") ).
		                 append("first_name",actor_data.getString("first_name")).
		                 append("last_name",actor_data.getString("last_name")).
		                 append("gender", actor_data.getString("gender")).
		                 append("movies_id",buffer);
		     				
		         coll2.insert(doc);
		         System.out.println("Document inserted successfully");
	         }
	         
	         ResultSet movie_data = fetch_movie_dir_data();
	         while(movie_data.next()){
	        	 String genre = movie_data.getString(4);
	        	 String g_buffer[] = genre.split(",");
	        	 
	        	 String dir_id = movie_data.getString(5);
	        	 String dir_id_buffer[] = dir_id.split(",");
	        	 int [] dir = new int[dir_id_buffer.length];
	        	 for(int i=0; i<dir_id_buffer.length;i++){
	        		 dir[i]=Integer.parseInt(dir_id_buffer[i]);
	        	 }
	        	 
	        	 String dir_f_name = movie_data.getString(6);
	        	 String dirf_buffer[] = dir_f_name.split(",");
	        	 
	        	 String dir_l_name = movie_data.getString(7);
	        	 String dirl_buffer[] = dir_l_name.split(",");
	        	 
	        	 BasicDBObject []dir_doc= new BasicDBObject[dir_id_buffer.length]; 
	        	 for(int i=0;i<dir_id_buffer.length;i++){
	        		 	 dir_doc[i] = new BasicDBObject("dir_id",dir[i] ).
	        			 append("fname",dirf_buffer[i]).
	        			 append("lname",dirl_buffer[i]);
	        	 }
	        	 
	        	 
	        	 
		         BasicDBObject doc = new BasicDBObject("title", "Movie").
		                 append("movie_id",movie_data.getInt(1) ).
		                 append("name",movie_data.getString(2)).
		                 append("year",movie_data.getString(3)).
		                 append("genre", g_buffer).
		                 append("director",dir_doc);
		     				
		         coll.insert(doc);
		         System.out.println("Document inserted successfully");
	         }
	        
	         
	      }catch(Exception e){
	         System.err.println( e.getClass().getName() + ": " + e.getMessage() );
	      }
	}
	
	public static ResultSet fetch_movie_dir_data(){
		try {
			//Setting class path 	
			Class.forName("com.mysql.jdbc.Driver");
			
			//Establishing connection with mysql server
			//imdb -databse name, root- username, Ritvik-password
			Connection con=DriverManager.getConnection("jdbc:mysql://localhost:3306/imdb","root","Ritvik");  
			
			Statement stmt=con.createStatement();
			
			ResultSet rs=stmt.executeQuery("Select m.id,m.name,m.year,group_concat(Distinct mg.genre),group_concat(Distinct d.id), "+
											"group_concat(distinct d.first_name),group_concat(distinct d.last_name) "+ 
											"from imdb.movies as m Join imdb.movies_genres as mg "+
											"On m.id=mg.movie_id "+ 
											"Join imdb.movies_directors as md "+ 
											"On mg.movie_id = md.movie_id "+
											"Join directors as d "+
											"On md.director_id=d.id "+
											"group by m.id Limit 100");
			
			//con.close();
			return rs;
			
			} catch (ClassNotFoundException | SQLException e) {
				// TODO Auto-generated catch block
				System.out.println(e);
			}
		
		return null;
	}

	public static ResultSet fetch_actor_data(){
		try {
			//Setting class path 	
			Class.forName("com.mysql.jdbc.Driver");
			
			//Establishing connection with mysql server
			//imdb -databse name, root- username, Ritvik-password
			Connection con=DriverManager.getConnection("jdbc:mysql://localhost:3306/imdb","root","Ritvik");  
			
			Statement stmt=con.createStatement();
			
			ResultSet rs=stmt.executeQuery("Select a.id,a.first_name,a.last_name,a.gender,group_concat(r.movie_id) "+
											"from imdb.actors as a Join imdb.roles as r "+
											"On a.id= r.actor_id group by a.id Limit 100000");
			
			//con.close();
			return rs;
			
			} catch (ClassNotFoundException | SQLException e) {
				// TODO Auto-generated catch block
				System.out.println(e);
			}
		return null;

		
	}

}
