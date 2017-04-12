//package Assignment2;
/** Program Description
 *   -   Provide a Java process to extract the previous data from MySQL 
 *   into one or more collections in MongoDB.
 *  DE Normalized
 *  Author : Ritvik Joshi
 */
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import com.mongodb.BasicDBObject;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.MongoClient;

public class NoSQL_denorm {

	public static void main(String args[]){
		try{
			
	         // To connect to mongodb server
	         MongoClient mongoClient = new MongoClient( "localhost");
				
	         // Now connect to your databases
	         DB db = mongoClient.getDB( "test" );
	         System.out.println("Connect to database successfully");
	         
	         
	         
	         DBCollection coll = db.getCollection("imdb2");
	         System.out.println("Collection imdb created and selected successfully");
	         coll.drop();
	         coll = db.getCollection("imdb2");
	        
	         
	         int counter=0;
	         ResultSet movie_data = fetch_data();
	         while(movie_data.next()){
	        	 counter++;
	        	 System.out.println("data fetched"+counter);
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
	        	 if(counter==251){
	        		 System.out.println("B**********"+dir_id_buffer.length);
	        	 }
	        	 for(int i=0;i<dir_id_buffer.length;i++){
	        		 	 dir_doc[i] = new BasicDBObject("dir_id",dir[i] ).
	        			 append("fname",dirf_buffer[i]).
	        			 append("lname",dirl_buffer[i]);
	        	 }
	        	 
	        	 if(counter==251){
	        		 System.out.println("A********"+dir_id_buffer.length);
	        	 }
	        	
	        	  
	        	 
	        	 String act_name = movie_data.getString(8);
	        	 String actn_buffer[] = act_name.split(",");
	        	 
	        	 String actf_buffer[] = new String[actn_buffer.length];
	        	 String actl_buffer[] = new String[actn_buffer.length];
	        	 String act_gbuffer[] = new String[actn_buffer.length];
	        	 int [] act = new int[actn_buffer.length]; 
	        	 if(counter==251){
	        		 System.out.println("************"+actn_buffer.length);
	        	 }
	        	
	        	 
	        	 //System.out.println("************"+actn_buffer.length);
	        	 for(int i=0;i<actn_buffer.length;i++){
	        		// System.out.println(i);
	        		// System.out.println(actn_buffer[i]);
	        		 
	        		 String buff[] = actn_buffer[i].split("@");
	        		 //System.out.println("l"+buff.length);
	        		 if (buff.length<4){
	        			 act[i]=-1;
	        			 actf_buffer[i] = null;
		        		 actl_buffer[i] = null;	 
		        		 act_gbuffer[i] = null;
	        		 }else{
	        		 act[i] = Integer.parseInt(buff[0]);	 
	        		 actf_buffer[i] = buff[1];
	        		 actl_buffer[i] = buff[2];	 
	        		 act_gbuffer[i] = buff[3];
	        		 }
	        		 
	        	 }
	        	 
	        	 BasicDBObject []act_doc= new BasicDBObject[act.length]; 
	        	 for(int i=0;i<act.length;i++){
	        		 	 
	        		 	act_doc[i] = new BasicDBObject("act_id",act[i] ).
	        			append("fname",actf_buffer[i]).
	        			append("lname",actl_buffer[i]).
	        		 	append("gender",act_gbuffer[i]);
	        	 }
	        	
	        	 
	        	 
	        	 
	        	 
		         BasicDBObject doc = new BasicDBObject("title", "Movie").
		                 append("movie_id",movie_data.getInt(1) ).
		                 append("name",movie_data.getString(2)).
		                 append("year",movie_data.getString(3)).
		                 append("genre", g_buffer).
		                 append("director",dir_doc).
		                 append("actor", act_doc);
		     				
		         coll.insert(doc);
		         System.out.println("Document inserted successfully");
	         }
	        
	         
	      }catch(Exception e){
	         System.err.println( e.getClass().getName() + ": " + e.getMessage() );
	      }
	}
	
	public static ResultSet fetch_data(){
		try {
			//Setting class path 	
			Class.forName("com.mysql.jdbc.Driver");
			
			//Establishing connection with mysql server
			//imdb -databse name, root- username, Ritvik-password
			Connection con=DriverManager.getConnection("jdbc:mysql://localhost:3306/imdb","root","Ritvik");  
			
			Statement stmt=con.createStatement();
			
			ResultSet rs=stmt.executeQuery("Select m.id,m.name,m.year,group_concat(Distinct mg.genre), "+
					"group_concat(Distinct d.id),group_concat(distinct d.first_name),group_concat(distinct d.last_name), "+
					"group_concat(distinct a.id,'@',a.first_name,'@',a.last_name,'@',a.gender) "+ 
					"from imdb.movies as m Join imdb.movies_genres as mg "+
					"On m.id=mg.movie_id "+
					"Join imdb.movies_directors as md "+ 
					"On mg.movie_id = md.movie_id "+
					"Join directors as d "+
					"On md.director_id=d.id "+ 
					"Join imdb.roles as r "+
					"On m.id = r.movie_id "+
					"Join imdb.actors as a "+
					"On r.actor_id = a.id "+ 
					"group by m.id Limit 100000;");
			
			//con.close();
			return rs;
			
			} catch (Exception e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		
		return null;
	}

	
}
