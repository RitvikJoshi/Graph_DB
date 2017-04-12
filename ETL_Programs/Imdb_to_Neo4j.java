package Assigment3;

import java.io.Externalizable;
import java.io.File;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;


import org.neo4j.graphdb.*;



import org.neo4j.unsafe.batchinsert.*;



class actor{
    int actor_id;
    String firstName;
    String lastName;

    actor(int actor_id,String fname, String lname){
        this.actor_id=actor_id;
        this.firstName=fname;
        this.lastName=lname;

    }
}

class roles{
    int actor_id;
    int movie_id;
    String role;
    roles(int actor_id,int movie_id,String role ){
        this.actor_id = actor_id;
        this.movie_id=movie_id;
        this.role=role;
    }
}


class actors_movie{
    int actor_id;
    String movie_id[];
    actors_movie(int act_id,String movie_id[]){
        this.actor_id=act_id;
        this.movie_id=movie_id;
    }
}

class movie_roles{
    int movie_id;
    String roles[];
    movie_roles(int movie_id,String roles[]){
        this.movie_id=movie_id;
        this.roles=roles;
    }
}

class director{
    int dir_id;
    String firstName;
    String lastName;
    director(int dir_id,String fname, String lname){
        this.dir_id=dir_id;
        this.firstName=fname;
        this.lastName=lname;

    }
}


class movie{
    int movie_id;
    String title;
    int year;

    movie(int movie_id,String name,int year){
        this.movie_id=movie_id;
        this.title=name;
        this.year=year;

    }
}


public class Imdb_to_Neo4j {

    HashMap<Integer,actor> actor_ht= new HashMap<Integer,actor>();
    HashMap<Integer,String[]> actor_movie_ht= new HashMap<Integer,String[]>();
    HashMap<Integer,String[]> movie_roles_ht= new HashMap<Integer,String[]>();
    HashMap<Integer,movie> movie_ht= new HashMap<Integer,movie>();
    HashMap<Integer,String[]> movie_genre_ht= new HashMap<Integer,String[]>();
    HashMap<Integer,director> director_ht= new HashMap<Integer,director>();
    HashMap<Integer,String[]> director_genre_ht= new HashMap<Integer,String[]>();
    HashMap<Integer,String[]> director_movie_ht= new HashMap<Integer,String[]>();

    ArrayList<String> Genre  = new ArrayList<String>();
    ArrayList<roles> Roles  = new ArrayList<roles>();

    public static void main(String args[]){
        Imdb_to_Neo4j gv = new Imdb_to_Neo4j();
        long start_time=System.currentTimeMillis();
        gv.fetch_data();
        long end_time=System.currentTimeMillis();
        System.out.println((end_time-start_time));
    }


    public void fetch_data(){
        try{
            Class.forName("com.mysql.jdbc.Driver");
            Connection con=DriverManager.getConnection("jdbc:mysql://localhost:3306/imdb","root","Ritvik");
            Statement stmt=con.createStatement();
            Class.forName("com.mysql.jdbc.Driver");

            ResultSet genre_rs = stmt.executeQuery("select distinct genre from movies_genres;");

            while(genre_rs.next()){
                String genre = genre_rs.getString(1);
                this.Genre.add(genre);
            }//genreend


            ResultSet movies_rs = stmt.executeQuery("select * from movies");
            while(movies_rs.next()){
                int id = movies_rs.getInt(1);
                String movie_name = movies_rs.getString("name");
                int year = movies_rs.getInt("year");

                movie_ht.put(id,new movie(id, movie_name,year));
            }//movies_rs end
            System.out.println("movies Done");
            ResultSet movie_genre_rs = stmt.executeQuery("select movie_id, group_concat(genre) from movies_genres group by  movie_id");
            while(movie_genre_rs.next()){
                int movie_id = movie_genre_rs.getInt(1);
                String genre[] = movie_genre_rs.getString(2).split(",");
                if(genre!=null) {
                    System.out.println(genre[0]);
                    movie_genre_ht.put(movie_id, genre);
                }
            }//movie_genre_rs end
            System.out.println("Movies_genre Done"+movie_genre_ht.size());
            ResultSet director_genre_rs = stmt.executeQuery("select director_id, group_concat(genre) from directors_genres group by  director_id");
            while(director_genre_rs.next()){
                int director_id = director_genre_rs.getInt(1);
                String genre[] = director_genre_rs.getString(2).split(",");

                director_genre_ht.put(director_id,genre);
            }//director_genre_rs end
            System.out.println("Director genre Done");
            ResultSet director_movie_rs = stmt.executeQuery("select director_id, group_concat(movie_id) from movies_directors group by  director_id");
            while(director_movie_rs.next()){
                int director_id = director_movie_rs.getInt(1);
                String movie[] = director_movie_rs.getString(2).split(",");

                director_movie_ht.put(director_id,movie);
            }//director_movie_rs end
            System.out.println("Directors movies Done");
            ResultSet dir_rs = stmt.executeQuery("select * from directors");
            while(dir_rs.next()){
                int dir_id = dir_rs.getInt(1);
                String firstName = dir_rs.getString(2);
                String lastName = dir_rs.getString(3);

                director_ht.put(dir_id,new director(dir_id,firstName,lastName));

            }//director end
            System.out.println("Directors Done");

            ResultSet act_rs = stmt.executeQuery("select * from actors");

            while(act_rs.next()){
                int act_id = act_rs.getInt(1);
                String firstName = act_rs.getString(2);
                String lastName = act_rs.getString(3);
                actor_ht.put(act_id, new actor(act_id,firstName,lastName));

            }//actor end
            System.out.println("actors Done");

            ResultSet actor_movie_rs = stmt.executeQuery("select actor_id, group_concat(movie_id) from roles group by actor_id");
            while(actor_movie_rs.next()){
                int act_id = actor_movie_rs.getInt(1);
                String movie_id[] = actor_movie_rs.getString(2).split(",");
                actor_movie_ht.put(act_id,movie_id);
            }

            ResultSet movie_role_rs = stmt.executeQuery("select actor_id, group_concat(movie_id) from roles group by actor_id");
            while(movie_role_rs.next()){
                int movie_id = movie_role_rs.getInt(1);
                String roles[] = movie_role_rs.getString(2).split(",");
                movie_roles_ht.put(movie_id,roles);
            }
            /*ResultSet roles_rs = stmt.executeQuery("select * from roles");

            while(roles_rs.next()){
                int act_id = roles_rs.getInt(1);
                int movie_id = roles_rs.getInt(2);
                String role = roles_rs.getString(3);
                Roles.add(new roles(act_id,movie_id,role));

            }//roles end
            */
            System.out.println("Roles Done");
            System.out.println("Movie count:"+movie_ht.size());
            System.out.println("Actor count:"+actor_ht.size());
            System.out.println("Director count:"+director_ht.size());

            put_data();

        }//try end
        catch(Exception e){

        }//catchend


    }

    public void put_data(){
        File file = new File("C:\\Users\\ritvi\\Documents\\Neo4j\\TDM\\GraphDB");
        try {
            BatchInserter inserter = BatchInserters.inserter(file);

            HashMap<Integer, Long> batch_movie_map = new HashMap<Integer,Long>();
            HashMap<String, Long> batch_genre_map = new HashMap<String,Long>();
            HashMap<Integer, Long> batch_actor_map = new HashMap<Integer,Long>();
            HashMap<Integer, Long> batch_director_map = new HashMap<Integer,Long>();

            for (String genre : Genre) {

                HashMap<String, Object> gen_map = new HashMap<String, Object>();
                gen_map.put("name", genre);
                batch_genre_map.put(genre,inserter.createNode(gen_map,Label.label("GENRE")));
            }
            System.out.println("Genre Batch process done");
            //Movie Insert
            Iterator iter = movie_ht.entrySet().iterator();
            int counter=0;
            while (iter.hasNext()) {
                Map.Entry pair = (Map.Entry) iter.next();
                movie movie_pair = (movie) pair.getValue();

                HashMap<String, Object> movie_map = new HashMap<>();
                movie_map.put("id", movie_pair.movie_id);
                movie_map.put("title", movie_pair.title);
                movie_map.put("year", movie_pair.year);

                long node = inserter.createNode(movie_map, Label.label("MOVIE"));
                batch_movie_map.put(movie_pair.movie_id, node);

                String[] gen_buffer = movie_genre_ht.get(movie_pair.movie_id);
                if(gen_buffer!=null ) {
                    for (String genre : gen_buffer) {
                        try {

                            long genre_node = batch_genre_map.get(genre);
                            inserter.createRelationship(node, genre_node, RelationshipType.withName("MOVIE_GENRE"), null);
                            //System.out.println("Relationship created"+(counter++));
                        } catch (Exception e) {
                            e.printStackTrace();
                            continue;
                        }
                    }
                }
            }//Movie Insert end
            System.out.println("Movie Batch process done");
            //actor insert
            iter = actor_ht.entrySet().iterator();
            while (iter.hasNext()) {
                Map.Entry pair = (Map.Entry) iter.next();
                actor actor_pair = (actor) pair.getValue();

                HashMap<String, Object> actor_map = new HashMap<>();
                actor_map.put("id", actor_pair.actor_id);
                actor_map.put("firstName", actor_pair.firstName);
                actor_map.put("lastName", actor_pair.lastName);

                long node = inserter.createNode(actor_map, Label.label("ACTOR"));
                batch_actor_map.put(actor_pair.actor_id, node);
                try {
                    String movie_buffer[] = actor_movie_ht.get(actor_pair.actor_id);
                    if (movie_buffer != null) {
                        for (String movie : movie_buffer) {
                            int movie_id = Integer.parseInt(movie);
                            String roles_buffer[] = movie_roles_ht.get(movie_id);
                            HashMap<String, Object> role_map = new HashMap<>();
                            if (roles_buffer != null) {
                                for (String role : roles_buffer) {
                                    role_map.put("role", role);
                                }
                            }else{
                                role_map=null;
                            }
                            long movie_node = batch_movie_map.get(movie_id);
                            inserter.createRelationship(node, movie_node, RelationshipType.withName("ACTED_IN"), role_map);
                        }
                    }
                }
                catch(Exception e){
                    e.printStackTrace();
                    continue;
                }
                /*
                for (roles role : Roles) {
                    if (role.actor_id == (actor_pair.actor_id)) {
                        long movie_node = batch_movie_map.get(role.movie_id);
                        inserter.createRelationship(node, movie_node, RelationshipType.withName("ACTED_IN"), null);
                    }
                }
                */

            } // actor insert end
            System.out.println("Actor Batch process done");

            //director end
            iter = director_ht.entrySet().iterator();
            counter=0;
            while (iter.hasNext()) {
                Map.Entry pair = (Map.Entry) iter.next();
                director director_pair = (director) pair.getValue();

                HashMap<String, Object> director_map = new HashMap<>();
                director_map.put("id", director_pair.dir_id);
                director_map.put("firstName", director_pair.firstName);
                director_map.put("lastName", director_pair.lastName);

                long node = inserter.createNode(director_map, Label.label("DIRECTOR"));
                batch_director_map.put(director_pair.dir_id, node);
                counter++;
                String[] genre_buffer = director_genre_ht.get(director_pair.dir_id);
                if (genre_buffer != null){
                    for (String genre : genre_buffer) {
                        try {
                            long genre_node = batch_genre_map.get(genre);
                            inserter.createRelationship(node, genre_node, RelationshipType.withName("DIRECTOR_GENRE"), null);
                        } catch (Exception e) {
                            e.printStackTrace();
                            continue;
                        }
                    }
                }
                String[] movie_buffer = director_movie_ht.get(director_pair.dir_id);
                if(movie_buffer!=null) {
                    for (String movie : movie_buffer) {
                        try {
                            long movie_node = batch_movie_map.get(Integer.parseInt(movie));
                            inserter.createRelationship(node, movie_node, RelationshipType.withName("DIRECTED_BY"), null);
                        } catch (Exception e) {
                            e.printStackTrace();
                            continue;
                        }
                    }
                }

            }//director end
            inserter.shutdown();
            System.out.println("Director Batch process done"+counter);
        }catch(Exception e){
            e.printStackTrace();
        }


    }

}

