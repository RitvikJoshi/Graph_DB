

import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.unsafe.batchinsert.BatchInserter;
import org.neo4j.unsafe.batchinsert.BatchInserters;

import java.io.File;
import java.io.FileInputStream;
import java.util.HashMap;
import java.util.Scanner;

/**
 * Created by ritvi on 3/28/2017.
 */
public class Protein_IGraph_Neo4j {
    String pathname="C:\\Users\\ritvi\\Documents\\Neo4j\\TDM\\Assignment";
    BatchInserter inserter=null;
    HashMap<String,Long> BatchHuman = new HashMap<>();
    HashMap<String,Long> BatchYeast = new HashMap<>();
    HashMap<String,Long> BatchProtein = new HashMap<>();
    public static void main(String args[]){
        Protein_IGraph_Neo4j pg = new Protein_IGraph_Neo4j();
        Scanner input = new Scanner(System.in);

        System.out.println("Enter input directory for database");
        pg.pathname = input.nextLine();


        System.out.println("Enter input file for Human::");
        String human = input.nextLine();
        //pg.iGraph_createNode("C:\\Users\\ritvi\\IdeaProjects\\graph\\src\\main\\java\\Assignment4\\human.igraph","Human",pg.BatchHuman);
        pg.iGraph_createNode(human,"Human",pg.BatchHuman);
        System.out.println("Enter input file for Yeast::");
        String Yeast = input.nextLine();


        //pg.iGraph_createNode("C:\\Users\\ritvi\\IdeaProjects\\graph\\src\\main\\java\\Assignment4\\yeast.igraph","Yeast",pg.BatchYeast);
        pg.iGraph_createNode(Yeast,"Yeast",pg.BatchYeast);
        System.out.println("iGraph nodes inserted");

        System.out.println("Enter input file for Protein::");
        String protein_path = input.nextLine();

        //String protein_path = "C:\\Users\\ritvi\\Desktop\\Graph\\assignment 4\\Proteins\\Proteins\\part3_Proteins\\Proteins\\target";

        File protein_dir = new File(protein_path);

        for(File filename:protein_dir.listFiles()){
                pg.Protein_createNode(protein_path+"\\"+filename.getName(),"Protein\\"+filename.getName());
        }
        System.out.println("Protein nodes inserted");


    }


    public void iGraph_createNode(String filename, String database_name,HashMap<String,Long> BatchCollection) {

        try {
            System.out.println(filename);
            File file = new File(filename);
            Scanner reader = new Scanner(new FileInputStream(file));

            while (reader.hasNext()) {
                String input_line = reader.nextLine();
                String line_buffer[] = input_line.split("\\s+");
                switch(line_buffer[0]){

                    case "t":
                        File dbpath =  new File(pathname+"\\"+database_name);
                        inserter = BatchInserters.inserter(dbpath);
                        break;
                    case "v":
                        String id = line_buffer[1];
                        if(line_buffer.length>3){
                            Label label [] = new Label[line_buffer.length-2];
                            for (int iter=2;iter<line_buffer.length;iter++){
                                label[iter-2] = Label.label(line_buffer[iter]);
                            }
                            HashMap<String,Object> map= new HashMap<>();
                            map.put("id",id);

                            BatchCollection.put(id,inserter.createNode(map,label));

                        }else{
                            Label label = Label.label(line_buffer[2]);
                            HashMap<String,Object> map= new HashMap<>();
                            map.put("id",id);
                            BatchCollection.put(id,inserter.createNode(map,label));
                        }
                        break;
                    case "e":


                        String node1 = line_buffer[1];
                        String node2 = line_buffer[2];
                        String label = line_buffer[3];

                        Long first_node = BatchCollection.get(node1);
                        Long second_node = BatchCollection.get(node2);

                        inserter.createRelationship(first_node, second_node, RelationshipType.withName(label),null);
                        break;

                    default:
                        break;

                }//switch end
            }//while_end
            inserter.shutdown();
        }//try end
        catch(Exception e){
            e.printStackTrace();
        }//catch end

    }//igraph create node end

    public void Protein_createNode(String filename,String database_name){
        try {
            System.out.println(filename);
            File file = new File(filename);
            Scanner reader = new Scanner(new FileInputStream(file));

            boolean vertex_flag=true;
            boolean first_occ=true;
            //databse_name = \\Protein\\filename
            File dbpath =  new File(pathname+"\\"+database_name);
            inserter = BatchInserters.inserter(dbpath);
            while (reader.hasNext()) {
                String input_line = reader.nextLine();
                String line_buffer[] = input_line.split("\\s+");
                if (line_buffer.length==1){
                    if(first_occ) {
                        first_occ = false;
                    }else{
                        vertex_flag=false;
                    }

                }else{
                    if(vertex_flag){
                        String id = line_buffer[0];
                        Label label = Label.label(line_buffer[1]);
                        HashMap<String, Object> map = new HashMap<>();
                        map.put("id",id);
                        BatchProtein.put(id,inserter.createNode(map,label));
                    }else{
                       String id1 = line_buffer[0];
                       String id2 = line_buffer[1];
                       Long node1=BatchProtein.get(id1);
                       Long node2=BatchProtein.get(id2);

                       inserter.createRelationship(node1, node2, RelationshipType.withName("Edge"),null);

                    }

                }


            }
            inserter.shutdown();
        }//try end
        catch(Exception e){
            e.printStackTrace();
        }//catch end
    }//protein function end

}//class end
