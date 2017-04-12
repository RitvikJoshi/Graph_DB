package Assignment4;

import org.neo4j.graphdb.*;
import org.neo4j.graphdb.factory.GraphDatabaseFactory;

import java.io.File;
import java.io.FileInputStream;
import java.util.*;

/**
 * Created by ritvi on 3/28/2017.
 */


class node{
    String id;
    String label;
    ArrayList<node> edge;
    node(String id,String label,ArrayList<node> edge){
        this.id=id;
        this.label=label;
        this.edge=edge;
    }

}
class SubGraphMatching{

    HashMap<String,node> Query;
    HashMap<String,ArrayList<Long> >SearchSpace =  new HashMap<>();
    ArrayList<String> Order = new ArrayList<>();
    ArrayList<String> edgevisited = new ArrayList<>();
    HashMap<String,Long> Solution = new HashMap<>();
    long start_time;
    int counter=0;
    static GraphDatabaseService db;
    File Filename;

    SubGraphMatching(HashMap<String,node> Query,File filename){
        this.Query=Query;
        this.Filename=filename;

    }

    public void matching(){
        searchspace();

        int index=0;
        Iterator iter= Query.entrySet().iterator();
        while(iter.hasNext()) {
            Map.Entry pair = (Map.Entry) iter.next();
            String id = (String) pair.getKey();
            if(!Order.contains(id))
                order(id);
        }
        for(String id:Order){
            System.out.print(id+",");
        }
        start_time = System.currentTimeMillis();
        search(0);
        System.out.println("\nTotal solution::"+counter);
        long end_time = System.currentTimeMillis();
        System.out.println("Total time taken in searching::"+(end_time-start_time)+"ms");
        db.shutdown();
    }

    public void order(String id){

        if(!Order.contains(id))
            Order.add(id);
        if(!edgevisited.contains(id)) {
            edgevisited.add(id);
            ArrayList<node> edge = Query.get(id).edge;
            for (node nbh : edge) {
                order(nbh.id);
            }
        }
    }
    public void searchspace(){

        Iterator iter= Query.entrySet().iterator();
        db= new GraphDatabaseFactory().newEmbeddedDatabase(Filename);
        Transaction tx = db.beginTx();
        while(iter.hasNext()){

            Map.Entry pair = (Map.Entry) iter.next();
            String id = (String) pair.getKey();
            node graph_node = (node) pair.getValue();
            Label label = Label.label(graph_node.label);
            ResourceIterator<Node> result = db.findNodes(label);
            ArrayList<Long> node_id = new ArrayList<>();
            while(result.hasNext()){
                Node node = result.next();
                node_id.add(node.getId());

            }
            SearchSpace.put(graph_node.label,node_id);
        }
        tx.close();


    }

    public void search(int index){
        String query_node = Order.get(index);
        String label = Query.get(query_node).label;
        ArrayList<Long> graph_nodes = SearchSpace.get(label);

        for(Long node: graph_nodes){

            if(counter == 1000){
                return ;
            }

            boolean enc = false;

            Collection<Long> nodes = Solution.values();
            if(nodes.contains(node))
                enc = true;
            if(!check(query_node,node) || enc){
                continue;
            }

            Solution.put(query_node,node);
            if(index < Order.size()-1)
                search(index+1);
            else if(Solution.size()==Order.size()){
                counter++;

                System.out.println(counter+":");
                Iterator iter= Solution.entrySet().iterator();
                while(iter.hasNext()) {
                    Map.Entry pair = (Map.Entry) iter.next();
                    Long id = (Long) pair.getValue();
                    System.out.print(id+",");
                }
                System.out.println();


                //Solution.remove(query_node);
            }
        }
        Solution.remove(query_node);

    }
    public boolean check(String query_node,Long graph_node){
        Transaction tx= db.beginTx();
        if(Solution.size()==0)
            return true;
        else{
            ArrayList<node> neighbhors = Query.get(query_node).edge;
            for(node nbh:neighbhors){
                if(Solution.containsKey(nbh.id)){
                    Long node_id = Solution.get(nbh.id);
                    Node graph_node1 = db.getNodeById(node_id);
                    Node graph_node2 = db.getNodeById(graph_node);
                    Iterable<Relationship> rel_list = graph_node1.getRelationships(Direction.BOTH);
                    boolean flag=false;
                    Iterator iter = rel_list.iterator();
                    while(iter.hasNext()){
                        Relationship rel =(Relationship) iter.next();
                        if(graph_node2.equals(rel.getOtherNode(graph_node1)))
                            flag=true;
                    }
                    if(!flag){
                        return false;
                    }
                }
            }
        }
        return true;
    }


}

public class query {

    HashMap<String,HashMap<String, node>> Query = new HashMap<>();

    public static void main(String args[]){
        query qr = new query();
        System.out.println("Options::");
        System.out.println("1. Human/Yeast");
        System.out.println("2. Protein");
        Scanner input = new Scanner(System.in);
        switch(input.nextInt()){
            case 1:
                try {
                    System.out.println("Enter Human query path file name::");
                    input = new Scanner(System.in);
                    String filename = input.nextLine();
                    System.out.println("Enter Human Database path::");
                    File database = new File(input.nextLine());
                    /*String filename = "C:\\Users\\ritvi\\IdeaProjects" +
                        "\\graph\\src\\main\\java\\Assignment4" +
                        "\\human_q10.igraph";

                    File database = new File("C:\\Users\\ritvi\\Documents\\Neo4j" +
                        "\\TDM\\Assignment\\Human");
                    */
                    qr.read_data_iGraph(filename);
                    //HashMap<String, node> graph=null;
                    Iterator iter = qr.Query.entrySet().iterator();
                    int iter_Count=0;
                    while (iter.hasNext()) {
                        Map.Entry pair = (Map.Entry) iter.next();
                        //if(iter_Count==1) {
                        System.out.println("Query t #"+(String)pair.getKey());
                        HashMap<String, node> graph = (HashMap) pair.getValue();
                        //}
                        SubGraphMatching sgm = new SubGraphMatching(graph, database);
                        sgm.matching();
                        //break;
                    }

                    break;
                }catch(Exception e){
                    e.printStackTrace();
                }

            case 2:
                try {
                    System.out.println("Enter Protein query path file name::");
                    input = new Scanner(System.in);
                    String protein_path = input.nextLine();
                    System.out.println("Enter Protein Database path::");
                    File database = new File(input.nextLine());


                    //String protein_path = "C:\\Users\\ritvi\\Desktop\\Graph\\assignment 4\\Proteins\\Proteins\\part3_Proteins\\Proteins\\target";

                    HashMap<String,node> graph= qr.read_data_Protein(protein_path );
                    SubGraphMatching sgm = new SubGraphMatching(graph, database);
                    sgm.matching();
                    break;
                }catch(Exception e) {
                    e.printStackTrace();
                }


            default:
                System.out.println("Wrong input...Please choose from one of the options");
                break;

        }
    }

    public void read_data_iGraph(String filename) {
        try {
            System.out.println(filename);
            File file = new File(filename);
            Scanner reader = new Scanner(new FileInputStream(file));
            HashMap<String, node> sub_graph = new HashMap<>();
            while (reader.hasNext()) {
                String input_line = reader.nextLine();
                String line_buffer[] = input_line.split("\\s+");
                switch (line_buffer[0]) {

                    case "t":
                        String id = line_buffer[2];
                        sub_graph = new HashMap<>();
                        Query.put(id,sub_graph);
                        break;
                    case "v":
                        String vid = line_buffer[1];
                        String label = line_buffer[2];
                        sub_graph.put(vid,new node(vid,label,new ArrayList<node>()));
                        break;
                    case "e":
                        String vid1 = line_buffer[1];
                        String vid2 = line_buffer[2];

                        node v1 = sub_graph.get(vid1);
                        node v2 = sub_graph.get(vid2);
                        v1.edge.add(v2);
                        v2.edge.add(v1);

                        break;

                    default:
                        break;


                }//switch end
            }//while end
        } catch (Exception e) {
            e.printStackTrace();
        }//catch end
    }

    public HashMap read_data_Protein(String filename){
        HashMap<String, node> sub_graph = new HashMap<>();
        try {
            System.out.println(filename);
            File file = new File(filename);
            Scanner reader = new Scanner(new FileInputStream(file));

            boolean vertex_flag=true;
            boolean first_occ=true;

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
                        String label = line_buffer[1];
                        sub_graph.put(id,new node(id,label,new ArrayList<node>()));

                    }else{
                        String id1 = line_buffer[0];
                        String id2 = line_buffer[1];
                        node node1 = sub_graph.get(id1);
                        node node2 = sub_graph.get(id2);
                        node1.edge.add(node2);
                        node2.edge.add(node1);

                    }

                }


            }

        }//try end
        catch(Exception e){
            e.printStackTrace();
        }//catch end

        return sub_graph;

    }//protein function end

}
