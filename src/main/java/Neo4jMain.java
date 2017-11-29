import com.google.gson.Gson;
import org.neo4j.driver.v1.*;

import java.util.Scanner;
import static org.neo4j.driver.v1.Values.parameters;

public class Neo4jMain {
    //Using an local Neo4j instance
    private static Driver driver;

    public Neo4jMain(Driver dr){
        driver = dr;
    }

    private String getLoadCsvStmt(String fileName){
        String file = getClass().getClassLoader().getResource(fileName).getPath();
        String periodicCommit = "USING PERIODIC COMMIT 1000\n";
        return periodicCommit + "LOAD CSV WITH HEADERS FROM \"file:"+file+"\" AS csvLine \n";
    }

    protected void createUserNode(Session session){
        session.run( getLoadCsvStmt("file/users.csv") +
                "MERGE (n:User {UserId: csvLine.UserId, FullName: csvLine.FullName, Address: csvLine.Address1, Zip:csvLine.Zip, " +
                "State:csvLine.State, Phone:csvLine.Phone, Email:csvLine.Email})");
    }

    protected void createProductNode(Session session){
        session.run( getLoadCsvStmt("file/products.csv") +
                "MERGE (n:Product { ProductId: csvLine.ProductId, ProductName: csvLine.Product})");
    }

    protected void createCustomerNode(Session session){
        session.run( getLoadCsvStmt("file/customers.csv") +
                "MERGE (n:Customer {CustomerId:csvLine.CustomerId, UserId: csvLine.UserId, FullName: csvLine.FullName, Address:csvLine.Address1, " +
                "Zip:csvLine.Zip, State:csvLine.State, Phone:csvLine.Phone, Email:csvLine.Email})\n");
    }

    protected void createUserProductRel(Session session){
        session.run( getLoadCsvStmt("file/userProduct.csv") +
                "MATCH (a:User),(b:Product) "+
                "WHERE a.UserId = csvLine.UserId AND b.ProductId = csvLine.ProductId "+
                "MERGE (a)-[r:uses]->(b) ");
    }

    protected void createSellsToRel(Session session){
        session.run( "MATCH (a:User),(b:Customer) "+
                "WHERE a.UserId = b.UserId "+
                "MERGE (a)-[r:sellsTo]->(b)");
    }

    protected void createSameAsRel(Session session) {
        session.run( "MATCH (n:Customer),(m:User) WHERE n.Zip=m.Zip AND n.State = m.State AND " +
                "(n.Address=m.Address OR n.Email=m.Email ) " +
                "MERGE (m)-[r:sameAs]->(n) " +
                "MERGE (n)-[q:sameAs]->(m) ");
    }

    protected void createConstraints(Session session){
        session.run("CREATE CONSTRAINT ON (u:Product) ASSERT u.ProductId IS UNIQUE");
        session.run("CREATE CONSTRAINT ON (u:User) ASSERT u.UserId IS UNIQUE");
        session.run("CREATE CONSTRAINT ON (u:Customer) ASSERT u.CustomerId IS UNIQUE");
        session.run("CREATE INDEX ON :User(FullName)");
    }

    protected void createNodes(Session session){
        createConstraints(session);
        createUserNode(session);
        createProductNode(session);
        createCustomerNode(session);
    }

    protected void createNodesAndRelationships() {
        // Driver objects are thread-safe and are typically made available application-wide.
        //Assumption: Data from hive comes in csv format
        try (Session session = driver.session()) {
            //Auto-commit transactions are the only way to execute USING PERIODIC COMMIT Cypher statements.
            createNodes(session);
            createUserProductRel(session);
            createSellsToRel(session);
            createSameAsRel(session);
        }
    }

    protected void cleanDB(){
        try (Session session = driver.session()) {
            session.run("MATCH (n)\n" +
                    "WITH n LIMIT 10000\n" +
                    "OPTIONAL MATCH (n)-[r]->()\n" +
                    "DELETE n,r");
        }
    }

    protected String getSubGraph(String name){
        Session session = driver.session();
        Gson gson = new Gson();
        StringBuffer sb= new StringBuffer();
        //Using this for uses, sellsTo and sameAs
        StatementResult result = session.run("MATCH (n:User) WHERE n.FullName=$fullName "+
                "MATCH (n)-[r]->(m)"+
                "RETURN type(r),m", parameters( "fullName", name ) );
        if (result.hasNext() == false){
            return "User "+name+" does not exist in the graph";
        }
        while (result.hasNext() ) {
            Record record = result.next();
            sb.append(gson.toJson(record.asMap())).append("\n");
        }
        result = session.run(//User to user
                "match (n:User)<-[:sameAs]-(c:Customer)<-[r:sellsTo]-(m:User) "+
                 "WHERE n.FullName=$fullName "+
                 "return $type, m", parameters( "fullName", name , "type", "isCustomerOf"));
        while ( result.hasNext() ) {
            Record record = result.next();
            sb.append(gson.toJson(record.asMap())).append("\n");
        }
        session.close();
        return sb.toString();
    }

    public static void main(String[] args) {
        Driver driver = GraphDatabase.driver("bolt://localhost:7687", AuthTokens.basic("thisisfors@gmail.com", "Passw0rd!"));
        Neo4jMain neo = new Neo4jMain(driver);
        neo.cleanDB();
        neo.createNodesAndRelationships();

        Scanner scanner=new Scanner(System.in);
        while (true) {
            System.out.println("Enter fullName whose subgraph is needed:");
            String name = scanner.nextLine();
            if(name.equals("exit")){
                break;
            }
            String result = neo.getSubGraph(name);
            System.out.println(result);
        }
        scanner.close();
        driver.close();
    }
}