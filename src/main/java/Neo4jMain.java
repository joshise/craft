import com.google.gson.Gson;
import org.neo4j.driver.v1.*;

import java.io.File;
import java.util.Scanner;
import static org.neo4j.driver.v1.Values.parameters;

public class Neo4jMain {
    //Using an local Neo4j instance
    private static final Driver driver = GraphDatabase.driver("bolt://localhost:7687", AuthTokens.basic("thisisfors@gmail.com", "Passw0rd!"));

    private static void createNodesAndRelationships() {
        // Driver objects are thread-safe and are typically made available application-wide.
        //Assumption: Data from hive comes in csv format
        try (Session session = driver.session()) {
            //Auto-commit transactions are the only way to execute USING PERIODIC COMMIT Cypher statements.
            session.run("CREATE CONSTRAINT ON (u:Product) ASSERT u.ProductId IS UNIQUE");
            session.run("CREATE CONSTRAINT ON (u:User) ASSERT u.UserId IS UNIQUE");
            session.run("CREATE CONSTRAINT ON (u:Customer) ASSERT u.CustomerId IS UNIQUE");
            session.run("CREATE INDEX ON :User(FullName)");

            session.run("USING PERIODIC COMMIT 1000\n" +
                    "LOAD CSV WITH HEADERS FROM \"file:///Users/seema/IdeaProjects/Intuit/src/main/resources/users.csv\" AS csvLine\n" +
                    "MERGE (n:User {UserId: csvLine.UserId, FullName: csvLine.FullName, Address: csvLine.Address1, Zip:csvLine.Zip, " +
                    "State:csvLine.State, Phone:csvLine.Phone, Email:csvLine.Email})");
///Users/seema/IdeaProjects/Intuit/src/main/resources/
            session.run("USING PERIODIC COMMIT 1000\n" +
                    "LOAD CSV WITH HEADERS FROM \"file:///Users/seema/IdeaProjects/Intuit/src/main/resources/products.csv\" AS csvLine\n" +
                    "MERGE (n:Product { ProductId: csvLine.ProductId, ProductName: csvLine.Product})");

            session.run("USING PERIODIC COMMIT 1000\n" +
                    "LOAD CSV WITH HEADERS FROM \"file:///Users/seema/IdeaProjects/Intuit/src/main/resources/customers.csv\" AS csvLine\n" +
                    "MERGE (n:Customer {CustomerId:csvLine.CustomerId, UserId: csvLine.UserId, FullName: csvLine.FullName, Address:csvLine.Address1, " +
                    "Zip:csvLine.Zip, State:csvLine.State, Phone:csvLine.Phone, Email:csvLine.Email})\n");

            session.run("USING PERIODIC COMMIT 1000\n" +
                    "LOAD CSV WITH HEADERS FROM \"file:///Users/seema/IdeaProjects/Intuit/src/main/resources/userProduct.csv\" AS csvLine\n"+
                    "MATCH (a:User),(b:Product) "+
                    "WHERE a.UserId = csvLine.UserId AND b.ProductId = csvLine.ProductId "+
                    "MERGE (a)-[r:uses]->(b) ");

            session.run(
                    "MATCH (a:User),(b:Customer) "+
                            "WHERE a.UserId = b.UserId "+
                            "MERGE (a)-[r:sellsTo]->(b)");

            session.run(
                    "MATCH (n:Customer),(m:User) WHERE n.Zip=m.Zip AND n.State = m.State AND " +
                            "(n.Address=m.Address OR n.Email=m.Email ) "+
                            "MERGE (m)-[r:sameAs]->(n) "+
                            "MERGE (n)-[q:sameAs]->(m) ");
        }
    }

    private static void cleanDB(){
        try (Session session = driver.session()) {
            session.run("MATCH (n)\n" +
                    "WITH n LIMIT 10000\n" +
                    "OPTIONAL MATCH (n)-[r]->()\n" +
                    "DELETE n,r");
        }
    }

    private static String printSubGraph(String name){
        Session session = driver.session();
        Gson gson = new Gson();
        StringBuffer sb= new StringBuffer();
        //Using this for most of uses, sellsTo and sameAs
        StatementResult result = session.run("MATCH (n:User) WHERE n.FullName=$fullName "+
                "MATCH (n)-[r]->(m)"+
                "RETURN type(r),m", parameters( "fullName", name ) );
        while ( result.hasNext() ) {
            Record record = result.next();
            record.asMap();
            sb.append(gson.toJson(record.asMap())).append("\n");
        }
        result = session.run(//User to user
                "match (n:User)<-[:sameAs]-(c:Customer)<-[r:sellsTo]-(m:User) "+
                 "WHERE n.FullName=$fullName "+
                 "return $type, m", parameters( "fullName", name , "type", "isCustomerOf"));
        while ( result.hasNext() ) {
            Record record = result.next();
            record.asMap();
            sb.append(gson.toJson(record.asMap())).append("\n");
        }
        session.close();
        return sb.toString();
    }

    public static void main(String[] args) {
        cleanDB();
        createNodesAndRelationships();

        Scanner scanner=new Scanner(System.in);
        while (true) {
            System.out.println("Enter fullName whose subgraph is needed:");
            String name = scanner.nextLine();
            if(name.equals("exit")){
                break;
            }
            String result = printSubGraph(name);
            System.out.println(result);
        }
        scanner.close();
        driver.close();
    }
}