import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.neo4j.driver.v1.*;

public class Neo4jMainTest
{
    Neo4jMain myMain;
    Session session;
    Driver driver;

    @Before
    public void setup(){
        driver = GraphDatabase.driver("bolt://localhost:7687",
                AuthTokens.basic("thisisfors@gmail.com", "Passw0rd!"));
        myMain = new Neo4jMain(driver);
        myMain.cleanDB();
        session = driver.session();
    }

    @After
    public void cleanup(){
        session.close();
        driver.close();
    }

    @Test
    public void testCreateNodes(){
        myMain.createConstraints(session);

        StatementResult result = session.run("MATCH (n:User) RETURN n");
        assert (result.hasNext() == false): "Users already exist";
        myMain.createUserNode(session);
        result = session.run("match (n:User) RETURN count(n)");
        assert (result.next().values().get(0).asInt() == 5): "User node count does not match";

        result = session.run("MATCH (n:Product) RETURN n");
        assert (result.hasNext() == false): "Product already exist";
        myMain.createProductNode(session);
        result = session.run("match (n:Product) RETURN count(n)");
        assert (result.next().values().get(0).asInt() == 3): "Product node count does not match";

        result = session.run("MATCH (n:Customer) RETURN n");
        assert (result.hasNext() == false): "Customer already exist";
        myMain.createCustomerNode(session);
        result = session.run("match (n:Customer) RETURN count(n)");
        assert (result.next().values().get(0).asInt() == 9): "Customer node count does not match";
    }

    @Test
    public void testCreateRel(){
        myMain.createNodes(session);
        StatementResult result = session.run("MATCH (n)-[r:uses]-(m) where m.ProductName=\"Self-Employed\"  RETURN r");
        assert (result.hasNext() == false): "Uses relationship count does not match";

        myMain.createUserProductRel(session);
        result = session.run("MATCH (n)-[r:uses]-(m) where m.ProductName=\"Self-Employed\"  RETURN r");
        assert (result.next().values().get(0) != null): "Uses relationship count does not match";

        result = session.run("MATCH (n)-[r:sellsTo]-(m) RETURN r");
        assert (result.hasNext() == false): "Sells to relationship already exists";

        myMain.createSellsToRel(session);
        result = session.run("MATCH (n)-[r:sellsTo]-(m)  RETURN r");
        assert (result.next().values().get(0) != null): "Sells to relationship does not exist";

        result = session.run("MATCH (n)-[r:sameAs]-(m) RETURN r");
        assert (result.hasNext() == false): "Same As relationship already exists";

        myMain.createSameAsRel(session);
        result = session.run("MATCH (n)-[r:sameAs]-(m)  RETURN r");
        assert (result.next().values().get(0) != null): "Same as relationship does not exist";
    }

    @Test
    public void testGetSubGraph(){
        session.run("CREATE (n:User {UserId: \"20\", FullName: \"John User\", Address: \"ABC\", Zip:\"55555\", " +
                "State:\"CA\", Phone:\"555 555 5555\", Email:\"tester@intuit.com\"})");
        session.run( "CREATE (n:Customer {CustomerId:\"30\", UserId: \"20\", FullName: \"Jane Customer\", Address:\"XYZ\", " +
                "Zip:\"55555\", State:\"CA\", Phone:\"555 888 5555\", Email:\"customer@intuit.com\"})\n");
        session.run("MATCH (a:User),(b:Customer)\n" +
                "WHERE b.UserId = a.UserId\n" +
                "CREATE (a)-[r:sellsTo]->(b)\n" +
                "RETURN r");
        String result = myMain.getSubGraph("John User");
        //Not the best test. Ideally match the result to expected
        assert !result.equals("User John User does not exist in the graph");
    }

}