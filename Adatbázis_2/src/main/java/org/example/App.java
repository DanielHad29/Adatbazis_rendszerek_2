package org.example;
import com.mongodb.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.MongoDatabase;
import org.bson.Document;
import java.text.MessageFormat;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Consumer;
import static com.mongodb.client.model.Filters.eq;
import static com.mongodb.client.model.Filters.where;

public class App
{
    private static MongoDatabase currentDatabase;
    private static String myCollection;

    public static void main( String[] args )
    {
        MongoClient mongoClient = new MongoClient("localhost", 27017); // Connect to database
        currentDatabase = mongoClient.getDatabase("Homework");
        myCollection = "dolgozo";
        createAndFillCollection(); // Create new collection

        System.out.println("Original collection dolgozo");
        printCollectionContent(myCollection); // List content

        createSupplierCollection();   // Create new table
        toolsForWork(); // create new table

        currentDatabase.getCollection(myCollection).
                updateOne(eq("name", "George"), new Document("$set", new Document("name", "Daniel"))); // Update data

        deleteEmployee("Jhon"); // Delete row
        showResult(); // List contents of tables
        runQuery();  // Run complex query
        mongoClient.close(); // Cleanup
    }

    private static void runQuery() {
        MongoCursor<Document> resultIterator = currentDatabase.getCollection("employee_tools")
                .find(where("this.piece<=30"))
                .iterator();
        try {
            System.out.println("\nQuerry result");
            if(!resultIterator.hasNext()){
                System.out.println("No match found...");
            }
            else {
                while (resultIterator.hasNext()) {
                    System.out.println(resultIterator.next().toJson());
                }
            }
        } finally {
            resultIterator.close();
        }
    }

    private static void toolsForWork() {
        currentDatabase.createCollection("employee_tools");
        Document doc1 = new Document("item", "desk").append("piece", 23);
        Document doc2 = new Document("item", "keyboard").append("piece", 11);
        Document doc3 = new Document("item", "mouse").append("piece", 54);
        Document doc4 = new Document("item", "chair").append("piece", 30);
        Document doc5 = new Document("item", "display").append("piece", 32);

        MongoCollection<Document> toolsForWork = currentDatabase.getCollection("employee_tools");
        toolsForWork.insertOne(doc1);
        toolsForWork.insertOne(doc2);
        toolsForWork.insertOne(doc3);
        toolsForWork.insertOne(doc4);
        toolsForWork.insertOne(doc5);

    }

    private static void showResult() {
        System.out.println("\nAfter delete employee Jhon");
        System.out.println("Collection: Dolgozo");
        printCollectionContent("dolgozo");
        System.out.println("\nCollection: Supplier");
        printCollectionContent("Supplier");
        System.out.println("\nCollection: employee_tools");
        printCollectionContent("employee_tools");
    }

    private static void createAndFillCollection() {
        currentDatabase.createCollection(myCollection);
        List<Document> documents = new ArrayList<>();
        Document doc = new Document("name", "George").append("beosztas", "iroda").append("fizetes", 3000);documents.add(doc);
        doc = new Document("name", "Jhon").append("beosztas", "iroda").append("fizetes", 6000);documents.add(doc);
        doc = new Document("name", "Márton").append("beosztas", "raktár").append("fizetes", 1000);documents.add(doc);
        doc = new Document("name", "Ezekiel").append("beosztas", "Ceo").append("fizetes", 12000);documents.add(doc);
        currentDatabase.getCollection(myCollection).insertMany(documents);
    }

    private static void createSupplierCollection(){
        Document doc1 = new Document("name", "Jani és Jani Kft.").append("already ordered", 2);
        Document doc2 = new Document("name", "Metal2000 Kft.").append("already ordered", 4);
        Document doc3 = new Document("name", "Dombos Kft.").append("already ordered", 5).append("in progress", 1);
        MongoCollection<Document> supplierCollection = currentDatabase.getCollection("Supplier");
        supplierCollection.insertOne(doc1);
        supplierCollection.insertOne(doc2);
        supplierCollection.insertOne(doc3);
    }

    private static void deleteEmployee(String name) {
        currentDatabase.getCollection(myCollection)
                .deleteOne(eq("name", name));
    }

    private static void printCollectionContent(String tableName) {
        MongoCollection<Document> table = currentDatabase.getCollection(tableName);
        table.find().forEach((Consumer<Document>) it -> System.out.println(it.toJson()));
    }


}
