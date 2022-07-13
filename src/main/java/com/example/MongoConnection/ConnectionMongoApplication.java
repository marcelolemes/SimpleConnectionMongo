package com.example.MongoConnection;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.mongodb.Block;
import com.mongodb.MongoClient;
import com.mongodb.MongoClientURI;
import com.mongodb.client.AggregateIterable;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import org.bson.Document;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

import java.io.IOException;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.text.DecimalFormat;
import java.util.*;


@SpringBootApplication
public class ConnectionMongoApplication {

    private static final DecimalFormat df = new DecimalFormat("000,000.00");

    public static void main(String[] args) {
        SpringApplication.run(ConnectionMongoApplication.class, args);

        ObjectMapper mapper = new ObjectMapper();
        List<String> amounts = new ArrayList<>();


        MongoClient mongoClient = new MongoClient(new MongoClientURI(Util.CONEXAO_MONGO));
        MongoDatabase database = mongoClient.getDatabase(Util.DOCUMENT_NAME);
        MongoCollection<Document> collection = database.getCollection(Util.DOCUMENT_NAME);

        AggregateIterable<Document> result = collection.aggregate(Arrays.asList(new Document("$match",
                        new Document("$and", Arrays.asList(new Document("transaction_date",
                                        new Document("$gte",
                                                new Date(1623369600000L))),
                                new Document("transaction_date",
                                        new Document("$lt",
                                                new Date(1623456000000L)))))),
                new Document("$project",
                        new Document("amount", 1L))));

        result.forEach(new Block<Document>() {

            @Override
            public void apply(final Document document) {
                try {
                    JsonNode node = mapper.readTree(document.toJson());
                    amounts.add(String.valueOf(node.get("amount")));

                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
            }

        });

        BigDecimal amount = new BigDecimal(BigInteger.ZERO);


        for (String string : amounts
        ) {
            System.out.println(string);
            amount = amount.add(convertStringBase64ToBigDecimal(string.substring(1, string.length() - 1)));
        }

        df.format(amount);
        System.out.println("Soma final: R$" + df.format(amount));
    }

    private static BigDecimal convertStringBase64ToBigDecimal(final String base64Value) {
        if (base64Value != null) {
            final byte[] decoded = Base64.getDecoder().decode(base64Value);
            final BigInteger bi = new BigInteger(decoded);
            return new BigDecimal(bi, 6);
        }

        return null;
    }

}


