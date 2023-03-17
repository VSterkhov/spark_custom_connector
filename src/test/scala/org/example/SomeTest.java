package org.example;

import org.junit.Rule;
import org.junit.Test;
import com.dimafeng.testcontainers.CassandraContainer;

public class SomeTest {

//    @Rule
//    public CassandraContainer cassandra = new CassandraContainer();
//
//
//    @Test
//    public void test(){
//        Cluster cluster = cassandra.getCluster();
//
//        try(Session session = cluster.connect()) {
//
//            session.execute("CREATE KEYSPACE IF NOT EXISTS test WITH replication = \n" +
//                    "{'class':'SimpleStrategy','replication_factor':'1'};");
//
//            List<KeyspaceMetadata> keyspaces = session.getCluster().getMetadata().getKeyspaces();
//            List<KeyspaceMetadata> filteredKeyspaces = keyspaces
//                    .stream()
//                    .filter(km -> km.getName().equals("test"))
//                    .collect(Collectors.toList());
//
//            assertEquals(1, filteredKeyspaces.size());
//        }
//    }

}