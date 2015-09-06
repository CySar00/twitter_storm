package Storm.Databases.CassandraDB.Preprocessing;

import me.prettyprint.cassandra.model.ConfigurableConsistencyLevel;
import me.prettyprint.cassandra.serializers.StringSerializer;
import me.prettyprint.cassandra.service.CassandraHostConfigurator;
import me.prettyprint.cassandra.service.ThriftKsDef;
import me.prettyprint.hector.api.Cluster;
import me.prettyprint.hector.api.HConsistencyLevel;
import me.prettyprint.hector.api.Keyspace;
import me.prettyprint.hector.api.beans.HColumn;
import me.prettyprint.hector.api.ddl.ColumnFamilyDefinition;
import me.prettyprint.hector.api.ddl.ComparatorType;
import me.prettyprint.hector.api.ddl.KeyspaceDefinition;
import me.prettyprint.hector.api.exceptions.HectorException;
import me.prettyprint.hector.api.factory.HFactory;
import me.prettyprint.hector.api.mutation.Mutator;
import me.prettyprint.hector.api.query.ColumnQuery;
import me.prettyprint.hector.api.query.QueryResult;
import org.apache.http.client.methods.Configurable;

import java.net.UnknownHostException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

/**
 * Created by christina on 7/24/15.
 */
public class CassandraSchemaForAuthors {

    public static StringSerializer serializer=new StringSerializer();

    public static String readAuthorsFromCassandraDB(){
        CassandraHostConfigurator hostConfigurator=new CassandraHostConfigurator("localhost:9610");
        Cluster cluster=HFactory.getOrCreateCluster("TestCluster","localhost:9160");

        KeyspaceDefinition keyspaceDefinition=cluster.describeKeyspace("authorsKeyspace");

        if(cluster.describeKeyspace("authorsKeyspace")==null){
            ColumnFamilyDefinition columnFamilyDefinition=HFactory.createColumnFamilyDefinition("authorsKeyspace", "authors", ComparatorType.BYTESTYPE);
            KeyspaceDefinition keyspaceDefinition1=HFactory.createKeyspaceDefinition("authorsKeyspace",ThriftKsDef.DEF_STRATEGY_CLASS,1,Arrays.asList(columnFamilyDefinition));
            cluster.addKeyspace(keyspaceDefinition1,true);
        }

        ConfigurableConsistencyLevel configurableConsistencyLevel=new ConfigurableConsistencyLevel();
        Map<String,HConsistencyLevel>consistencyLevelMap=new HashMap<String, HConsistencyLevel>();
        consistencyLevelMap.put("AUTHORS",HConsistencyLevel.ONE);
        configurableConsistencyLevel.setReadCfConsistencyLevels(consistencyLevelMap);
        configurableConsistencyLevel.setWriteCfConsistencyLevels(consistencyLevelMap);

        Keyspace keyspace=HFactory.createKeyspace("authorsKeyspace",cluster,configurableConsistencyLevel);
        String[]authorsStringArray=new String[1],author={"author"};
        try{
            ColumnQuery<String,String,String>columnQuery=HFactory.createStringColumnQuery(keyspace);
            for(int i=0;i<authorsStringArray.length;i++) {
                columnQuery.setColumnFamily("AUTHORS").setKey("author").setName(author[i]);
                QueryResult<HColumn<String, String>> result = columnQuery.execute();

                if (result == null) {
                    return null;
                }

                HColumn<String, String> column = result.get();
                if (column == null) {
                    return null;
                }

                authorsStringArray[0] = column.getValue();
                //    System.out.println(authorsStringArray[0]);
            }
        }catch (HectorException ex){
            ex.printStackTrace();
        }
        return authorsStringArray[0];
    }

    public static void writeAuthorsToCassandra(String authors){
        CassandraHostConfigurator hostConfigurator=new CassandraHostConfigurator("localhost:9160");
        Cluster cluster=HFactory.getOrCreateCluster("TestCluster","localhost:9160");

        ConfigurableConsistencyLevel configurableConsistencyLevel=new ConfigurableConsistencyLevel();
        Map<String,HConsistencyLevel>consistencyLevelMap=new HashMap<String, HConsistencyLevel>();
        consistencyLevelMap.put("AUTHORS",HConsistencyLevel.ONE);
        configurableConsistencyLevel.setReadCfConsistencyLevels(consistencyLevelMap);
        configurableConsistencyLevel.setWriteCfConsistencyLevels(consistencyLevelMap);

        KeyspaceDefinition keyspaceDefinition=cluster.describeKeyspace("authorsKeyspace");
        if(cluster.describeKeyspace("authorsKeyspace")==null){
            ColumnFamilyDefinition columnFamilyDefinition=HFactory.createColumnFamilyDefinition("authorsKeyspace","author",ComparatorType.BYTESTYPE);
            KeyspaceDefinition keyspaceDefinition1=HFactory.createKeyspaceDefinition("authorsKeyspace",ThriftKsDef.DEF_STRATEGY_CLASS,1,Arrays.asList(columnFamilyDefinition));
            cluster.addKeyspace(keyspaceDefinition1,true);
        }

        Keyspace keyspace=HFactory.createKeyspace("authorsKeyspace",cluster,configurableConsistencyLevel);
        Mutator<String>mutator=HFactory.createMutator(keyspace,StringSerializer.get());
        try{
            mutator.insert("author","AUTHORS",HFactory.createStringColumn("author",authors));
        }catch (HectorException ex){
            ex.printStackTrace();
        }


    }





}
