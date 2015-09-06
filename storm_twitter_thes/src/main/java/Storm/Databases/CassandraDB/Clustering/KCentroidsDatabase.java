package Storm.Databases.CassandraDB.Clustering;

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
import me.prettyprint.hector.api.query.QueryResult;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

/**
 * Created by christina on 7/17/15.
 */
public class KCentroidsDatabase {


    private static me.prettyprint.cassandra.serializers.StringSerializer stringSerializer= me.prettyprint.cassandra.serializers.StringSerializer.get();

    public static String[]getSerializedClusterMap(){
        CassandraHostConfigurator hostConfigurator=new CassandraHostConfigurator("localhost:9160");
        Cluster cluster=HFactory.getOrCreateCluster("TestCluster", "localhost:9160");

        KeyspaceDefinition keyspaceDefinition=cluster.describeKeyspace("myKeyspace");

        if (cluster.describeKeyspace("myKeyspace")==null){
            ColumnFamilyDefinition columnFamilyDefinition=HFactory.createColumnFamilyDefinition("myKeyspace","clusters",ComparatorType.BYTESTYPE);
            KeyspaceDefinition keyspaceDefinition1=HFactory.createKeyspaceDefinition("myKeyspace",  ThriftKsDef.DEF_STRATEGY_CLASS,1,Arrays.asList(columnFamilyDefinition));
            cluster.addKeyspace(keyspaceDefinition1,true);

        }

        ConfigurableConsistencyLevel configurableConsistencyLevel=new ConfigurableConsistencyLevel();
        Map<String,HConsistencyLevel>consistencyLevelMap=new HashMap<String, HConsistencyLevel>();

        consistencyLevelMap.put("user",HConsistencyLevel.ONE);
        configurableConsistencyLevel.setReadCfConsistencyLevels(consistencyLevelMap);
        configurableConsistencyLevel.setWriteCfConsistencyLevels(consistencyLevelMap);

        Keyspace keyspace=HFactory.createKeyspace("myKeyspace", cluster,configurableConsistencyLevel);
        String[]serializedVectors=new String[2],clusters={"cluster-0","cluster-1"};
        try{
            me.prettyprint.hector.api.query.ColumnQuery<String,String,String> columnQuery=HFactory.createStringColumnQuery(keyspace);
            for(int i=0;i<clusters.length;i++){
                columnQuery.setColumnFamily("user").setKey("cluster").setName(clusters[i]);
                QueryResult<HColumn<String,String>>result=columnQuery.execute();

                if(result==null) {
                    return null;
                }
                HColumn<String,String>column=result.get();

                if(column==null){
                    return null;
                }
                serializedVectors[i]=column.getValue();
            }
        }catch (HectorException ex){
            ex.printStackTrace();
        }
        return serializedVectors;
    }

    public static void setSerializedMap(int index,String serializedVector){
        CassandraHostConfigurator hostConfigurator=new CassandraHostConfigurator("localhost:9160");
        Cluster cluster=HFactory.getOrCreateCluster("TestCluster", "localhost:9160");

        ConfigurableConsistencyLevel configurableConsistencyLevel=new ConfigurableConsistencyLevel();
        Map<String,HConsistencyLevel>consistencyLevelMap=new HashMap<String, HConsistencyLevel>();

        consistencyLevelMap.put("user",HConsistencyLevel.ONE);
        configurableConsistencyLevel.setReadCfConsistencyLevels(consistencyLevelMap);
        configurableConsistencyLevel.setWriteCfConsistencyLevels(consistencyLevelMap);

        KeyspaceDefinition keyspaceDefinition=cluster.describeKeyspace("myKeyspace");

        if (cluster.describeKeyspace("myKeyspace")==null){
            ColumnFamilyDefinition columnFamilyDefinition=HFactory.createColumnFamilyDefinition("myKeyspace","cluster",ComparatorType.BYTESTYPE);
            KeyspaceDefinition keyspaceDefinition1=HFactory.createKeyspaceDefinition("myKeyspace",  ThriftKsDef.DEF_STRATEGY_CLASS,1,Arrays.asList(columnFamilyDefinition));
            cluster.addKeyspace(keyspaceDefinition1,true);
        }

        Keyspace keyspace=HFactory.createKeyspace("myKeyspace", cluster,configurableConsistencyLevel);

        Mutator<String> mutator = HFactory.createMutator(keyspace, me.prettyprint.cassandra.serializers.StringSerializer.get());
        try{
            mutator.insert("cluster","user", HFactory.createStringColumn("cluster-" + index, serializedVector));
        }catch(HectorException e){
            e.printStackTrace();
        }

    }
}
