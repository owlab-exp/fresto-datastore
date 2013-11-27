package fresto.datastore.titan;

import org.apache.commons.configuration.BaseConfiguration;
import org.apache.commons.configuration.Configuration;
import com.thinkaurelius.titan.core.TitanFactory;
import com.thinkaurelius.titan.core.TitanGraph;
import com.thinkaurelius.titan.core.TitanKey;
import com.thinkaurelius.titan.core.TypeMaker;
import com.tinkerpop.blueprints.Edge;
import com.tinkerpop.blueprints.Vertex;
import com.tinkerpop.blueprints.Direction;

public class TitanInitialization {
	private static String CASSANDRA_HOST;

	public static void main(String[] args) {
		if(args.length < 1) {
			System.out.println("An argument needed: <Host name of Cassandra>");
			System.exit(1);
		}
		CASSANDRA_HOST = args[0];

		initialize();
	}

	public static void initialize() {
		Configuration conf = new BaseConfiguration();
		//Cassandra Configuration
		conf.setProperty("storage.backend", "cassandra");
		conf.setProperty("storage.hostname", CASSANDRA_HOST);

		//Elasticsearch Configuration
		TitanGraph g = TitanFactory.open(conf);

		/**
		*/
		g.makeType().name("second").dataType(Long.class).indexed(Vertex.class).unique(Direction.OUT).makePropertyKey();
		g.makeType().name("guuid").dataType(String.class).indexed(Vertex.class).unique(Direction.OUT).makePropertyKey();

		
		g.commit();
		g.shutdown();
	}
}
