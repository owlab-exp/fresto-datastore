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
	public static void main(String[] args) {
		initialize();
	}

	public static void initialize() {
		Configuration conf = new BaseConfiguration();
		//Cassandra Configuration
		conf.setProperty("storage.backend", "cassandra");
		conf.setProperty("storage.hostname", "fresto2.owlab.com");

		//Elasticsearch Configuration
		conf.setProperty("storage.index.search.backend", "elasticsearch");
		conf.setProperty("storage.index.search.hostname", "fresto2,fresto3,fresto4");
		//conf.setProperty("storage.index.search.index-name", "fresto");
		conf.setProperty("storage.index.search.client-only", "true");
		TitanGraph g = TitanFactory.open(conf);

		/**
		//// To search event by its name
		////g.makeType().name("event").dataType(String.class).indexed(Vertex.class).unique(Direction.OUT).indexed(Edge.class).unique(Direction.OUT).makePropertyKey();
		////g.makeType().name("uuid").dataType(String.class).indexed(Vertex.class).unique(Direction.OUT).makePropertyKey();
		////g.makeType().name("sequence").dataType(Integer.class).indexed(Vertex.class).unique(Direction.OUT).makePropertyKey();
		////g.makeType().name("timestamp").dataType(Long.class).indexed(Vertex.class).unique(Direction.OUT).makePropertyKey();
		////g.makeType().name("elapsedTime").dataType(Long.class).indexed(Vertex.class).unique(Direction.OUT).makePropertyKey();

		////g.makeType().name("second").dataType(Long.class).indexed(Vertex.class).makePropertyKey();
		*/
		//g.makeType().name("second").dataType(Long.class).indexed(Vertex.class).unique(Direction.OUT).makePropertyKey();
		//g.makeType().name("guuid").dataType(String.class).indexed(Vertex.class).unique(Direction.OUT).makePropertyKey();

		
		g.commit();
		g.shutdown();
	}
}
