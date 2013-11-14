package tutorial;

import java.util.Iterator;
import java.util.Set;
import java.util.HashSet;

import org.apache.commons.configuration.BaseConfiguration;
import org.apache.commons.configuration.Configuration;
import com.thinkaurelius.titan.core.TitanFactory;
import com.thinkaurelius.titan.core.TitanGraph;
import com.tinkerpop.blueprints.Edge;
import com.tinkerpop.blueprints.Vertex;

public class TitanTutorial {
	public static void main(String[] args) {
		long time = System.currentTimeMillis();

		TitanGraph g = getGraph();

		//getCount(g, time + 2000, "Request");
		//getCount(g, time - 2000, "Request");
		//getUniqueCount(g, time + 2000, "Request", "clientIp");
		//getUniqueCount(g, time - 2000, "Request", "clientIp");
		//getWeavingPoints(g, 1384326237000L, "Response");
		getResponseTimeDetail(g, 508362204L);

		g.shutdown();
	}

	public static TitanGraph getGraph() {
		Configuration conf = new BaseConfiguration();
		conf.setProperty("storage.backend", "cassandra");
		conf.setProperty("storage.hostname", "fresto2.owlab.com");
		TitanGraph g = TitanFactory.open(conf);
		return g;

	}

	public static void getResponseTimeDetail(TitanGraph g, long vertexId) {
		Vertex v = g.getVertex(vertexId);
		if(v == null) {
			//TODO
			return;
		} else {
			System.out.println("found vertex by id");
		}

		String uuid = v.getProperty("uuid");
		if(uuid == null) {
			//TODO
			return;
		} else {
			System.out.println("found uuid: " + uuid);
		}

		Iterator<Vertex> vIt = g.getVertices("guuid", uuid).iterator();

		String clientIp = null;
		String url = null;
		int responseTime = 0;
		String entryOperationName = null;
		int entryOperationElapsedTime = 0;
		int controllerElapsedTime = 0;
		int daoElapsedTime = 0;
		int sqlElapsedTime = 0;
		
		String event = null;
		if(vIt.hasNext()) {
			Vertex uuidV = vIt.next();
			for(Vertex vertex: uuidV.query().labels("flow").vertices()) {
				event = vertex.getProperty("event");
				System.out.println("Event: " + event);
				if("Request".equals(event)) {
					url = vertex.getProperty("url");
					clientIp = vertex.getProperty("clientIp");
				}
				if("Response".equals(event)) {
					responseTime = vertex.getProperty("elapsedTime");
				}
				if("EntryOperationReturn".equals(event)) {
					entryOperationElapsedTime = vertex.getProperty("elapsedTime");
				}
				if("OperationReturn".equals(event)) {
					int depth = vertex.getProperty("depth");
					int elapsedTime = vertex.getProperty("elapsedTime");
					if(depth  == 2) {
						controllerElapsedTime += elapsedTime;
					} else if(depth == 3) {
						daoElapsedTime += elapsedTime;
					} else {
						System.out.println("unknown depth = " + depth);
					}
				}
				if("SqlReturn".equals(event)) {
					int elapsedTime = vertex.getProperty("elapsedTime");
					sqlElapsedTime += elapsedTime;
				}
			}
		}

		System.out.println("Client IP: " + clientIp);
		System.out.println("URL: " + url);
		System.out.println("ResponseTime: " + responseTime);
		System.out.println("EntryOpTime: " + entryOperationElapsedTime);
		System.out.println("controllerElapsedTime: " + controllerElapsedTime);
		System.out.println("daoTime: " + daoElapsedTime);
		System.out.println("sqlTime: " + sqlElapsedTime);
	}
	public static void getWeavingPoints(TitanGraph g, long secondInMillis,  String target) {
		long second =  (secondInMillis / 1000) * 1000;

		Iterator<Vertex> it = g.getVertices("second", second).iterator();

		if(it.hasNext()) {
			Vertex v = it.next();
			for(Vertex vertex: v.query().labels("include").has("event", target).vertices()) {
				System.out.println("Vertex ID: " + vertex.getId() + ", uuid: " + vertex.getProperty("uuid"));
				Long id = (Long) vertex.getId();
				System.out.println("UUID by ID: " + g.getVertex(id).getProperty("uuid"));

				Iterator<Vertex> iti = g.getVertices("guuid", vertex.getProperty("uuid")).iterator();
				if(iti.hasNext()) {
					Vertex vu = iti.next();
					for(Vertex vs : vu.query().labels("flow").vertices()) {
						System.out.println("Flow : " + vs.getProperty("event") + "," + vs.getProperty("depth") + "," + vs.getProperty("sequence"));
					}
				}
			}
		}

	}
		
	public static void getUniqueCount(TitanGraph g, long secondInMillis, String target, String key) {
		long count = 0;
		long second = (secondInMillis/1000)*1000;

		//Iterable<Vertex> vertices = g.getVertices("second", second);
		Iterator<Vertex> it = g.getVertices("second", second).iterator();
		Vertex v = null;
		Set<String> strSet = new HashSet<String>();
		if(it.hasNext()) {
			v = it.next();
			for(Vertex vertex : v.query().labels("include").has("event", target).vertices()) {
				System.out.println("ID:" + vertex.getId());
				strSet.add(key);

			}
			count = strSet.size();
		}
		System.out.println("unique:" + count);
	}

	public static void getCount(TitanGraph g, long secondInMillis, String target) {
		long count = 0;
		long second = (secondInMillis/1000)*1000;

		//Iterable<Vertex> vertices = g.getVertices("second", second);
		Iterator<Vertex> it = g.getVertices("second", second).iterator();
		Vertex v = null;
		if(it.hasNext()) {
			v = it.next();
			//count = v.out("include").has("event", target).count();
			count = v.query().labels("include").has("event", target).count();
		}
		System.out.println(count);
	}

	public static void trial01() {
		Configuration conf = new BaseConfiguration();
		conf.setProperty("storage.backend", "cassandra");
		conf.setProperty("storage.hostname", "fresto2.owlab.com");
		TitanGraph g = TitanFactory.open(conf);

		g.createKeyIndex("name", Vertex.class);
		Vertex juno = g.addVertex(null);
		juno.setProperty("name", "juno");
		juno = g.getVertices("name", "juno").iterator().next();

		System.out.println(juno.getProperty("name"));
	}
}
