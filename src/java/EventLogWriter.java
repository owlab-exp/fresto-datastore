/**************************************************************************************
 * Copyright 2013 TheSystemIdeas, Inc and Contributors. All rights reserved.          *
 *                                                                                    *
 *     https://github.com/owlab/fresto                                                *
 *                                                                                    *
 *                                                                                    *
 * ---------------------------------------------------------------------------------- *
 * This file is licensed under the Apache License, Version 2.0 (the "License");       *
 * you may not use this file except in compliance with the License.                   *
 * You may obtain a copy of the License at http://www.apache.org/licenses/LICENSE-2.0 * 
 **************************************************************************************/
package fresto.datastore;

import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.HashMap;
import java.util.logging.Logger;
import java.io.IOException;

import org.apache.thrift.TDeserializer;
import org.apache.thrift.TException;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TBinaryProtocol.Factory;

import org.zeromq.ZMQ;
import org.zeromq.ZMQ.Context;
import org.zeromq.ZMQ.Socket;

import fresto.data.FrestoData;
import fresto.data.Pedigree;
import fresto.data.DataUnit;
import fresto.data.ClientID;
import fresto.data.ResourceID;
import fresto.data.OperationID;
import fresto.data.SqlID;
import fresto.data.RequestEdge;
import fresto.data.ResponseEdge;
import fresto.data.EntryOperationCallEdge;
import fresto.data.EntryOperationReturnEdge;
import fresto.data.OperationCallEdge;
import fresto.data.OperationReturnEdge;
import fresto.data.SqlCallEdge;
import fresto.data.SqlReturnEdge;
import fresto.command.CommandEvent;

import fresto.datastore.FrestoEventQueue;
import fresto.datastore.FrestoEvent;
//import org.perf4j.javalog.JavaLogStopWatch;
//import org.perf4j.StopWatch;

//import org.apache.commons.configuration.BaseConfiguration;
//import org.apache.commons.configuration.Configuration;
//import com.thinkaurelius.titan.core.TitanFactory;
//import com.thinkaurelius.titan.core.TitanGraph;
//import com.tinkerpop.blueprints.Edge;
//import com.tinkerpop.blueprints.Vertex;

public class EventLogWriter {
	private static String THIS_CLASS_NAME = "EventLogWriter";
	private static Logger LOGGER = Logger.getLogger(THIS_CLASS_NAME);

	private static String frontHost;
	private static String frontPort;
	private static String subOrPull;

	// Client Events
	private static final String TOPIC_REQUEST = "CB";
	private static final String TOPIC_RESPONSE = "CF";

	// Server Events
	private static final String TOPIC_ENTRY_CALL = "EB";
	private static final String TOPIC_ENTRY_RETURN = "EF";
	private static final String TOPIC_OPERATION_CALL = "OB";
	private static final String TOPIC_OPERATION_RETURN = "OF";
	private static final String TOPIC_SQL_CALL = "SB";
	private static final String TOPIC_SQL_RETURN = "SF";

	// Command Events
	private static final String TOPIC_COMMAND_EVENT = "CMD";
	
	//private FrestoData frestoData = new FrestoData();
	private static TDeserializer deserializer = new TDeserializer(new TBinaryProtocol.Factory());
	

	private static volatile boolean work = true;
	private static int SLEEP_TIME = 10;
	//private static TitanGraph g;
	//private static int maxCommitCount = 1000;

	public EventLogWriter() {

	}

	public static void main(String[] args) throws Exception {
		if(args.length !=  2) {
			LOGGER.severe("Argumests needed : <frontHost> <frontPort>");
			System.exit(1);
		} else {
				frontHost = args[0];
				frontPort = args[1]; 
				LOGGER.info("Connecting... " + frontHost + ":" + frontPort + " with SUB");
		}

		final ZMQ.Context context = ZMQ.context(1);

		final FrestoEventQueue frestoEventQueue = new FrestoEventQueue();
		
		final Thread queueMonitorThread = new Thread() {
			Logger _LOGGER = Logger.getLogger("logWriteThread");
			@Override
			public void run() {
				while(work) {
					try {
						_LOGGER.info("frestoEventQueue size = " + frestoEventQueue.size());
						Thread.sleep(1000);
					} catch(InterruptedException ie) {
					}

				}
			}
		};


		final Thread logWriteThread = new Thread() {
			Logger _LOGGER = Logger.getLogger("logWriteThread");
			@Override
			public void run() {
				//FrestoStopWatch _watch = new FrestoStopWatch();
				//FrestoStopWatch _durationWatch = new FrestoStopWatch();

				EventLogWriter eventLogWriter = new EventLogWriter();


				// Open database
				//eventLogWriter.openTitanGraph();

				ZMQ.Socket receiver = null;
				//if("pull".equalsIgnoreCase(subOrPull)) {
				//	receiver = context.socket(ZMQ.PULL);
				//	receiver.connect("tcp://" + frontHost + ":" + frontPort);
				//} else if("sub".equalsIgnoreCase(subOrPull)) {
					receiver = context.socket(ZMQ.SUB);
					receiver.connect("tcp://" + frontHost + ":" + frontPort);
					receiver.subscribe("".getBytes());
				//} else {
				//	LOGGER.severe(subOrPull + " is not supported.");
				//	System.exit(1);
				//}


				//Consume socket data
				frestoEventQueue.setPullerSocket(receiver);
				frestoEventQueue.start();
				
				int waitingEventCount = 0;
				//int count = 0;
				//long elapsedTime = 0;
				//long duration = 0;
				
				//_durationWatch.start();

				while(work) {

					// To wait until at least one event in queue
					if(frestoEventQueue.isEmpty()) {
						try {
							//_LOGGER.info("FrestoEventQueue is empty. Waiting " + SLEEP_TIME + "ms...");
							Thread.sleep(SLEEP_TIME);
							continue;
						} catch(InterruptedException ie) {
						}
					}

					waitingEventCount = frestoEventQueue.size();

					for(int i = 0; i < waitingEventCount; i++) {
						//_watch.start();
						//count++;

						FrestoEvent frestoEvent = frestoEventQueue.poll(); 

						try {
							eventLogWriter.writeEventData(frestoEvent.topic, frestoEvent.eventBytes);
						} catch(Exception e) {
							e.printStackTrace();
						} finally {
							//
						}

						//elapsedTime += _watch.stop();
						//duration += _durationWatch.stop();

						//if(count == maxCommitCount) {
						//	eventLogWriter.commitGraph();
						//	_LOGGER.info(count + " events processed for " + elapsedTime + " ms. (total time " + duration + " ms.) Remaining events " + frestoEventQueue.size());
						//	
						//	count = 0;
						//	elapsedTime = 0;
						//	duration = 0;
						//	// Stop FOR clause
						//}
					}

					//eventLogWriter.commitGraph();

					_LOGGER.info("Remaining events " + frestoEventQueue.size());

					//count = 0;
					//elapsedTime = 0;
					//duration = 0;
				}
				_LOGGER.info("Shutting down...");

				//if(g.isOpen()) {
				//	g.commit();
				//	g.shutdown();
				//}

				receiver.close();
				context.term();

				_LOGGER.info("Good bye.");
			}
		};

		Runtime.getRuntime().addShutdownHook(new Thread() {
         		@Override
         		public void run() {
         		   System.out.println(" Interrupt received, killing logger¡¦");
			   // To break while clause
			   frestoEventQueue.stopWork();
			   work = false;

         		  try {
				  	logWriteThread.join();
				  	frestoEventQueue.join();
				  	//queueMonitorThread.join();

         		  } catch (InterruptedException e) {
					//
         		  }
         		}
      		});

		//queueMonitorThread.start();
		logWriteThread.start();

	}

	//public TitanGraph openTitanGraph() {
	//	//OGraphDatabase oGraph = new OGraphDatabase(DB_URL);
	//	LOGGER.info("Setting up Titan"); 
	//	Configuration config = new BaseConfiguration();
	//	config.setProperty("storage.backend", "cassandra");
	//	config.setProperty("storage.hostname", subOrPull);
	//	config.setProperty("storage.connection-pool-size", 8);
	//	
	//	g = TitanFactory.open(config);
	//	return g;
	//}

	//public void commitGraph() {
	//	g.commit();
	//}

	public void writeEventData(String topic, byte[] eventBytes) throws TException, IOException {

		if(TOPIC_REQUEST.equals(topic) 
			|| TOPIC_RESPONSE.equals(topic)
			|| TOPIC_ENTRY_CALL.equals(topic)
			|| TOPIC_ENTRY_RETURN.equals(topic)
			|| TOPIC_OPERATION_CALL.equals(topic)
			|| TOPIC_OPERATION_RETURN.equals(topic)
			|| TOPIC_SQL_CALL.equals(topic)
			|| TOPIC_SQL_RETURN.equals(topic)
			) {

			FrestoData frestoData = new FrestoData();
			deserializer.deserialize(frestoData, eventBytes);

			Pedigree pedigree = new Pedigree();
            pedigree.setReceivedTime(System.currentTimeMillis());

            frestoData.setPedigree(pedigree);

			System.out.println("=================>");
			if(frestoData.dataUnit.isSetRequestEdge()) {

				RequestEdge requestEdge = frestoData.dataUnit.getRequestEdge();
				ClientID clientId = requestEdge.clientId;
				ResourceID resourceId = requestEdge.resourceId;

				//Vertex v = g.addVertex(null);
				System.out.println("event: " + "Request");
				System.out.println("clientIp: " + clientId.getClientIp());
				System.out.println("url: " + resourceId.getUrl());
				System.out.println("referrer: " + requestEdge.referrer);
				System.out.println("method: " + requestEdge.method);
				System.out.println("timestamp: " + requestEdge.timestamp);
				System.out.println("uuid: " + requestEdge.uuid);
				

				//_watch.lap("Request event processed");
				////linkToTS(g, v, requestEdge.timestamp);
				////linkToGUUID(g, v, requestEdge.uuid);
				//_watch.stop("Link event processed");

			} else if(frestoData.dataUnit.isSetResponseEdge()) {

				ResponseEdge responseEdge = frestoData.dataUnit.getResponseEdge();
				ClientID clientId = responseEdge.clientId;
				ResourceID resourceId = responseEdge.resourceId;

				//StopWatch _watch = new LoggingStopWatch("Writing Response Event");

				//ODocument response = oGraph.create//Vertex("Response")
				//Vertex v = g.add//Vertex(null);
					System.out.println("event: " + "Response");
					System.out.println("clientIp: " + clientId.getClientIp());
					System.out.println("url: " + resourceId.getUrl());
					System.out.println("httpStatus: " + responseEdge.httpStatus);
					System.out.println("elapsedTime: " + responseEdge.elapsedTime);
					System.out.println("timestamp: " + responseEdge.timestamp);
					System.out.println("uuid: " + responseEdge.uuid);
				

				//_watch.lap("Response event processed");
				//linkToTS(g, v, responseEdge.timestamp);
				//linkToGUUID(g, v, responseEdge.uuid);
				////linkToTS(oGraph, tempDoc.getIdentity(), "response: " + responseEdge.timestamp);
				//////linkToTS(oGraph, response.getIdentity(), "response: " + responseEdge.timestamp);
				//_watch.stop("Link event processed");

			} else if(frestoData.dataUnit.isSetEntryOperationCallEdge()) {

				EntryOperationCallEdge entryOperationCallEdge = frestoData.dataUnit.getEntryOperationCallEdge();
				ResourceID resourceId = entryOperationCallEdge.resourceId;
				OperationID operationId = entryOperationCallEdge.operationId;

				//StopWatch _watch = new LoggingStopWatch("Writing EntryOperationCall");

				//ODocument entryCall = oGraph.create//Vertex("EntryOperationCall")
				//Vertex v = g.add//Vertex(null);
					System.out.println("event: " + "EntryOperationCall");
					System.out.println("hostName: " + entryOperationCallEdge.localHost);
					System.out.println("contextPath: " + entryOperationCallEdge.contextPath);
					System.out.println("port: " + entryOperationCallEdge.localPort);
					System.out.println("servletPath: " + entryOperationCallEdge.servletPath);
					System.out.println("operationName: " + operationId.getOperationName());
					System.out.println("typeName: " + operationId.getTypeName());
					System.out.println("httpMethod: " + entryOperationCallEdge.httpMethod);
					System.out.println("uuid: " + entryOperationCallEdge.uuid);
					System.out.println("timestamp: " + entryOperationCallEdge.timestamp);
					System.out.println("sequence: " + entryOperationCallEdge.sequence);
					System.out.println("depth: " + entryOperationCallEdge.depth);
				


				//_watch.lap("EntryOperationCall event processed");
				//linkToTS(g, v, entryOperationCallEdge.timestamp);
				//linkToGUUID(g, v, entryOperationCallEdge.uuid);
				////linkToTS(oGraph, tempDoc.getIdentity(), "entryCall: " + entryOperationCallEdge.timestamp);
				//////linkToTS(oGraph, entryCall.getIdentity(), "entryCall: " + entryOperationCallEdge.timestamp);
				//_watch.stop("Link event processed");




			} else if(frestoData.dataUnit.isSetEntryOperationReturnEdge()) {

				EntryOperationReturnEdge entryOperationReturnEdge = frestoData.dataUnit.getEntryOperationReturnEdge();
				ResourceID resourceId = entryOperationReturnEdge.resourceId;
				OperationID operationId = entryOperationReturnEdge.operationId;

				//StopWatch _watch = new LoggingStopWatch("Writing EntryOperationReturn");

				//ODocument entryReturn = oGraph.create//Vertex("EntryOperationReturn")
				//Vertex v = g.add//Vertex(null);
					System.out.println("event: " + "EntryOperationReturn");
					System.out.println("servletlPath: " + entryOperationReturnEdge.servletPath);
					System.out.println("operationName: " + operationId.getOperationName());
					System.out.println("typeName: " + operationId.getTypeName());
					System.out.println("httpStatus: " + entryOperationReturnEdge.httpStatus);
					System.out.println("timestamp: " + entryOperationReturnEdge.timestamp);
					System.out.println("elapsedTime: " + entryOperationReturnEdge.elapsedTime);
					System.out.println("uuid: " + entryOperationReturnEdge.uuid);
					System.out.println("sequence: " + entryOperationReturnEdge.sequence);
					System.out.println("depth: " + entryOperationReturnEdge.depth);
				

				//_watch.lap("EntryOperationReturn event processed");
				//linkToTS(g, v, entryOperationReturnEdge.timestamp);
				//linkToGUUID(g, v, entryOperationReturnEdge.uuid);
				////linkToTS(oGraph, tempDoc.getIdentity(), "entryReturn: " + entryOperationReturnEdge.timestamp);
				//////linkToTS(oGraph, entryReturn.getIdentity(), "entryReturn: " + entryOperationReturnEdge.timestamp);
				//_watch.stop("Link event processed");


			} else if(frestoData.dataUnit.isSetOperationCallEdge()) {

				OperationCallEdge operationCallEdge = frestoData.dataUnit.getOperationCallEdge();
				OperationID operationId = operationCallEdge.operationId;

				//StopWatch _watch = new LoggingStopWatch("Writing OperationCall");

				//ODocument operationCall = oGraph.create//Vertex("OperationCall")
				//Vertex v = g.add//Vertex(null);
					System.out.println("event: " + "OperationCall");
					System.out.println("operationName: " + operationId.getOperationName());
					System.out.println("typeName: " + operationId.getTypeName());
					System.out.println("timestamp: " + operationCallEdge.timestamp);
					System.out.println("uuid: " + operationCallEdge.uuid);
					System.out.println("depth: " + operationCallEdge.depth);
					System.out.println("sequence: " + operationCallEdge.sequence);
				

				//_watch.lap("OperationCall event processed");
				//linkToTS(g, v, operationCallEdge.timestamp);
				//linkToGUUID(g, v, operationCallEdge.uuid);
				////linkToTS(oGraph, tempDoc.getIdentity(), "operationCall: " + operationCallEdge.timestamp);
				//////linkToTS(oGraph, operationCall.getIdentity(), "operationCall: " + operationCallEdge.timestamp);
				//_watch.stop("Link event processed");

			} else if(frestoData.dataUnit.isSetOperationReturnEdge()) {
				OperationReturnEdge operationReturnEdge = frestoData.dataUnit.getOperationReturnEdge();
				OperationID operationId = operationReturnEdge.operationId;

				//StopWatch _watch = new LoggingStopWatch("Writing OperationReturn");

				//ODocument operationReturn = oGraph.create//Vertex("OperationReturn")
				//Vertex v = g.add//Vertex(null);
					System.out.println("event: " + "OperationReturn");
					System.out.println("operationName: " + operationId.getOperationName());
					System.out.println("operationName: " + operationId.getOperationName());
					System.out.println("typeName: " + operationId.getTypeName());
					System.out.println("timestamp: " + operationReturnEdge.timestamp);
					System.out.println("elapsedTime: " + operationReturnEdge.elapsedTime);
					System.out.println("uuid: " + operationReturnEdge.uuid);
					System.out.println("sequence: " + operationReturnEdge.sequence);
					System.out.println("depth: " + operationReturnEdge.depth);
				

				//_watch.lap("OperationReturn event processed");
				//linkToTS(g, v, operationReturnEdge.timestamp);
				//linkToGUUID(g, v, operationReturnEdge.uuid);
				////linkToTS(oGraph, tempDoc.getIdentity(), "operationReturn: " + operationReturnEdge.timestamp);
				//////linkToTS(oGraph, operationReturn.getIdentity(), "operationReturn: " + operationReturnEdge.timestamp);
				//_watch.stop("Link event processed");
			} else if(frestoData.dataUnit.isSetSqlCallEdge()) {

				SqlCallEdge sqlCallEdge = frestoData.dataUnit.getSqlCallEdge();
				SqlID sqlId = sqlCallEdge.sqlId;

				//StopWatch _watch = new LoggingStopWatch("Writing SqlCall");

				//ODocument sqlCall = oGraph.create//Vertex("SqlCall")
				//Vertex v = g.add//Vertex(null);
					System.out.println("event: " + "SqlCall");
					//System.out.println("databaseUrl: " + sqlId.getDatabaseUrl());
					System.out.println("sql: " + sqlId.getSql());
					System.out.println("timestamp: " + sqlCallEdge.timestamp);
					System.out.println("uuid: " + sqlCallEdge.uuid);
					System.out.println("depth: " + sqlCallEdge.depth);
					System.out.println("sequence: " + sqlCallEdge.sequence);
				

				//_watch.lap("SqlCall event processed");
				//linkToTS(g, v, sqlCallEdge.timestamp);
				//linkToGUUID(g, v, sqlCallEdge.uuid);
				////linkToTS(oGraph, tempDoc.getIdentity(), "sqlCall: " + sqlCallEdge.timestamp);
				//////linkToTS(oGraph, sqlCall.getIdentity(), "sqlCall: " + sqlCallEdge.timestamp);
				//_watch.stop("Link event processed");

			} else if(frestoData.dataUnit.isSetSqlReturnEdge()) {
				SqlReturnEdge sqlReturnEdge = frestoData.dataUnit.getSqlReturnEdge();
				SqlID sqlId = sqlReturnEdge.sqlId;

				//StopWatch _watch = new LoggingStopWatch("Writing SqlReturn");

				//ODocument sqlReturn = oGraph.create//Vertex("SqlReturn")
				//Vertex v = g.add//Vertex(null);
					System.out.println("event: " + "SqlReturn");
					//System.out.println("databaseUrl: " + sqlId.getDatabaseUrl());
					System.out.println("sql: " + sqlId.getSql());
					System.out.println("timestamp: " + sqlReturnEdge.timestamp);
					System.out.println("elapsedTime: " + sqlReturnEdge.elapsedTime);
					System.out.println("uuid: " + sqlReturnEdge.uuid);
					System.out.println("depth: " + sqlReturnEdge.depth);
					System.out.println("sequence: " + sqlReturnEdge.sequence);
				

				//_watch.lap("SqlReturn event processed");
				//linkToTS(g, v, sqlReturnEdge.timestamp);
				//linkToGUUID(g, v, sqlReturnEdge.uuid);
				////linkToTS(oGraph, tempDoc.getIdentity(), "sqlReturn", sqlReturnEdge.timestamp);
				//linkToTS(oGraph, sqlReturn.getIdentity(), "sqlReturn", sqlReturnEdge.timestamp);
				//_watch.stop("Link event processed");
			} else {
				LOGGER.info("No data unit exist.");
			}

			
		} else {
			LOGGER.warning("Event topic: " + topic + " not recognized.");
		}
	}

//        public static ODocument findOne(OGraphDatabase oGraph, OSQLSynchQuery oQuery, Map<String, Object> params) {
//		List<ODocument> result = oGraph.command(oQuery).execute(params);
//                for(ODocument doc: result) {
//                                LOGGER.fine("Found.");
//                                return doc;
//                }
//                return null;
//
//        }
//
//	public static ODocument lookForVertex(OGraphDatabase oGraph, String indexName, Object key) {
//		ODocument vertex = null;
//		OIndex<?> idx = oGraph.getMetadata().getIndexManager().getIndex(indexName);
//		if(idx != null) {
//			OIdentifiable rec = (OIdentifiable) idx.get(key);
//			if(rec != null) {
//				vertex = oGraph.getRecord(rec);
//			} else {
//				LOGGER.info("ORID: " + rec + " does not exist");
//			}
//		} else {
//			LOGGER.info("INDEX: " + idx + " does not exist");
//		}
//
//		return vertex;
//	}
//
//	public static void linkToTS(TitanGraph g, Vertex v, long timestamp) {
//		long second = (timestamp/1000) * 1000;
//		long minute = (timestamp/60000) * 60000;
//
//		//Index<Long> secondIndex = g.getIndex("second", Vertex.class);
//		Iterable<Vertex> vertices = g.getVertices("second", second);
//		//Iterable<Vertex> vertices = secondIndex.get("second", second);
//		Vertex secondVertex = null;
//		if(vertices.iterator().hasNext()) {
//			secondVertex = vertices.iterator().next();
//		} else {
//			secondVertex = g.addVertex(null);
//			secondVertex.setProperty("second", second);
//			//g.commit();
//			//LOGGER.info("Second[" + second + "] created.");
//		}
//		secondVertex.addEdge("include", v).setProperty("event", v.getProperty("event"));
//	}
//
//	public static void linkToGUUID(TitanGraph g, Vertex v, String uuid) {
//
//		Iterable<Vertex> vertices = g.getVertices("guuid", uuid);
//		Vertex guuidVertex = null;
//		if(vertices.iterator().hasNext()) {
//			guuidVertex = vertices.iterator().next();
//		} else {
//			guuidVertex = g.addVertex(null);
//			guuidVertex.setProperty("guuid", uuid);
//			//g.commit();
//			//LOGGER.info("GUUID[" + uuid + "] created.");
//		}
//		guuidVertex.addEdge("flow", v).setProperty("event", v.getProperty("event"));
//	}
}

