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

//import org.perf4j.javalog.JavaLogStopWatch;
//import org.perf4j.StopWatch;

import org.apache.commons.configuration.BaseConfiguration;
import org.apache.commons.configuration.Configuration;
import com.thinkaurelius.titan.core.TitanFactory;
import com.thinkaurelius.titan.core.TitanGraph;
import com.tinkerpop.blueprints.Edge;
import com.tinkerpop.blueprints.Vertex;

public class TitanEventWriterD {
	private static String THIS_CLASS_NAME = "TitanEventWriterD";
	private static Logger LOGGER = Logger.getLogger(THIS_CLASS_NAME);

	private static String frontHost;
	private static String frontPort;
	private static String dbHost;

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
	private static TitanGraph g;
	private static int maxCommitCount = 1000;

	public TitanEventWriterD() {

	}

	public static void main(String[] args) throws Exception {
		if(args.length <  3) {
			LOGGER.severe("Argumests needed : <frontHost> <frontPort> <dbHost> <max commit count>");
			System.exit(1);
		} else {
				frontHost = args[0];
				frontPort = args[1]; 
				dbHost = args[2];
				if(args.length == 3) {
					LOGGER.info("Default commit count (" + maxCommitCount + ") used.");
				} else {
					try {
						maxCommitCount = Integer.parseInt(args[3]);
					} catch(NumberFormatException e) {
						LOGGER.severe("Commit count should be an integer");
					}
				}
		}

		final ZMQ.Context context = ZMQ.context(1);

		final FrestoEventQueue frestoEventQueue = new FrestoEventQueue();
		
		final Thread queueMonitorThread = new Thread() {
			Logger _LOGGER = Logger.getLogger("writerThread");
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


		final Thread writerThread = new Thread() {
			Logger _LOGGER = Logger.getLogger("writerThread");
			@Override
			public void run() {
				//StopWatch _watch = new JavaLogStopWatch(_LOGGER);
				FrestoStopWatch _watch = new FrestoStopWatch();
				FrestoStopWatch _durationWatch = new FrestoStopWatch();

				TitanEventWriterD eventWriter = new TitanEventWriterD();


				// Open database
				eventWriter.openTitanGraph();

				ZMQ.Socket puller = context.socket(ZMQ.PULL);
				puller.connect("tcp://" + frontHost + ":" + frontPort);

				//Consume socket data
				frestoEventQueue.setPullerSocket(puller);
				frestoEventQueue.start();
				
				int nextCommitCount = 0;
				int count = 0;
				long elapsedTime = 0;
				long duration = 0;
				
				_durationWatch.start();

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

					nextCommitCount = frestoEventQueue.size();

					for(int i = 0; i < nextCommitCount; i++) {
						_watch.start();
						count++;

						FrestoEvent frestoEvent = frestoEventQueue.poll(); 

						try {
							eventWriter.writeEventData(frestoEvent.topic, frestoEvent.eventBytes);
						} catch(Exception e) {
							e.printStackTrace();
						} finally {
							//
						}

						elapsedTime += _watch.stop();
						duration += _durationWatch.stop();

						if(count == maxCommitCount) {
							eventWriter.commitGraph();
							_LOGGER.info(count + " events processed for " + elapsedTime + " ms. (total time " + duration + " ms.) Remaining events " + frestoEventQueue.size());
							
							count = 0;
							elapsedTime = 0;
							duration = 0;
							// Stop FOR clause
						}
					}

					eventWriter.commitGraph();

					_LOGGER.info(count + " events processed for " + elapsedTime + " ms. (total time " + duration + " ms.) Remaining events " + frestoEventQueue.size());

					count = 0;
					elapsedTime = 0;
					duration = 0;
				}
				_LOGGER.info("Shutting down...");

				if(g.isOpen()) {
					g.commit();
					g.shutdown();
				}

				puller.close();
				context.term();

				_LOGGER.info("Good bye.");
			}
		};

		Runtime.getRuntime().addShutdownHook(new Thread() {
         		@Override
         		public void run() {
         		   System.out.println("Interrupt received, killing server¡¦");
			   // To break while clause
			   frestoEventQueue.stopWork();
			   work = false;

         		  try {
				  writerThread.join();
				  frestoEventQueue.join();
				  //queueMonitorThread.join();

         		  } catch (InterruptedException e) {
         		  }
         		}
      		});

		//queueMonitorThread.start();
		writerThread.start();

	}

	public TitanGraph openTitanGraph() {
		//OGraphDatabase oGraph = new OGraphDatabase(DB_URL);
		LOGGER.info("Setting up Titan"); 
		Configuration config = new BaseConfiguration();
		config.setProperty("storage.backend", "cassandra");
		config.setProperty("storage.hostname", dbHost);
		config.setProperty("storage.connection-pool-size", 8);
		
		g = TitanFactory.open(config);
		return g;
	}

	public void commitGraph() {
		g.commit();
	}

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

			if(frestoData.dataUnit.isSetRequestEdge()) {

				RequestEdge requestEdge = frestoData.dataUnit.getRequestEdge();
				ClientID clientId = requestEdge.clientId;
				ResourceID resourceId = requestEdge.resourceId;

				Vertex v = g.addVertex(null);
				v.setProperty("event", "Request");
				v.setProperty("clientIp", clientId.getClientIp());
				v.setProperty("url", resourceId.getUrl());
				v.setProperty("referrer", requestEdge.referrer);
				v.setProperty("method", requestEdge.method);
				v.setProperty("timestamp", requestEdge.timestamp);
				v.setProperty("uuid", requestEdge.uuid);
				

				//_watch.lap("Request event processed");
				linkToTS(g, v, requestEdge.timestamp);
				linkToGUUID(g, v, requestEdge.uuid);
				//_watch.stop("Link event processed");

			} else if(frestoData.dataUnit.isSetResponseEdge()) {

				ResponseEdge responseEdge = frestoData.dataUnit.getResponseEdge();
				ClientID clientId = responseEdge.clientId;
				ResourceID resourceId = responseEdge.resourceId;

				//StopWatch _watch = new LoggingStopWatch("Writing Response Event");

				//ODocument response = oGraph.createVertex("Response")
				Vertex v = g.addVertex(null);
					v.setProperty("event", "Response");
					v.setProperty("clientIp", clientId.getClientIp());
					v.setProperty("url", resourceId.getUrl());
					v.setProperty("httpStatus", responseEdge.httpStatus);
					v.setProperty("elapsedTime", responseEdge.elapsedTime);
					v.setProperty("timestamp", responseEdge.timestamp);
					v.setProperty("uuid", responseEdge.uuid);
				

				//_watch.lap("Response event processed");
				linkToTS(g, v, responseEdge.timestamp);
				linkToGUUID(g, v, responseEdge.uuid);
				//linkToTS(oGraph, tempDoc.getIdentity(), "response", responseEdge.timestamp);
				////linkToTS(oGraph, response.getIdentity(), "response", responseEdge.timestamp);
				//_watch.stop("Link event processed");

			} else if(frestoData.dataUnit.isSetEntryOperationCallEdge()) {

				EntryOperationCallEdge entryOperationCallEdge = frestoData.dataUnit.getEntryOperationCallEdge();
				ResourceID resourceId = entryOperationCallEdge.resourceId;
				OperationID operationId = entryOperationCallEdge.operationId;

				//StopWatch _watch = new LoggingStopWatch("Writing EntryOperationCall");

				//ODocument entryCall = oGraph.createVertex("EntryOperationCall")
				Vertex v = g.addVertex(null);
					v.setProperty("event", "EntryOperationCall");
					v.setProperty("hostName", entryOperationCallEdge.localHost);
					v.setProperty("contextPath", entryOperationCallEdge.contextPath);
					v.setProperty("port", entryOperationCallEdge.localPort);
					v.setProperty("servletPath", entryOperationCallEdge.servletPath);
					v.setProperty("operationName", operationId.getOperationName());
					v.setProperty("typeName", operationId.getTypeName());
					v.setProperty("httpMethod", entryOperationCallEdge.httpMethod);
					v.setProperty("uuid", entryOperationCallEdge.uuid);
					v.setProperty("timestamp", entryOperationCallEdge.timestamp);
					v.setProperty("sequence", entryOperationCallEdge.sequence);
					v.setProperty("depth", entryOperationCallEdge.depth);
				


				//_watch.lap("EntryOperationCall event processed");
				linkToTS(g, v, entryOperationCallEdge.timestamp);
				linkToGUUID(g, v, entryOperationCallEdge.uuid);
				//linkToTS(oGraph, tempDoc.getIdentity(), "entryCall", entryOperationCallEdge.timestamp);
				////linkToTS(oGraph, entryCall.getIdentity(), "entryCall", entryOperationCallEdge.timestamp);
				//_watch.stop("Link event processed");




			} else if(frestoData.dataUnit.isSetEntryOperationReturnEdge()) {

				EntryOperationReturnEdge entryOperationReturnEdge = frestoData.dataUnit.getEntryOperationReturnEdge();
				ResourceID resourceId = entryOperationReturnEdge.resourceId;
				OperationID operationId = entryOperationReturnEdge.operationId;

				//StopWatch _watch = new LoggingStopWatch("Writing EntryOperationReturn");

				//ODocument entryReturn = oGraph.createVertex("EntryOperationReturn")
				Vertex v = g.addVertex(null);
					v.setProperty("event", "EntryOperationReturn");
					v.setProperty("servletlPath", entryOperationReturnEdge.servletPath);
					v.setProperty("operationName", operationId.getOperationName());
					v.setProperty("typeName", operationId.getTypeName());
					v.setProperty("httpStatus", entryOperationReturnEdge.httpStatus);
					v.setProperty("timestamp", entryOperationReturnEdge.timestamp);
					v.setProperty("elapsedTime", entryOperationReturnEdge.elapsedTime);
					v.setProperty("uuid", entryOperationReturnEdge.uuid);
					v.setProperty("sequence", entryOperationReturnEdge.sequence);
					v.setProperty("depth", entryOperationReturnEdge.depth);
				

				//_watch.lap("EntryOperationReturn event processed");
				linkToTS(g, v, entryOperationReturnEdge.timestamp);
				linkToGUUID(g, v, entryOperationReturnEdge.uuid);
				//linkToTS(oGraph, tempDoc.getIdentity(), "entryReturn", entryOperationReturnEdge.timestamp);
				////linkToTS(oGraph, entryReturn.getIdentity(), "entryReturn", entryOperationReturnEdge.timestamp);
				//_watch.stop("Link event processed");


			} else if(frestoData.dataUnit.isSetOperationCallEdge()) {

				OperationCallEdge operationCallEdge = frestoData.dataUnit.getOperationCallEdge();
				OperationID operationId = operationCallEdge.operationId;

				//StopWatch _watch = new LoggingStopWatch("Writing OperationCall");

				//ODocument operationCall = oGraph.createVertex("OperationCall")
				Vertex v = g.addVertex(null);
					v.setProperty("event", "OperationCall");
					v.setProperty("operationName", operationId.getOperationName());
					v.setProperty("typeName", operationId.getTypeName());
					v.setProperty("timestamp", operationCallEdge.timestamp);
					v.setProperty("uuid", operationCallEdge.uuid);
					v.setProperty("depth", operationCallEdge.depth);
					v.setProperty("sequence", operationCallEdge.sequence);
				

				//_watch.lap("OperationCall event processed");
				linkToTS(g, v, operationCallEdge.timestamp);
				linkToGUUID(g, v, operationCallEdge.uuid);
				//linkToTS(oGraph, tempDoc.getIdentity(), "operationCall", operationCallEdge.timestamp);
				////linkToTS(oGraph, operationCall.getIdentity(), "operationCall", operationCallEdge.timestamp);
				//_watch.stop("Link event processed");

			} else if(frestoData.dataUnit.isSetOperationReturnEdge()) {
				OperationReturnEdge operationReturnEdge = frestoData.dataUnit.getOperationReturnEdge();
				OperationID operationId = operationReturnEdge.operationId;

				//StopWatch _watch = new LoggingStopWatch("Writing OperationReturn");

				//ODocument operationReturn = oGraph.createVertex("OperationReturn")
				Vertex v = g.addVertex(null);
					v.setProperty("event", "OperationReturn");
					v.setProperty("operationName", operationId.getOperationName());
					v.setProperty("operationName", operationId.getOperationName());
					v.setProperty("typeName", operationId.getTypeName());
					v.setProperty("timestamp", operationReturnEdge.timestamp);
					v.setProperty("elapsedTime", operationReturnEdge.elapsedTime);
					v.setProperty("uuid", operationReturnEdge.uuid);
					v.setProperty("sequence", operationReturnEdge.sequence);
					v.setProperty("depth", operationReturnEdge.depth);
				

				//_watch.lap("OperationReturn event processed");
				linkToTS(g, v, operationReturnEdge.timestamp);
				linkToGUUID(g, v, operationReturnEdge.uuid);
				//linkToTS(oGraph, tempDoc.getIdentity(), "operationReturn", operationReturnEdge.timestamp);
				////linkToTS(oGraph, operationReturn.getIdentity(), "operationReturn", operationReturnEdge.timestamp);
				//_watch.stop("Link event processed");
			} else if(frestoData.dataUnit.isSetSqlCallEdge()) {

				SqlCallEdge sqlCallEdge = frestoData.dataUnit.getSqlCallEdge();
				SqlID sqlId = sqlCallEdge.sqlId;

				//StopWatch _watch = new LoggingStopWatch("Writing SqlCall");

				//ODocument sqlCall = oGraph.createVertex("SqlCall")
				Vertex v = g.addVertex(null);
					v.setProperty("event", "SqlCall");
					//v.setProperty("databaseUrl", sqlId.getDatabaseUrl());
					v.setProperty("sql", sqlId.getSql());
					v.setProperty("timestamp", sqlCallEdge.timestamp);
					v.setProperty("uuid", sqlCallEdge.uuid);
					v.setProperty("depth", sqlCallEdge.depth);
					v.setProperty("sequence", sqlCallEdge.sequence);
				

				//_watch.lap("SqlCall event processed");
				linkToTS(g, v, sqlCallEdge.timestamp);
				linkToGUUID(g, v, sqlCallEdge.uuid);
				//linkToTS(oGraph, tempDoc.getIdentity(), "sqlCall", sqlCallEdge.timestamp);
				////linkToTS(oGraph, sqlCall.getIdentity(), "sqlCall", sqlCallEdge.timestamp);
				//_watch.stop("Link event processed");

			} else if(frestoData.dataUnit.isSetSqlReturnEdge()) {
				SqlReturnEdge sqlReturnEdge = frestoData.dataUnit.getSqlReturnEdge();
				SqlID sqlId = sqlReturnEdge.sqlId;

				//StopWatch _watch = new LoggingStopWatch("Writing SqlReturn");

				//ODocument sqlReturn = oGraph.createVertex("SqlReturn")
				Vertex v = g.addVertex(null);
					v.setProperty("event", "SqlReturn");
					//v.setProperty("databaseUrl", sqlId.getDatabaseUrl());
					v.setProperty("sql", sqlId.getSql());
					v.setProperty("timestamp", sqlReturnEdge.timestamp);
					v.setProperty("elapsedTime", sqlReturnEdge.elapsedTime);
					v.setProperty("uuid", sqlReturnEdge.uuid);
					v.setProperty("depth", sqlReturnEdge.depth);
					v.setProperty("sequence", sqlReturnEdge.sequence);
				

				//_watch.lap("SqlReturn event processed");
				linkToTS(g, v, sqlReturnEdge.timestamp);
				linkToGUUID(g, v, sqlReturnEdge.uuid);
				//linkToTS(oGraph, tempDoc.getIdentity(), "sqlReturn", sqlReturnEdge.timestamp);
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
	public static void linkToTS(TitanGraph g, Vertex v, long timestamp) {
		long second = (timestamp/1000) * 1000;
		long minute = (timestamp/60000) * 60000;

		//Index<Long> secondIndex = g.getIndex("second", Vertex.class);
		Iterable<Vertex> vertices = g.getVertices("second", second);
		//Iterable<Vertex> vertices = secondIndex.get("second", second);
		Vertex secondVertex = null;
		if(vertices.iterator().hasNext()) {
			secondVertex = vertices.iterator().next();
		} else {
			secondVertex = g.addVertex(null);
			secondVertex.setProperty("second", second);
			//g.commit();
			//LOGGER.info("Second[" + second + "] created.");
		}
		secondVertex.addEdge("include", v).setProperty("event", v.getProperty("event"));
	}

	public static void linkToGUUID(TitanGraph g, Vertex v, String uuid) {

		Iterable<Vertex> vertices = g.getVertices("guuid", uuid);
		Vertex guuidVertex = null;
		if(vertices.iterator().hasNext()) {
			guuidVertex = vertices.iterator().next();
		} else {
			guuidVertex = g.addVertex(null);
			guuidVertex.setProperty("guuid", uuid);
			//g.commit();
			//LOGGER.info("GUUID[" + uuid + "] created.");
		}
		guuidVertex.addEdge("flow", v).setProperty("event", v.getProperty("event"));
	}
}

