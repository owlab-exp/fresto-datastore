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
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.Iterator;

import java.util.logging.Logger;

import org.zeromq.ZMQ;
import org.zeromq.ZMQ.Socket;
import org.zeromq.ZMQException;

public class FrestoEventQueue extends Thread {
	Logger LOGGER = Logger.getLogger("FrestoEventQueue");

	private ConcurrentLinkedQueue<FrestoEvent> queue = new ConcurrentLinkedQueue<FrestoEvent>();
	private AtomicBoolean work = new AtomicBoolean(true);
	private ZMQ.Socket receiveSocket;

	public FrestoEventQueue() {
	}

	public FrestoEventQueue(ZMQ.Socket receiveSocket) {
		this.receiveSocket = receiveSocket;
	}
	public void run() {
		LOGGER.info("Starting...");

		if(receiveSocket == null) {
			LOGGER.warning("ReceiveSocket not set");
			return;
		}

		while(work.get()) {
			try {
				String topic = new String(receiveSocket.recv(0)); 
				byte[] eventBytes = receiveSocket.recv(0);
				FrestoEvent frestoEvent = new FrestoEvent(topic, eventBytes);
				queue.add(frestoEvent);
			} catch(ZMQException e) {
				if(e.getErrorCode() == ZMQ.Error.ETERM.getCode()){
					LOGGER.info("Breaking...");
					break;
				}
			}
			//LOGGER.fine("EventQueue size = " + queue.size());
		}
		LOGGER.info("Event Queue Shutting down...");
	}

	public void setPullerSocket(ZMQ.Socket receiveSocket) {
		this.receiveSocket = receiveSocket;
	}
	public void stopWork() {
		this.work.set(false);
	}

	public int size() {
		return queue.size();
	}
	
	public FrestoEvent poll() {
		return this.queue.poll();
	}

	public Iterator<FrestoEvent> getIterator() {
		return queue.iterator();
	}
	
	public void remove(FrestoEvent event) {
		this.queue.remove(event);
	}

	public boolean isEmpty() {
		return this.queue.isEmpty();
	}
}


	
