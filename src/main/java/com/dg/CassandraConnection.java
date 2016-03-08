package com.dg;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Host;
import com.datastax.driver.core.Metadata;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.SocketOptions;
import com.datastax.driver.core.policies.ConstantReconnectionPolicy;
import com.datastax.driver.core.policies.DCAwareRoundRobinPolicy;
import com.datastax.driver.core.policies.DefaultRetryPolicy;
import com.datastax.driver.core.policies.DowngradingConsistencyRetryPolicy;
import com.datastax.driver.core.policies.RoundRobinPolicy;
import com.datastax.driver.core.policies.TokenAwarePolicy;

public class CassandraConnection{

	public Session session;
	public Cluster cluster;

	static long t1;

	public Session getSession() {
		return this.session;
	}

	public void connect() {

		cluster = Cluster.builder().addContactPoints("127.0.0.1").withRetryPolicy(DefaultRetryPolicy.INSTANCE)
				.withPort(9042).withCredentials("cassandra", "cassandra").build();
		Metadata metadata = cluster.getMetadata();
		System.out.printf("Connected to cluster: %s\n",
				metadata.getClusterName());


		session = cluster.connect("cassandra_load");
	}

	public void close() {
		//System.out.println("Time taken  " + (System.currentTimeMillis() - t1)/ 1000.0 + " seconds ");
		session.close();
		cluster.close();
	}

}
