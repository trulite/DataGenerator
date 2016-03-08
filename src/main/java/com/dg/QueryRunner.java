package com.dg;

import com.datastax.driver.core.*;
import com.datastax.driver.core.policies.DefaultRetryPolicy;

class RunnableDemo implements Runnable {
    private String ip;
    private String keySpace;

    private Cluster cluster;
    private Session session;

    private Thread t;
    private int week;

    RunnableDemo(int week) {
        this.week = week;
        System.out.println("Creating thread week-" + week);

        ip = "127.0.0.1";
        keySpace = "cassandra_load";

        cluster = Cluster.builder().addContactPoints(ip).withRetryPolicy(DefaultRetryPolicy.INSTANCE)
                .withPort(9042).withCredentials("cassandra", "cassandra").build();

        session = cluster.connect(keySpace);
        System.out.println("Cassandra connection established.... week-" + week);
    }

    @Override
    public void run() {
        System.out.println("Running week-" + week);
        try {

            StringBuilder sb = new StringBuilder("SELECT * FROM cassandra_load.messages WHERE tenant_id='GIC' and year=2015 and month=2 and week_of_month=");
            sb.append((week % 3) + 1);

            sb.append(" and stargate ='{ filter : {type : \"match\", field : \"destination\", value : \"PAPER\"} }' LIMIT 2000;");

            Statement statement = new SimpleStatement(sb.toString());
            statement.setConsistencyLevel(ConsistencyLevel.TWO);

            session.execute(statement);
        } catch (Exception e) {
            e.printStackTrace();
            System.out.println("Thread week-" + week + " interrupted.");
        }
        System.out.println("Thread week-" + week + " exiting.");
    }

    public void start() throws InterruptedException {
        System.out.println("Starting week-" + week);
        if (t == null) {
            t = new Thread(this, "week-" + week);
            t.start();
            t.join();
        }
    }

}

public class QueryRunner {

    public static void main(String args[]) throws InterruptedException {

        int numThreads = 30;

        RunnableDemo[] threads = new RunnableDemo[numThreads];

        for (int i = 0 ; i < numThreads ; i++) {
            threads[i] = new RunnableDemo(i);
        }
        for (int i = 0 ; i < numThreads ; i++) {
            threads[i].start();
        }
    }
}
