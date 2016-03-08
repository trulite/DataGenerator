package com.dg;

import com.datastax.driver.core.*;
import com.google.common.base.Joiner;
import org.joda.time.DateTime;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;

import java.util.ArrayList;
import java.util.Calendar;
import java.util.List;
import java.util.concurrent.*;

public class DateRangeExecutions extends CassandraConnection {

    private static final int THREADS = 20;
    static int i = 0;


    static Calendar now = Calendar.getInstance();


    public static void main(String args1[]) throws Exception {
        DateRangeExecutions executions = new DateRangeExecutions();
        executions.connect();
        ExecutorService executor = Executors.newFixedThreadPool(THREADS);
        String[] args = args1.length == 0 ? new String[4] : args1;
        String v_StartDate = args[0] == null ? "20150201" : args[0];
        String v_EndDate = args[1] == null ? "20150228" : args[1];
        String v_limit = args[2] == null ? "5000" : args[2];
        String v_client = args[3] == null ? "GIC" : args[3];
        DateTimeFormatter fmt = DateTimeFormat.forPattern("yyyyMMdd");
        DateTime start = fmt.parseDateTime(v_StartDate);
        DateTime end = fmt.parseDateTime(v_EndDate);
        Joiner joiner = Joiner.on(" ");
        List<Object[]> allResults = executions.process(executor, v_limit, v_client, fmt, start, end);
        System.out.println("\nFinished all threads");
        long t2 = System.currentTimeMillis();
        System.out.println("\n Row count : " + allResults.size());
        System.out.println("Time taken to Print Query Result: " + (System.currentTimeMillis() - t2) / 1000.0 + " Sec");
        executor.shutdown();
        try {
            executor.awaitTermination(Long.MAX_VALUE, TimeUnit.NANOSECONDS);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

        return;
    }

    private List<Object[]> process(ExecutorService executor, String v_limit, String v_client, DateTimeFormatter fmt, DateTime start, DateTime end) throws ExecutionException, InterruptedException {
        List<String> datesBetween = new ArrayList<String>();

        while (start.compareTo(end) <= 0) {
            String v_date = fmt.print(start);
            datesBetween.add(v_date);
            DateTime dateBetween = start.plusDays(1);
            start = dateBetween;
        }

        ArrayList<String> statements = new ArrayList<String>();

        int tmp_year = 0;
        int tmp_month = 0;
        int tmp_weekofmonth = 0;

        for (int i = 0; i < datesBetween.size(); i++) {

            String dateSent = datesBetween.get(i);
            String[] v_date = String.valueOf(dateSent).split("");

            now.set(Integer.parseInt(v_date[0] + v_date[1] + v_date[2] + v_date[3]), Integer.parseInt(v_date[4] + v_date[5]), Integer.parseInt(v_date[6] + v_date[7]));
            int v_year = now.get(Calendar.YEAR);
            int v_month = now.get(Calendar.MONTH);
            int v_week_ofmonth = now.get(Calendar.WEEK_OF_MONTH);


            if (v_year == tmp_year && v_month == tmp_month && v_week_ofmonth == tmp_weekofmonth) {

            } else {
                tmp_year = v_year;
                tmp_month = v_month;
                tmp_weekofmonth = v_week_ofmonth;


                String quantileQuery = "SELECT * FROM cassandra_load.messages WHERE"
                        + " stargate ='{"
                        + "   filter : {"
                        + "              type : \"boolean\","
                        + "                  should  : [{type : \"match\", field : \"destination\", value : \"null\"}],"
                        + "						not  : [{type : \"match\", field : \"msg_type\", value : \"null\"}]}}'"
                        + " and tenant_id='" + v_client + "' and year=" + v_year + " and month=" + v_month + " and week_of_month=" + v_week_ofmonth + " limit " + v_limit + "";


                System.out.println(quantileQuery);

                statements.add(quantileQuery);

            }

        }


        long t1 = System.currentTimeMillis();

        List<Future<ResultSet>> list = new ArrayList<Future<ResultSet>>();
        for (int i = 0; i < statements.size(); i++) {
            SinglePartitionQuery query = new SinglePartitionQuery(getSession(), statements.get(i));
            list.add(executor.submit(query));
        }

        List<Object[]> allResults = new ArrayList<Object[]>();
        for (Future<ResultSet> future : list) {
            allResults.addAll(SinglePartitionQuery.getModel(future.get()));
        }

        long t2 = System.currentTimeMillis();
        System.out.println("Time taken to execute Query: " + (t2 - t1) / 1000.0 + " Sec");
        return allResults;
    }


    public static class SinglePartitionQuery implements Callable<ResultSet> {


        private final String statement;
        private final Session session;

        SinglePartitionQuery(Session session, String statement) {
            this.statement = statement;
            this.session = session;

        }

        public ResultSet call() {
            SimpleStatement st = (SimpleStatement) new SimpleStatement(statement).setConsistencyLevel(ConsistencyLevel.TWO);
            return session.execute(st);
        }

        public static List<Object[]> getModel(ResultSet resultSet) throws ExecutionException, InterruptedException {
            ArrayList<Object[]> model = new ArrayList<Object[]>();
            for (Row row : resultSet) {
                //System.out.println(row.getString(0) + "  " + row.getString(1));
                model.add(new Object[]{row.getObject(0), row.getObject(1), row.getObject(2), row.getObject(3), row.getObject(4), row.getObject(5), row.getObject(6)});
            }
            return model;

        }
    }


}
