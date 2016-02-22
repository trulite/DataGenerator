package com.dg;

import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.Random;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.policies.DefaultRetryPolicy;

public class DataGenerator {

    private String ip;
    private String keySpace;

    public static final String DATE1 = "1-MAR-2015 00:00:00:000";
    public static final String DATE2 = "20-MAR-2016 00:00:00:000";
    public static final String FORMAT = "dd-MMM-yyyy HH:mm:ss:SSS";
    public static final SimpleDateFormat sdf = new SimpleDateFormat(FORMAT);
    public static String[] SYMBOLS = { "MSFT", "IBM", "AAPL", "INTC", "GOOG", "C", "JPM", "GS", "CSCO", "BAML", "AMG",
            "AFL" };

    public static String[] DESTINATIONS = { "BAML_ALGO_US", "BAML_CASH", "BAML_PROGRAM", "BAML_DMA", "CITI_ALGO_US",
            "CITI_CASH", "CITI_PROGRAM", "CITI_DMA", "CS_ALGO_US", "CS_CASH", "CS_PROGRAM", "CS_DMA", "JPM_ALGO_US",
            "JPM_CASH", "JPM_PROGRAM", "JPM_DMA", "PAPER" };

    private static Random random = new Random(System.currentTimeMillis());

    private Cluster cluster;
    private Session session;

    public DataGenerator() {

        ip = "127.0.0.1";
        keySpace = "cassandra_load";

        cluster = Cluster.builder().addContactPoints(ip).withRetryPolicy(DefaultRetryPolicy.INSTANCE)
                .withPort(9042).withCredentials("cassandra", "cassandra").build();

        session = cluster.connect(keySpace);
        System.out.println("Cassandra connection established....");
    }

    private static int M = 1000000;

    static int messageCount;

    public static void main(String args[]) {
        DataGenerator dg = new DataGenerator();
        Utils.SimpleTimer timer = new Utils.SimpleTimer();
        timer.start();
        try {
            while (true) {
                int multiplier = random.nextInt(7) + 1;
                int symbolIndex = random.nextInt(SYMBOLS.length - 1);
                // int destIndex = random.nextInt(DESTINATIONS.length-1);

                dg.generateData(SYMBOLS[symbolIndex], multiplier * M * 10);
                System.out.println(messageCount);

                if (messageCount > 6500000) {
                    break;
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            timer.endLogTime("");
            dg.closeCassandraConnections();
        }
    }

    private void closeCassandraConnections() {
        session.close();
        cluster.close();

    }

    public void generateData(String symbol, int qty) {

        String inbound_tkt_id = generateInboundOrder(symbol, qty);
        generateOutBoundOrder(inbound_tkt_id, symbol, qty);

    }

    private String generateInboundOrder(String symbol, int qty) {
        String ticket_id = insertInboundSummary(symbol, qty);
        insertInboundOrder(ticket_id, symbol, qty);

        int executedQty = 0;

        while (executedQty < qty) {
            int exQty = M * (random.nextInt(5) + 1);
            if (exQty > qty - executedQty) {
                exQty = qty - executedQty;
            }
            insertInboundExecution(ticket_id, symbol, exQty);
            executedQty += exQty;

        }
        return ticket_id;
    }

    int k = 10000;

    private void generateOutBoundOrder(String inbound_tkt_id, String symbol, int qty) {
        int outOrderedQty = 0;
        while (outOrderedQty < qty) {
            String destination = DESTINATIONS[random.nextInt(DESTINATIONS.length - 1)];
            int orderQty = M * (random.nextInt(10) + 1);
            if (orderQty > qty - outOrderedQty) {
                orderQty = qty - outOrderedQty;
            }
            String ticket_id = insertOutboundSummary(inbound_tkt_id, symbol, destination, orderQty);

            insertOutboundOrder(ticket_id, symbol, destination, outOrderedQty);
            int executedQty = 0;
            while (executedQty < orderQty) {
                int exQty = k * (random.nextInt(20) + 1);
                if (exQty > orderQty - executedQty) {
                    exQty = orderQty - executedQty;
                }
                insertOutboundExecution(ticket_id, symbol, exQty);
                executedQty += exQty;

            }
            outOrderedQty += orderQty;

        }
    }

    private String insertInboundSummary(String symbol, int qty) {
        StringBuilder sb = new StringBuilder("INSERT INTO ");
        String ticket_id = getTicketId();
        {
            sb.append("summary ");
            sb.append(" ( ");

            sb.append("ticket_id").append(",");
            sb.append("basket_id").append(",");
            sb.append("wave_id").append(",");

            sb.append("symbol").append(",");
            sb.append("side").append(",");

            sb.append("order_qty").append(",");
            sb.append("order_price").append(",");
            sb.append("type").append(",");

            sb.append("tif").append(",");
            sb.append("destination").append(",");
            sb.append("status").append(",");

            sb.append("exec_qty").append(",");
            sb.append("exec_price").append(",");

            sb.append("sent_date").append(",");
            sb.append("sent_time").append(",");

            sb.append("year").append(",");
            sb.append("month").append(",");
            sb.append("week_of_month").append(",");
            sb.append("tenant_id").append(",");

            sb.append("customer").append(",");
            sb.append("account").append(",");
            sb.append("strategy").append(",");

            sb.append("parent_tickets").append(",");
            sb.append("sender_comp_id").append(",");
            sb.append("sender_sub_id");

            sb.append(" ) ");

            // values part
            sb.append(" VALUES ( ");

            sb.append("'").append(ticket_id).append("'").append(",");
            sb.append("'").append(getBasketId()).append("'").append(",");
            sb.append("'").append(getWaveId()).append("'").append(",");

            sb.append("'").append(symbol).append("'").append(",");
            sb.append("'").append("BUY").append("'").append(",");
            sb.append(" ").append(qty).append(" ").append(",");
            sb.append(" ").append("0").append(" ").append(",");
            sb.append("'").append("Market").append("'").append(","); // type

            sb.append("'").append("Good Till Cancel").append("'").append(",");
            sb.append("'").append("PAPER").append("'").append(",");
            sb.append("'").append("Filled").append("'").append(",");

            sb.append(" ").append(qty).append(" ").append(",");
            sb.append(" ").append(getPrx()).append(" ").append(",");

            // String dateTime = ticket.getSendingTime(); //in the FIX time
            // format "yyyyMMdd-HH:mm:ss"
            // sb.append("'").append(dateTime.substring(0,
            // 8)).append("'").append(",");
            // sb.append("'").append(dateTime.substring(
            // 9)).append("'").append(",");
            String dateTime = getRandomDateTime(DATE1, DATE2); // FORMAT
            // =
            // "dd-MMM-yyyy
            // HH:mm:ss:SSS"
            sb.append("'").append(dateTime.substring(0, 11)).append("'").append(",");
            sb.append("'").append(dateTime.substring(12)).append("'").append(",");

            // year int,
            int year = getDateField(dateTime, Calendar.YEAR);
            sb.append("").append(year).append("").append(",");

            // month int,
            int month = getDateField(dateTime, Calendar.MONTH);
            sb.append("").append(month).append("").append(",");
            // week_of_month int,
            int week_of_month = getDateField(dateTime, Calendar.WEEK_OF_MONTH);
            sb.append("").append(week_of_month).append("").append(",");

            sb.append("'").append("GIC").append("'").append(",");

            sb.append("'").append("C1").append("'").append(",");
            sb.append("'").append("A1").append("'").append(",");
            sb.append("'").append("S1").append("'").append(",");

            sb.append("'").append("").append("'").append(",");
            sb.append("'").append("INDIGOT").append("'").append(",");
            sb.append("'").append("vk").append("'");

            // tenant_id text,
            // year int,
            // month int,
            // week_of_month int,
            // stargate text,

            sb.append(" ) ");

        }

        session.execute(sb.toString());

        return ticket_id;
    }

    // inbound_tkt_id, symbol, destination, orderQty
    private String insertOutboundSummary(String inbound_tkt_id, String symbol, String destination, int qty) {
        StringBuilder sb = new StringBuilder("INSERT INTO ");
        String ticket_id = getTicketId();
        {
            sb.append("summary ");
            sb.append(" ( ");

            sb.append("ticket_id").append(",");
            sb.append("basket_id").append(",");
            sb.append("wave_id").append(",");

            sb.append("symbol").append(",");
            sb.append("side").append(",");

            sb.append("order_qty").append(",");
            sb.append("order_price").append(",");
            sb.append("type").append(",");

            sb.append("tif").append(",");
            sb.append("destination").append(",");
            sb.append("status").append(",");

            sb.append("exec_qty").append(",");
            sb.append("exec_price").append(",");

            sb.append("sent_date").append(",");
            sb.append("sent_time").append(",");

            sb.append("year").append(",");
            sb.append("month").append(",");
            sb.append("week_of_month").append(",");
            sb.append("tenant_id").append(",");

            sb.append("customer").append(",");
            sb.append("account").append(",");
            sb.append("strategy").append(",");

            sb.append("parent_tickets").append(",");
            sb.append("sender_comp_id").append(",");
            sb.append("sender_sub_id");

            sb.append(" ) ");

            // values part
            sb.append(" VALUES ( ");

            sb.append("'").append(ticket_id).append("'").append(",");
            sb.append("'").append(getBasketId()).append("'").append(",");
            sb.append("'").append(getWaveId()).append("'").append(",");

            sb.append("'").append(symbol).append("'").append(",");
            sb.append("'").append("BUY").append("'").append(",");
            sb.append(" ").append(qty).append(" ").append(",");
            sb.append(" ").append("0").append(" ").append(",");
            sb.append("'").append("Market").append("'").append(","); // type

            sb.append("'").append("Day").append("'").append(",");
            sb.append("'").append(destination).append("'").append(",");
            sb.append("'").append("Filled").append("'").append(",");

            sb.append(" ").append(qty).append(" ").append(",");
            sb.append(" ").append(getPrx()).append(" ").append(",");

            // String dateTime = ticket.getSendingTime(); //in the FIX time
            // format "yyyyMMdd-HH:mm:ss"
            // sb.append("'").append(dateTime.substring(0,
            // 8)).append("'").append(",");
            // sb.append("'").append(dateTime.substring(
            // 9)).append("'").append(",");
            String dateTime = getRandomDateTime(DATE1, DATE2); // FORMAT
            // =
            // "dd-MMM-yyyy
            // HH:mm:ss:SSS"
            sb.append("'").append(dateTime.substring(0, 11)).append("'").append(",");
            sb.append("'").append(dateTime.substring(12)).append("'").append(",");

            // year int,
            int year = getDateField(dateTime, Calendar.YEAR);
            sb.append("").append(year).append("").append(",");

            // month int,
            int month = getDateField(dateTime, Calendar.MONTH);
            sb.append("").append(month).append("").append(",");
            // week_of_month int,
            int week_of_month = getDateField(dateTime, Calendar.WEEK_OF_MONTH);
            sb.append("").append(week_of_month).append("").append(",");

            sb.append("'").append("GIC").append("'").append(",");

            sb.append("'").append("C1").append("'").append(",");
            sb.append("'").append("A1").append("'").append(",");
            sb.append("'").append("S1").append("'").append(",");

            sb.append("'").append(inbound_tkt_id).append("'").append(",");
            sb.append("'").append("INDIGOT").append("'").append(",");
            sb.append("'").append("vk").append("'");

            // tenant_id text,
            // year int,
            // month int,
            // week_of_month int,
            // stargate text,

            sb.append(" ) ");

        }

        session.execute(sb.toString());

        return ticket_id;

    }

    public void insertOutboundOrder(String ticket_id, String symbol, String destination, int qty) {

        messageCount++;
        StringBuilder sb = new StringBuilder("INSERT INTO ");
        sb.append(" messages ");
        sb.append(" ( ");

        sb.append("ticket_id").append(",");
        sb.append("id").append(",");
        sb.append("basket_id").append(",");
        sb.append("wave_id").append(",");

        sb.append("symbol").append(",");
        sb.append("side").append(",");

        sb.append("order_qty").append(",");
        sb.append("order_price").append(",");
        sb.append("order_type").append(",");

        sb.append("tif").append(",");
        sb.append("destination").append(",");

        sb.append("sent_date").append(",");
        sb.append("sent_time").append(",");

        sb.append("year").append(",");
        sb.append("month").append(",");
        sb.append("week_of_month").append(",");
        sb.append("tenant_id").append(",");

        sb.append("msg_type").append(",");
        sb.append("row_type").append(",");

        sb.append("sender_comp_id").append(",");
        sb.append("sender_sub_id");

        sb.append(" ) ");

        // values part
        // ZOrder order = ticket.getOrder();
        sb.append(" VALUES ( ");
        sb.append("'").append(ticket_id).append("'").append(",");
        sb.append("'").append(getOrderId()).append("'").append(",");
        sb.append("'").append(getBasketId()).append("'").append(",");
        sb.append("'").append(getWaveId()).append("'").append(",");

        sb.append("'").append(symbol).append("'").append(",");
        sb.append("'").append("BUY").append("'").append(",");
        sb.append(" ").append(qty).append(" ").append(",");
        sb.append(" ").append("0").append(" ").append(",");
        sb.append("'").append("Market").append("'").append(",");

        sb.append("'").append("DAY").append("'").append(",");
        sb.append("'").append(destination).append("'").append(",");
        // sb.append("'").append(ticket.getStatus()).append("'").append(",");

        String dateTime = getRandomDateTime(DATE1, DATE2); // in the FIX time
        // format
        // "yyyyMMdd-HH:mm:ss"
        sb.append("'").append(dateTime.substring(0, 11)).append("'").append(",");
        sb.append("'").append(dateTime.substring(12)).append("'").append(",");

        int year = getDateField(dateTime, Calendar.YEAR);
        sb.append("").append(year).append("").append(",");

        int month = getDateField(dateTime, Calendar.MONTH);
        sb.append("").append(month).append("").append(",");

        int week_of_month = getDateField(dateTime, Calendar.WEEK_OF_MONTH);
        sb.append("").append(week_of_month).append("").append(",");

        sb.append("'").append("GIC").append("'").append(",");

        sb.append("'").append("ORDER").append("'").append(",");
        sb.append("'").append("OUTBOUND").append("'").append(",");

        sb.append("'").append("INDIGOT").append("'").append(",");
        sb.append("'").append("vk").append("'");
        sb.append(" ) ");

        // System.out.println(sb.toString());
        // System.out.println(".");
        session.execute(sb.toString());

    }

    public void insertInboundOrder(String ticket_id, String symbol, int qty) {
        messageCount++;
        StringBuilder sb = new StringBuilder("INSERT INTO ");
        sb.append(" messages ");
        sb.append(" ( ");

        sb.append("ticket_id").append(",");
        sb.append("id").append(",");
        sb.append("basket_id").append(",");
        sb.append("wave_id").append(",");

        sb.append("symbol").append(",");
        sb.append("side").append(",");

        sb.append("order_qty").append(",");
        sb.append("order_price").append(",");
        sb.append("order_type").append(",");

        sb.append("tif").append(",");
        sb.append("destination").append(",");

        sb.append("sent_date").append(",");
        sb.append("sent_time").append(",");

        sb.append("year").append(",");
        sb.append("month").append(",");
        sb.append("week_of_month").append(",");
        sb.append("tenant_id").append(",");

        sb.append("msg_type").append(",");
        sb.append("row_type").append(",");

        sb.append("sender_comp_id").append(",");
        sb.append("sender_sub_id");

        sb.append(" ) ");

        // values part
        // ZOrder order = ticket.getOrder();
        sb.append(" VALUES ( ");
        sb.append("'").append(ticket_id).append("'").append(",");
        sb.append("'").append(getOrderId()).append("'").append(",");
        sb.append("'").append(getBasketId()).append("'").append(",");
        sb.append("'").append(getWaveId()).append("'").append(",");

        sb.append("'").append(symbol).append("'").append(",");
        sb.append("'").append("BUY").append("'").append(",");
        sb.append(" ").append(qty).append(" ").append(",");
        sb.append(" ").append("0").append(" ").append(",");
        sb.append("'").append("MARKET").append("'").append(",");

        sb.append("'").append("DAY").append("'").append(",");
        sb.append("'").append("PAPER").append("'").append(",");
        // sb.append("'").append(ticket.getStatus()).append("'").append(",");

        String dateTime = getRandomDateTime(DATE1, DATE2); // in the FIX time
        // format
        // "yyyyMMdd-HH:mm:ss"
        sb.append("'").append(dateTime.substring(0, 11)).append("'").append(",");
        sb.append("'").append(dateTime.substring(12)).append("'").append(",");

        int year = getDateField(dateTime, Calendar.YEAR);
        sb.append("").append(year).append("").append(",");

        int month = getDateField(dateTime, Calendar.MONTH);
        sb.append("").append(month).append("").append(",");

        int week_of_month = getDateField(dateTime, Calendar.WEEK_OF_MONTH);
        sb.append("").append(week_of_month).append("").append(",");

        sb.append("'").append("GIC").append("'").append(",");

        sb.append("'").append("ORDER").append("'").append(",");
        sb.append("'").append("INBOUND").append("'").append(",");

        sb.append("'").append("INDIGOT").append("'").append(",");
        sb.append("'").append("vk").append("'");
        sb.append(" ) ");

        // System.out.println(sb.toString());
        // System.out.println(".");
        session.execute(sb.toString());

    }

    public void insertOutboundExecution(String ticket_id, String symbol, int qty) {
        messageCount++;
        StringBuilder sb = new StringBuilder("INSERT INTO ");
        sb.append(" messages ");
        sb.append(" ( ");

        sb.append("ticket_id").append(",");
        sb.append("id").append(",");
        sb.append("basket_id").append(",");
        sb.append("wave_id").append(",");

        sb.append("symbol").append(",");
        sb.append("side").append(",");

        sb.append("order_qty").append(",");
        sb.append("order_price").append(",");
        sb.append("order_type").append(",");

        sb.append("tif").append(",");
        sb.append("destination").append(",");

        sb.append("sent_date").append(",");
        sb.append("sent_time").append(",");

        sb.append("year").append(",");
        sb.append("month").append(",");
        sb.append("week_of_month").append(",");
        sb.append("tenant_id").append(",");

        sb.append("msg_type").append(",");
        sb.append("row_type").append(",");

        sb.append("sender_comp_id").append(",");
        sb.append("sender_sub_id");

        sb.append(" ) ");

        // values part
        // ZOrder order = ticket.getOrder();
        sb.append(" VALUES ( ");
        sb.append("'").append(ticket_id).append("'").append(",");
        sb.append("'").append(getOrderId()).append("'").append(",");
        sb.append("'").append(getBasketId()).append("'").append(",");
        sb.append("'").append(getWaveId()).append("'").append(",");

        sb.append("'").append(symbol).append("'").append(",");
        sb.append("'").append("BUY").append("'").append(",");
        sb.append(" ").append(qty).append(" ").append(",");
        sb.append(" ").append("0").append(" ").append(",");
        sb.append("'").append("PARTIALLY FILLED").append("'").append(",");

        sb.append("'").append("DAY").append("'").append(",");
        sb.append("'").append("null").append("'").append(",");
        // sb.append("'").append(ticket.getStatus()).append("'").append(",");

        String dateTime = getRandomDateTime(DATE1, DATE2); // in the FIX time
        // format
        // "yyyyMMdd-HH:mm:ss"
        sb.append("'").append(dateTime.substring(0, 11)).append("'").append(",");
        sb.append("'").append(dateTime.substring(12)).append("'").append(",");

        int year = getDateField(dateTime, Calendar.YEAR);
        sb.append("").append(year).append("").append(",");

        int month = getDateField(dateTime, Calendar.MONTH);
        sb.append("").append(month).append("").append(",");

        int week_of_month = getDateField(dateTime, Calendar.WEEK_OF_MONTH);
        sb.append("").append(week_of_month).append("").append(",");

        sb.append("'").append("GIC").append("'").append(",");

        sb.append("'").append("Execution").append("'").append(",");
        sb.append("'").append("OUTBOUND").append("'").append(",");

        sb.append("'").append("INDIGOT").append("'").append(",");
        sb.append("'").append("vk").append("'");
        sb.append(" ) ");

        // System.out.println(sb.toString());
        // System.out.println(".");

        session.execute(sb.toString());

    }

    public void insertInboundExecution(String ticket_id, String symbol, int qty) {
        messageCount++;
        StringBuilder sb = new StringBuilder("INSERT INTO ");
        sb.append(" messages ");
        sb.append(" ( ");

        sb.append("ticket_id").append(",");
        sb.append("id").append(",");
        sb.append("basket_id").append(",");
        sb.append("wave_id").append(",");

        sb.append("symbol").append(",");
        sb.append("side").append(",");

        sb.append("order_qty").append(",");
        sb.append("order_price").append(",");
        sb.append("order_type").append(",");

        sb.append("tif").append(",");
        sb.append("destination").append(",");

        sb.append("sent_date").append(",");
        sb.append("sent_time").append(",");

        sb.append("year").append(",");
        sb.append("month").append(",");
        sb.append("week_of_month").append(",");
        sb.append("tenant_id").append(",");

        sb.append("msg_type").append(",");
        sb.append("row_type").append(",");

        sb.append("sender_comp_id").append(",");
        sb.append("sender_sub_id");

        sb.append(" ) ");

        // values part
        // ZOrder order = ticket.getOrder();
        sb.append(" VALUES ( ");
        sb.append("'").append(ticket_id).append("'").append(",");
        sb.append("'").append(getOrderId()).append("'").append(",");
        sb.append("'").append(getBasketId()).append("'").append(",");
        sb.append("'").append(getWaveId()).append("'").append(",");

        sb.append("'").append(symbol).append("'").append(",");
        sb.append("'").append("BUY").append("'").append(",");
        sb.append(" ").append(qty).append(" ").append(",");
        sb.append(" ").append("0").append(" ").append(",");
        sb.append("'").append("MARKET").append("'").append(",");

        sb.append("'").append("DAY").append("'").append(",");
        sb.append("'").append("null").append("'").append(",");
        // sb.append("'").append(ticket.getStatus()).append("'").append(",");

        String dateTime = getRandomDateTime(DATE1, DATE2); // in the FIX time
        // format
        // "yyyyMMdd-HH:mm:ss"
        sb.append("'").append(dateTime.substring(0, 11)).append("'").append(",");
        sb.append("'").append(dateTime.substring(12)).append("'").append(",");

        int year = getDateField(dateTime, Calendar.YEAR);
        sb.append("").append(year).append("").append(",");

        int month = getDateField(dateTime, Calendar.MONTH);
        sb.append("").append(month).append("").append(",");

        int week_of_month = getDateField(dateTime, Calendar.WEEK_OF_MONTH);
        sb.append("").append(week_of_month).append("").append(",");

        sb.append("'").append("GIC").append("'").append(",");

        sb.append("'").append("Execution").append("'").append(",");
        sb.append("'").append("INBOUND").append("'").append(",");

        sb.append("'").append("INDIGOT").append("'").append(",");
        sb.append("'").append("vk").append("'");
        sb.append(" ) ");

        // System.out.println(sb.toString());
        // System.out.println(".");
        session.execute(sb.toString());

    }

    private int ticketIdCount;

    private String getTicketId() {
        return "INDIGOT." + String.format("%09d", ticketIdCount++);
    }

    private int orderIdCount;

    private String getOrderId() {
        return "Order_id_" + orderIdCount++;
    }

    private int basketIdCount;

    private String getBasketId() {
        return "Basket_id_" + basketIdCount++;
    }

    private int waveIdCount;

    private String getWaveId() {
        return "wave_id_" + waveIdCount++;
    }

    private double getPrx() {
        return 30 + random.nextInt(400) + random.nextDouble();
    }

    private String getRandomDateTime(String ds1, String ds2) {
        try {
            Date d1 = sdf.parse(ds1);
            Date d2 = sdf.parse(ds2);

            if (d2.before(d1)) {
                throw new IllegalArgumentException(ds1 + " is after d2 " + ds2);
            }

            Calendar cal = Calendar.getInstance();
            cal.setTime(d1);
            int day1 = cal.get(Calendar.DAY_OF_YEAR);

            cal.setTime(d2);
            int day2 = cal.get(Calendar.DAY_OF_YEAR);

            int diff = 0;

            if (day2 > day1) {
                diff = day2 - day1;
            } else if (day1 > day2) { // must be different years
                diff = day2 - day1 + 365;
            }

            int offset = random.nextInt(diff + 1);

            cal.setTime(d1);
            cal.add(Calendar.DATE, offset);

            return sdf.format(cal.getTime());
        } catch (Exception ex) {
            ex.printStackTrace();
        }

        return sdf.format(new Date());
    }

    private int getDateField(String dateTime, int field) {
        try {
            Date date = sdf.parse(dateTime);
            Calendar cal = Calendar.getInstance();
            cal.setTime(date);
            return cal.get(field);
        } catch (Exception ex) {
            ex.printStackTrace();
        }
        return -1;

    }

}
