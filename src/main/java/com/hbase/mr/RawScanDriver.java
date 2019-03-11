package com.hbase.mr;

import org.apache.commons.cli.*;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.mapreduce.TableInputFormat;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;


public class RawScanDriver {

	private static Configuration conf = null;
	private static final String OPTZOOKEEPERSERVER = "zookeeper-server";
	private static final String OPTZOOKEEPERPORT = "zookeeper-port";
	private static final String OPTZOOKEEPERZNODE = "zookeeper-znode";
	private static final String OPTTABLENAME = "table-name";
	private static final String OUTPUTPATH = "output-path";
	private static String TABLENAME = "";
	private static String PATH = "";
	private static Options options;


	public void setConf(CommandLine cmdline) {
		if (cmdline.hasOption("z")) {
			conf.set(HConstants.ZOOKEEPER_QUORUM, cmdline.getOptionValue(OPTZOOKEEPERSERVER));
		}
		if (cmdline.hasOption("p")) {
			conf.set(HConstants.ZOOKEEPER_CLIENT_PORT, cmdline.getOptionValue(OPTZOOKEEPERPORT));
		}
		if (cmdline.hasOption("n")) {
			conf.set(HConstants.ZOOKEEPER_ZNODE_PARENT, cmdline.getOptionValue(OPTZOOKEEPERZNODE));
		}
		if (cmdline.hasOption("t") && cmdline.hasOption("o")) {
			TABLENAME = cmdline.getOptionValue(OPTTABLENAME);
			PATH = cmdline.getOptionValue("o");
			conf.set(TableInputFormat.INPUT_TABLE, cmdline.getOptionValue(OPTTABLENAME));
		}else {
			usage(options);
		}
	}


	public static void usage(Options options) {
		HelpFormatter formatter = new HelpFormatter();
		formatter.printHelp(
				"java -cp `hbase classpath`:hbase-rawscan-mr-1.2-SNAPSHOT.jar com.hbase.util.RawScanDriver [--zookpeer-server <comma seperated list>] [--zookeper-znode <znode>] [--zookeeper-port <zk port>] --table-name <tablename>  --output-path <path>",
				options);
		System.exit(-1);
	}


	public static void main(String[] args) {
		 options = new Options();
		try {
			RawScanDriver status = new RawScanDriver();
			CommandLineParser parser = new GnuParser();
			options.addOption("z", OPTZOOKEEPERSERVER, true,
					"comma seperated zookeeper servers");
			options.addOption("p", OPTZOOKEEPERPORT, true,
					"zookeeper port");
			options.addOption("n", OPTZOOKEEPERZNODE, true,
					"zookeeper znode");
			options.addOption("t", OPTTABLENAME, true,
					"table name");
			options.addOption("o", OUTPUTPATH, true,
					"output path");
			CommandLine line = parser.parse(options, args);
			conf = HBaseConfiguration.create();
			status.setConf(line);
			Scan scan = new Scan();
			scan.setMaxVersions();
			scan.setCaching(2000);
			scan.setRaw(true);
			Job job = new Job(new JobConf(conf));
			job.setJarByClass(RawScanDriver.class);
			job.setNumReduceTasks(0);
			job.setMapOutputKeyClass(Text.class);
			job.setMapOutputKeyClass(Text.class);
			FileOutputFormat.setOutputPath(job, new Path(PATH));
			TableMapReduceUtil.initTableMapperJob(TABLENAME, scan,
					RawScanMapper.class, Text.class, Text.class, job);
			boolean b = job.waitForCompletion(true);
			if (!b) {
				throw new IOException("error with job!");
			}
		}catch (Exception e) {
			e.printStackTrace();
		}
	}
}
