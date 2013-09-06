package com.adintellig.hive.orc.model;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;

public class VV implements Writable {
	public String stat_date;
	public String stat_hour;
	public String ip;
	public String logdate;
	public String method;
	public String url;
	public String uid;
	public String pid;
	public int aid;
	public int wid;
	public int vid;
	public int type;
	public int stat;
	public float mtime;
	public float ptime;
	public String channel;
	public String boxver;
	public int bftime;
	public String country;
	public String province;
	public String city;
	public String isp;
	public int ditchid;
	public int drm;
	public int charge;
	public int ad;
	public int adclick;
	public int groupid;
	public int client;
	public int usertype;
	public int ptolemy;
	public String fixedid;
	public String userid;

	@Override
	public void write(DataOutput out) throws IOException {
		throw new UnsupportedOperationException("no write");
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		throw new UnsupportedOperationException("no read");
	}
}
