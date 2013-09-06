package com.adintellig.hive.orc.convertor;

import org.apache.hadoop.io.Writable;

import com.adintellig.hive.orc.model.VV;

public class VVConvertor implements Convertor {

	@Override
	public Writable convert(String str) {
		VV row = new VV();

		String[] arr = str.split("\t", -1);
		if (arr.length >= 33) {
			row.stat_date = arr[0];
			row.stat_hour = arr[1];
			row.ip = arr[2];
			row.logdate = arr[3];
			row.method = arr[3];
			row.url = arr[5];
			row.uid = arr[6];
			row.pid = arr[7];
			try {
				row.aid = (arr[8].equals("") || null == arr[8]) ? 0 : Integer
						.parseInt(arr[8]);
				row.wid = (arr[9].equals("") || null == arr[9]) ? 0 : Integer
						.parseInt(arr[9]);
				row.vid = (arr[10].equals("") || null == arr[10]) ? 0 : Integer
						.parseInt(arr[10]);
				row.type = (arr[11].equals("") || null == arr[11]) ? 0
						: Integer.parseInt(arr[11]);
				row.stat = (arr[12].equals("") || null == arr[12]) ? 0
						: Integer.parseInt(arr[12]);
				row.mtime = (arr[13].equals("") || null == arr[13]) ? 0.0f
						: Float.parseFloat(arr[13]);
				row.ptime = (arr[14].equals("") || null == arr[14]) ? 0.0f
						: Float.parseFloat(arr[14]);
			} catch (Exception e) {
				System.out.println(e.getMessage());
			}
			row.channel = arr[15];
			row.boxver = arr[16];
			try {
				row.bftime = (arr[17].equals("") || null == arr[17]) ? 0
						: Integer.parseInt(arr[17]);
			} catch (Exception e) {
				System.out.println(e.getMessage());
			}
			row.country = arr[18];
			row.province = arr[19];
			row.city = arr[20];
			row.isp = arr[21];
			try {
				row.ditchid = (arr[22].equals("") || null == arr[22]) ? 0
						: Integer.parseInt(arr[22]);
				row.drm = (arr[23].equals("") || null == arr[23]) ? 0 : Integer
						.parseInt(arr[23]);
				row.charge = (arr[24].equals("") || null == arr[24]) ? 0
						: Integer.parseInt(arr[24]);
				row.ad = (arr[25].equals("") || null == arr[25]) ? 0 : Integer
						.parseInt(arr[25]);
				row.adclick = (arr[26].equals("") || null == arr[26]) ? 0
						: Integer.parseInt(arr[26]);
				row.groupid = (arr[27].equals("") || null == arr[27]) ? 0
						: Integer.parseInt(arr[27]);
				row.client = (arr[28].equals("") || null == arr[28]) ? 0
						: Integer.parseInt(arr[28]);
				row.usertype = (arr[29].equals("") || null == arr[29]) ? 0
						: Integer.parseInt(arr[29]);
				row.ptolemy = (arr[30].equals("") || null == arr[30]) ? 0
						: Integer.parseInt(arr[30]);
			} catch (Exception e) {
				System.out.println(e.getMessage());
			}
			row.fixedid = arr[31];
			row.userid = arr[32];

		}
		return row;
	}

	@Override
	public int getFieldSize() {
		return VV.class.getFields().length;
	}
}
