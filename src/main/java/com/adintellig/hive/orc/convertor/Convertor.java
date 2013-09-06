package com.adintellig.hive.orc.convertor;

import org.apache.hadoop.io.Writable;

public interface Convertor {
	Writable convert(String str);
	int getFieldSize();
}
