package com.adintellig.hive.orc;

import com.adintellig.hive.orc.model.LogKind;

public class LogKindFactory {

	public static LogKind createLogKind(String logKind) {
		if ("vv".equals(logKind))
			return LogKind.VV;
		else if ("lvv".equals(logKind))
			return LogKind.LVV;
		else
			return LogKind.NONE;
	}
}
