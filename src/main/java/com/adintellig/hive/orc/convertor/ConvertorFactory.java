package com.adintellig.hive.orc.convertor;

import com.adintellig.hive.orc.model.LogKind;

public class ConvertorFactory {

	public static Convertor createConvertor(LogKind kind) {
		switch (kind) {
		case NONE:
			return null;
		case VV:
			return new VVConvertor();
		case LVV:
			return null;
		default:
			throw new IllegalArgumentException("Unknown Log kind: " + kind);
		}
	}
}
