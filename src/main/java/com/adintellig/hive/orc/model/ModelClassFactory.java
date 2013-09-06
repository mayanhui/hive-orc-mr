package com.adintellig.hive.orc.model;

import com.adintellig.hive.orc.LogKindFactory;

public class ModelClassFactory extends LogKindFactory {

	public static Class<?> createClass(LogKind kind) {
		switch (kind) {
		case NONE:
			return null;
		case VV:
			return VV.class;
		case LVV:
			return null;
		default:
			throw new IllegalArgumentException("Unknown Log kind: " + kind);
		}
	}

	public static Class<?> createClass(String kind) {
		return createClass(createLogKind(kind));
	}
}
