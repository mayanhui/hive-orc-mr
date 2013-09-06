package com.adintellig.hive.orc.model;

public class ModelClassFactory {

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
}
