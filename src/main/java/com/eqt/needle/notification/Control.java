package com.eqt.needle.notification;



public enum Control implements KMesg<Control> {
	START,
	STOP,
	RETRY,
	PAUSE,
	FAIL,
	FINISH;

	@Override
	public String pack() {
		return this.toString();
	}

	@Override
	public Control unpack(String packed) {
		return Control.valueOf(packed);
	}
}