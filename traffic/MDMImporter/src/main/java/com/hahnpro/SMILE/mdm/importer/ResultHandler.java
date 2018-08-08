package com.hahnpro.SMILE.mdm.importer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.List;
import java.util.Locale;
import java.util.Map;

public abstract class ResultHandler {

	public ResultHandler() {
	}

	public ResultHandler initialize(ImporterRunnable importer) {
		return this;
	}

	public static ResultHandler getResultHandler(String className) {
		ResultHandler result = null;
		switch (className.toLowerCase()) {
			case "savetofilehandler":
				result = new SaveToFileHandler();
				break;
			case "sendtoconverterhandler":
				result = new SendToConverterHandler();
				break;
			default:
				break;
		}
		System.out.println("Creating a new result handler: " + result);
		return result;
	}

	abstract void configure(Map<String, Object> properties);

	private static final Logger logger = LoggerFactory.getLogger(ResultHandler.class);

	abstract void handleResult(List<String> lines, String lastModified, String subscriptionId);

	static final SimpleDateFormat dateInput = new SimpleDateFormat("EEE, d MMM yyyy HH:mm:ss z", Locale.ENGLISH);
	static final SimpleDateFormat dateOutput = new SimpleDateFormat("yyyy-MM-d_HH-mm-ss_z", Locale.ENGLISH);

	static String lastModifiedToFileName(String lastModified) {
		String date = lastModified;
		try {
			Date parse = dateInput.parse(date);

			return dateOutput.format(parse);
		} catch (ParseException e) {
			logger.error("dateParse Exception", e);
		}
		return "error";
	}
}
