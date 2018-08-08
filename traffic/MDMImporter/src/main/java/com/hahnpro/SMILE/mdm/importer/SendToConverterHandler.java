package com.hahnpro.SMILE.mdm.importer;

import org.apache.http.HttpResponse;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.ByteArrayEntity;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.util.EntityUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

public class SendToConverterHandler extends ResultHandler {

	public static final String CONVERTER_LOCATION = "converterLocation";
	public static final String CONVERTER_OUTPUT = "converterOutput";
	public static final String XML_PATH = "converterXMLPath";
	public static final String XML_PATH_IN_CONVERTER = "xmlPath";
	public static final String SUBSCRIPTION_ID = "subscriptionID";
	public static final String OUTPUT = "output";
	public static final String SAVE_RESULT_TO_FILE = "safeResultToFile";


	public String converterLocation;
	public String xmlPath;
	public String converterOutput;

	private static final Logger logger = LoggerFactory.getLogger(SendToConverterHandler.class);
	private HttpClient httpClient;
	private String safeResultToFile;


	public SendToConverterHandler(HttpClient httpClient) {
		this.httpClient = httpClient;
	}

	public SendToConverterHandler() {
		this.httpClient = HttpClients.createDefault();
	}

	@Override
	void configure(Map<String, Object> properties) {
		this.converterLocation = "" + properties.getOrDefault(CONVERTER_LOCATION, "http://localhost:4568") + "/convert";
		this.converterOutput = ("" + properties.getOrDefault(CONVERTER_OUTPUT, "csv")).toUpperCase();
		this.xmlPath = "" + properties.getOrDefault(XML_PATH, "d2LogicalModel/payloadPublication/parkingAreaStatus");

		safeResultToFile = "" + properties.getOrDefault(SAVE_RESULT_TO_FILE, "./cenvertOut");
	}

	public static SendToConverterHandler create(HttpClient httpClient, LinkedHashMap<String, String> headers) {
		if (httpClient == null) {
			httpClient = HttpClients.createDefault();
		}
		SendToConverterHandler sendToConverterHandler = new SendToConverterHandler(httpClient);

		handleHeaders(sendToConverterHandler, headers);
		return sendToConverterHandler;
	}


	public static SendToConverterHandler handleHeaders(SendToConverterHandler handler, LinkedHashMap<String, String> headers) {
		boolean converterConfigured = false;
		for (String key : headers.keySet()) {
			if (key.startsWith("converter")) {
				converterConfigured = true;
				break;
			}
		}
		//if (converterConfigured) {
			handler.configure(new LinkedHashMap<>(headers));
		//}
		return handler;
	}

	@Override
	public void handleResult(List<String> lines, String lastModified, String subscriptionId) {
		if (converterLocation == null) {
			return;
		}
		try {
			HttpPost httpPost = new HttpPost(converterLocation);
			httpPost.addHeader(SUBSCRIPTION_ID, subscriptionId);
			httpPost.addHeader(OUTPUT, converterOutput);
			httpPost.addHeader(XML_PATH_IN_CONVERTER, xmlPath);

			StringBuilder stringBuilder = new StringBuilder();
			for (String line : lines) {
				stringBuilder.append(line);
				stringBuilder.append('\n');
			}

			httpPost.setEntity(new ByteArrayEntity(stringBuilder.toString().getBytes()));
			HttpResponse execute = httpClient.execute(httpPost);

			logger.info(execute.getStatusLine().toString());
			int statusCode = execute.getStatusLine().getStatusCode();
			String result = "";
			boolean successful = false;
			if (200 >= statusCode && statusCode < 400) {
				// successful
				successful = true;
				result = EntityUtils.toString(execute.getEntity());
			}
			httpPost.reset();

			if (successful) {
				// save XML
				String[] resultLines = result.split(System.lineSeparator());

				if (safeResultToFile != null) {
					try {
						Path path = Paths.get(safeResultToFile.toString() + "/" + subscriptionId + '_' + lastModifiedToFileName(lastModified) + "." + this.converterOutput.toLowerCase());

						// create directory
						Files.createDirectories(path.getParent());
						// create file
						path = Files.createFile(path);

						BufferedWriter writer = new BufferedWriter(new FileWriter(path.toFile()));
						for (String line : resultLines) {
							writer.write(line);
							writer.write("\n");
						}
						writer.flush();
						writer.close();
					} catch (Exception e) {
						logger.error("handleLines Error", e);
					}
				}

			}
		} catch (Exception e) {
			logger.error("handleLines Error", e);
		}
	}
}
