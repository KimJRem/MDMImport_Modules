package com.hahnpro.SMILE.mdm.importer;

import org.apache.http.client.HttpClient;
import org.apache.http.impl.client.HttpClients;
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

public class SaveToFileHandler extends ResultHandler {
	private static final Logger logger = LoggerFactory.getLogger(SaveToFileHandler.class);
	public static final String OUTPUT_PATH = "outputPath";
	private Path outputPath;

	SaveToFileHandler() {
	}

	public static SaveToFileHandler create(HttpClient httpClient, LinkedHashMap<String, String> headers) {
		SaveToFileHandler sendToConverterHandler = new SaveToFileHandler();

		handleHeaders(sendToConverterHandler, headers);
		return sendToConverterHandler;
	}


	public static SaveToFileHandler handleHeaders(SaveToFileHandler handler, LinkedHashMap<String, String> headers) {
		//if (headers.containsKey(OUTPUT_PATH)) {
			handler.configure(new LinkedHashMap<>(headers));
		//}
		return handler;
	}

	@Override
	void configure(Map<String, Object> properties) {
		this.outputPath = Paths.get("" + properties.getOrDefault(OUTPUT_PATH, "./output/"));
	}

	@Override
	void handleResult(List<String> lines, String lastModified, String subscriptionId) {
		logger.info("handleResult SaveToFileHandler " + subscriptionId);
		if (outputPath == null) {
			return;
		}
		try {
			Path path = Paths.get(outputPath.toString() + "/" + subscriptionId + '_' + lastModifiedToFileName(lastModified) + ".xml");

			// create directory
			Files.createDirectories(path.getParent());
			// create file
			path = Files.createFile(path);

			BufferedWriter writer = new BufferedWriter(new FileWriter(path.toFile()));
			for (String line : lines) {
				writer.write(line);
				writer.write("\n");
			}
			writer.flush();
			writer.close();
			logger.info(path.toString());
		} catch (Exception e) {
			logger.error("handleLines Error", e);
		}
	}
}
