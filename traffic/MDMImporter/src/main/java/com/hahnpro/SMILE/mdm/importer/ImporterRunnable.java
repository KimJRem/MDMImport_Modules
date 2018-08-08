package com.hahnpro.SMILE.mdm.importer;
import org.apache.http.Header;
import org.apache.http.HttpEntity;
import org.apache.http.HttpResponse;
import org.apache.http.StatusLine;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpGet;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.net.ssl.SSLHandshakeException;
import java.io.*;
import java.util.*;
import java.util.stream.Collectors;

class ImporterRunnable implements Runnable {
	public static final int SLEEP_TIME = 10000;
	private static final Logger logger = LoggerFactory.getLogger(ImporterRunnable.class);
	public static final String SUBSCRIPTION_ID = "subscriptionID";
	private final String subscriptionId;
	public HttpClient httpClient;
	private final String uri;


	private LinkedList<ResultHandler> resultHandlers = new LinkedList<>();

	public ImporterRunnable(HttpClient httpClient, String subscriptionId, ResultHandler... resultHandlers) { // , String subscriptionId, LinkedHashMap<String, String> headers,
		this.httpClient = httpClient;
		this.subscriptionId = subscriptionId;
		this.uri = "https://broker.mdm-portal.de/BASt-MDM-Interface/srv/" + subscriptionId + "/clientPullService?subscriptionID=" + subscriptionId;
		for (ResultHandler resultHandler : resultHandlers) {
			this.resultHandlers.add(resultHandler);
		}
	}

	@Override
	public void run() {
		Header lastModified = null;
		while (!Thread.interrupted()) {
			try {
				HttpGet httpUriRequest = new HttpGet(uri);
				if (lastModified != null) {
					httpUriRequest.addHeader("If-Modified-Since", lastModified.getValue());
				}
				HttpResponse response = httpClient.execute(httpUriRequest);
				StatusLine statusLine = response.getStatusLine();
				if (statusLine.getStatusCode() == 304) {
					// not modified
					logger.info("Not Modified");
				} else if (statusLine.getStatusCode() > 400) {
					logger.error("couldn't fetch data ({}, from {})", statusLine.getStatusCode(), uri);
				} else {
					lastModified = handle(response);
				}
				httpUriRequest.releaseConnection();
				Thread.sleep(SLEEP_TIME);
			} catch (SSLHandshakeException e) {
				logger.error("Missing certificate..", e);
				break;
			} catch (IOException e) {
				e.printStackTrace();
				logger.error("Couldn't import..", e);
				break;
			} catch (InterruptedException e) {
				logger.info("ImporterTask {} stopped", Thread.currentThread());
//				e.printStackTrace();
				break;
			}
		}
	}

	private Header handle(HttpResponse response) {
		try {
			BufferedReader reader;
			//Timestamp timestamp = new Timestamp(System.currentTimeMillis());
			HttpEntity entity = response.getEntity();
			Header[] lastModifiedArray = response.getHeaders("Last-Modified");
			Header lastModified = null;
			if (lastModifiedArray.length == 1) {
				lastModified = lastModifiedArray[0];
			}

			InputStream content = entity.getContent();
			reader = new BufferedReader(new InputStreamReader(content));
			//if (reader.ready()) {
			List<String> collect = reader.lines().collect(Collectors.toList());
			for (ResultHandler handler : resultHandlers) {
				handler.handleResult(collect, lastModified.getValue(), subscriptionId);
			}
			logger.info("Handled: " + collect.size());
			//System.out.println(line);
			return lastModified;
			//}
		} catch (Exception e) {
//			e.printStackTrace();
			logger.error("handle Error", e);
		}
		return null;
	}

}
