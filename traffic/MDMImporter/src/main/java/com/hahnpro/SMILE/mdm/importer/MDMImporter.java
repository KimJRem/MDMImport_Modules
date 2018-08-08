package com.hahnpro.SMILE.mdm.importer;

import org.apache.http.impl.client.HttpClients;
import org.apache.http.ssl.SSLContextBuilder;
import org.apache.http.ssl.SSLContexts;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.yaml.snakeyaml.Yaml;

import javax.net.ssl.SSLContext;
import java.io.*;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.security.*;
import java.security.cert.CertificateException;
import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;

public class MDMImporter {
	private final Logger logger;

	private ExecutorService executorService;
	private HashMap<String, Future<?>> taskMap = new LinkedHashMap<>();
	private HashMap<Path, String> certificates = new LinkedHashMap<>();
	private SSLContext sslContext;
	private KeyStore keyStore;

	public MDMImporter() throws Exception {
		logger = LoggerFactory.getLogger(MDMImporter.class);
		try {
			keyStore = KeyStore.getInstance("PKCS12");
			sslContext = SSLContexts.createDefault();

			executorService = java.util.concurrent.Executors.newScheduledThreadPool(2);
		} catch (Exception e) {
			e.printStackTrace();
			throw new Exception("Couldn't initiate MDMImporter");
		}
	}

	public void addCertificate(String certificatePath, String keyPassphrase) throws Exception {
		try {
			Path certificate = Paths.get(certificatePath);
			certificates.put(certificate, keyPassphrase);
//			keyStore.load(new FileInputStream(certificatePath), keyPassphrase.toCharArray());
			reloadSSLContext();
		} catch (IOException e) {
			logger.error("certificate not found!", e);
			throw new Exception("Couldn't import certificate");
		} catch (NoSuchAlgorithmException | CertificateException | UnrecoverableKeyException | KeyManagementException | KeyStoreException e) {
			logger.error("Problem with certificate", e);
			throw new Exception("Couldn't import certificate");
		}
	}

	public void reloadSSLContext() throws KeyManagementException, NoSuchAlgorithmException, UnrecoverableKeyException, CertificateException, KeyStoreException, IOException {
		Set<Map.Entry<Path, String>> certificates = this.certificates.entrySet();
		Iterator<Map.Entry<Path, String>> iterator = certificates.iterator();
		SSLContextBuilder sslContextbuilder = SSLContexts.custom();
		while (iterator.hasNext()) {
			Map.Entry<Path, String> next = iterator.next();
			Path certificate = next.getKey();
			String keyPassphrase = next.getValue();
			sslContextbuilder.loadKeyMaterial(certificate.toFile(), keyPassphrase.toCharArray(), keyPassphrase.toCharArray());
		}
		sslContext = sslContextbuilder.build();
	}


	public ImporterRunnable createTask(String config) {
		// parse yaml
		Yaml yaml = new Yaml();
		LinkedHashMap<String, LinkedHashMap<String, Object>> load = yaml.load(config);
		System.out.println(load);
		LinkedHashMap<String, Object> importer = load.get("Importer");
		ImportTaskConfig importTaskConfig = ImportTaskConfig.create(importer);

		ImporterRunnable importerRunnable = ImportTaskConfig.toImporterTask(importTaskConfig);
		importerRunnable.httpClient = HttpClients.custom().setSSLContext(sslContext).build();
		return importerRunnable;
	}

	public String startImport(String yamlFile) {
		logger.debug("Starting import task with config {}", yamlFile);
		ImporterRunnable runnable = createTask(yamlFile);
		String uuid = addMapEntry(executorService.submit(runnable));

		logger.info("Started import task {}, from {}", uuid);
		return uuid;
	}

	public String startImport(String subscriptionId, ResultHandler... resultHandlers) {
		logger.debug("Starting import task from {}", subscriptionId);
		Runnable runnable = new ImporterRunnable(HttpClients.custom().setSSLContext(sslContext).build(), subscriptionId, resultHandlers);

		String uuid = addMapEntry(executorService.submit(runnable));

		logger.info("Started import task {}, from {}", uuid, subscriptionId);
		return uuid;
	}

	private String addMapEntry(Future<?> future) {
		String uuid = UUID.randomUUID().toString();
		this.taskMap.put(uuid, future);
		return uuid;
	}


	private boolean cancel(Future<?> submit) {
		if (submit != null) {
			return submit.cancel(true);
		}
		return false;
	}

	public boolean stopImport(String uuid) {
		Future<?> future = this.taskMap.get(uuid);
		if (future != null) {
			logger.debug("Stopped importTask {}", uuid);
			return cancel(future);
		}
		logger.warn("Couldn't stop importTask{}", uuid);
		return false;
	}

	public String[] stopAllImports() {
		logger.info("stopping all import tasks");
		Set<String> keys = this.taskMap.keySet();
		String[] strings = keys.toArray(new String[keys.size()]);
		for (String key : keys) {
			if (stopImport(key)) {
				this.taskMap.remove(key);
			}
		}
		return strings;
	}

	public void stop() {
		stopAllImports();
		this.executorService.shutdownNow();
	}

}
