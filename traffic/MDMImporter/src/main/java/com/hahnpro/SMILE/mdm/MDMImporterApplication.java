package com.hahnpro.SMILE.mdm;

import com.hahnpro.SMILE.mdm.importer.MDMImporter;
import com.hahnpro.SMILE.mdm.importer.SaveToFileHandler;
import com.hahnpro.SMILE.mdm.importer.SendToConverterHandler;
import com.mashape.unirest.http.HttpResponse;
import com.mashape.unirest.http.JsonNode;
import com.mashape.unirest.http.Unirest;
import com.mashape.unirest.http.exceptions.UnirestException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import spark.Spark;

import java.io.*;
import java.nio.file.*;
import java.security.KeyStore;
import java.security.KeyStoreException;
import java.security.NoSuchAlgorithmException;
import java.security.cert.CertificateException;
import java.util.LinkedHashMap;
import java.util.Set;
import java.util.Timer;
import java.util.stream.Collectors;

import static spark.Spark.*;

public class MDMImporterApplication {
	public static final String CERTIFICATE_PATH = "./certificates/";
	private MDMImporter mdmImporter;
	private Path certificatePath;

	private static final Logger logger = LoggerFactory.getLogger(MDMImporterApplication.class);

	public MDMImporterApplication() {
		//String keyPassphrase = System.getenv("keyPassphrase");
		//String certificate = "./local_resources/smilemdm-alex.hahnpro.com.p12";
		try {
			mdmImporter = new MDMImporter();
			// clear old certificates
			Path certificatePath = Paths.get(CERTIFICATE_PATH);
			Files.walkFileTree(certificatePath, new ClearDirectoryVisitor());

			//mdmImporter.addCertificate(certificate, keyPassphrase);
		} catch (Exception e) {
			e.printStackTrace();
			return;
		}

		Logger logger = LoggerFactory.getLogger(MDMImporterApplication.class);
		logger.info("Stating Application");
		get("/start", (req, res) -> {
			Set<String> headers = req.headers();
			LinkedHashMap<String, String> headerMap = new LinkedHashMap<>();
			// convert headers to HashMap
			for (String header : headers) {
				headerMap.put(header, req.headers(header));
			}
			String id = req.queryParams("subscriptionID");
			if (id == null) {
				id = "2683003";
			}
			if (id != null) {
				// todo: check, what result handlers we want to use..
				//headerMap
				String uuid = mdmImporter.startImport(id, SaveToFileHandler.create(null, headerMap), SendToConverterHandler.create(null, headerMap));
				if (uuid != null) {
					return "started import " + uuid;
				}
			} else {
				// TODO: Error Handling
			}
			return "couldn't start specified import";
		});
		post("/start", (req, res) -> {
			// todo: check, what result handlers we want to use..
			String uuid = mdmImporter.startImport(req.body());
			if (uuid != null) {
				return "started import " + uuid;
			}
			return "couldn't start specified import";
		});
		get("/stop", (req, res) -> {
			Object id = req.queryParams("id");
			if (id != null) {
				boolean stopped = mdmImporter.stopImport((String) id);
				if (stopped) {
					return "stopped import";
				}
			} else {
				Spark.halt(404);
			}
			return "couldn't stop specified import";
		});
		get("/stopAll", (req, res) -> {
			String[] keys = mdmImporter.stopAllImports();
			return "stopped imports (" + keys.length + ")";
		});
		this.certificatePath = Paths.get(CERTIFICATE_PATH);
		try {
			Files.createDirectories(this.certificatePath);
			post("addCertificate", (req, res) -> {
				String keyPass = req.queryParams("keyPassphrase");
				if (keyPass != null) {
					byte[] cert = req.bodyAsBytes();
					if (cert != null) {
						// save to certificates
						Path newCert = certificatePath.resolve("./" + System.currentTimeMillis() + ".p12");
						if (checkCertificate(cert, keyPass)) {
							Files.write(newCert, cert);

							// now load the certificate
							mdmImporter.addCertificate(newCert.toString(), keyPass);

							return "Certificate Imported";
						}
					}
					return "Error Importing Certificate";
				}
				return "no keyPassphrase specified";
			});
		} catch (IOException e) {
			logger.error("Couldn't create Certificate Path", e);
		}
		get("/", (req, res) -> "<a href=\"start\">start?subscriptionID={subscriptionID}</href><br/><a href=\"stopImport\">stopImport?id={uuid}</href><br/><a href=\"stopAll\">stopAll</href>");
	}

	private boolean checkCertificate(byte[] cert, String keyPass) {
		try {
			KeyStore p12 = KeyStore.getInstance("pkcs12");
			p12.load(new ByteArrayInputStream(cert), keyPass.toCharArray());
		} catch (IOException | NoSuchAlgorithmException | KeyStoreException | CertificateException e) {
			e.printStackTrace();
			return false;
		}
		return true;
	}

	public void start() {
		Spark.awaitInitialization();
		System.out.println("Listening on: http://localhost:" + port());
	}

	public void stop() {
	}

	public static void main(String[] args) throws InterruptedException, UnirestException, IOException {
		Spark.port(4567);
		MDMImporterApplication application = new MDMImporterApplication();
		application.start();

		File file = new File("mdm-test.hahnpro.com.p12");

		System.out.println(file.getAbsolutePath());

		// sleep 5s

		Thread.sleep(3000);

		// send certificates request
		 final InputStream stream = new FileInputStream(file);

		final byte[] bytes = new byte[stream.available()];
		stream.read(bytes);
		stream.close();

		HttpResponse<InputStream> httpResponse = Unirest.post("http://localhost:4567/addCertificate?keyPassphrase=qJT78NDcGU")
				.body(bytes)
				.asBinary();

		System.out.println(new BufferedReader(new InputStreamReader(httpResponse.getBody()))
				.lines()
				.collect(Collectors.joining("\n")));


		// send subscription id
		HttpResponse<InputStream> nodeHttpResponse = Unirest.get("http://localhost:4567/start?subscriptionID=2683000").asBinary();

		System.out.println(new BufferedReader(new InputStreamReader(nodeHttpResponse.getBody()))
				.lines()
				.collect(Collectors.joining("\n")));


	}

}
