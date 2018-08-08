package com.hahnpro.SMILE.mdm.importer;

import java.io.Serializable;
import java.util.*;

public class ImportTaskConfig implements Serializable {

	private List<String> resultHandlerList;
	private String subscriptionID;

	public static ImportTaskConfig create(LinkedHashMap<String, Object> importer) {
		ImportTaskConfig res = new ImportTaskConfig();

		Object resultHandler = importer.get("ResultHandler");
		if (resultHandler instanceof List) {
			res.resultHandlerList = (List<String>) resultHandler;
		}
		res.subscriptionID = "" + importer.get("SubscriptionID");
		return res;
	}

	public static ImporterRunnable toImporterTask(ImportTaskConfig importerConfig) {
		ImporterRunnable importerRunnable = new ImporterRunnable(null, importerConfig.subscriptionID, toResultHandler(importerConfig.resultHandlerList));
		return importerRunnable;
	}

	static ResultHandler[] toResultHandler(List resultHandlerList) {
		ArrayList<ResultHandler> result = new ArrayList<>();
		if (resultHandlerList != null) {
			for (Object o : resultHandlerList) {
				if (o instanceof String) {
					result.add(ResultHandler.getResultHandler((String) o));
				} else if (o instanceof HashMap) {
					// key is the handler class:

					Set set = ((HashMap) o).entrySet();
					Iterator iterator = set.iterator();
					while (iterator.hasNext()) {
						Map.Entry entry = (Map.Entry) iterator.next();

						ResultHandler resultHandler = ResultHandler.getResultHandler((String) entry.getKey());
						resultHandler.configure((Map<String, Object>) entry.getValue());

						result.add(resultHandler);
					}
				}
			}
		}
		return result.toArray(new ResultHandler[result.size()]);
	}
}
