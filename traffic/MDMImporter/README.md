MDMImporter

**get**:`/start?subscriptionID=SUBSCRIPTION_ID` 
Startet den Import mit der angegebenen `SUBSCRIPTION_ID`.
Momentan werden die daten in den ordner `/output` gespeichert..
Gibt eine ID zurück, die benutzt werden kann, um diesen Import task wieder zu stoppen.
TODO: Weiterleiten der dateien an nächsten Microservice.

**get**:`/stop?id=ID`
Stoppt den Import mit der id `ID`, die beim Starten eines Task zurückgegeben wurde.

**get**:`/stopAll`
Stoppt alle import tasks.

**post**:`/addCertificate?keyPassphrase=KEY_PASSPHRASE`
Fügt ein Zertifikat zur Authentifizierung hinzu. Das Zertificat wird binär im Body übertragen.

