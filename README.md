# yandexcloud_dht
Навык для Алисы

Датчик DHT подключен к RPi. 
RPi с определенной периодичностью собирает данные и отправляет в YandexDataBase (YDB) на Yandex.Cloud. 
Алиса по запросу "Узнай у датчика температуру" забирает данные с YDB и озвучивает их.
