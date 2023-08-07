# kafka
Sending msgs to a standalone kafka instance

## Start kafka and zookeeper

<code>
docker-compose up -d
</code>

## Check logs

<code>
docker-compose logs
</code>

## Run the test

Run the Main class from your IDE

* starts a consumer
* starts a producer that sends 1000 messages
* consumer receives 1000 msgs
* consumer shuts down
