## Kubernetes


### Create ConfigMap

```
kubectl create configmap spark-conf --from-file=spark-conf
```

This loads the contents of the folder spark-conf as kubernetes configmap.

Using log4j.properties in the configmap; did set logging for driver; however, the executor still was outputing INFO level messages.


### Deploy flight-stateful

```
kubectl apply -f flight-stateful.yaml
```

### Deploy rttest

```
kubectl apply -f rttest-send-deploy-test.yaml
```

We'll use rttest to send messages to Kafka Topic.

### Watch flight-stateful exec logs

```
kubectl logs flight-stateful-1553280004615-exec-1 -f | grep "^Update"

```


### Send Messages to Kafka

Look up the pod name for rttest (e.g.  kubectl get pods | grep rttest)

Then exec into pod running the tmux command (tmux is a terminal multiplexer; it keeps the connection open)

```
kubectl exec -it rttest-send-deployment-5479dc68cc-r82h2 tmux
```

Now send messages

```
cd /opt/rttest
java -cp /opt/rttest/target/rttest.jar com.esri.rttest.send.Kafka gateway-cp-kafka:9092 flight-stateful Flight.csv 1 10
```

### Confirm messages Output

You should see the messages in the logs.

For example:

```
Update received for flightId=SWA2706, longitude=-79.58573904399998, latitude=34.26552103900008!
Update received for flightId=SWA724, longitude=-76.40528883799993, latitude=39.57327096200004!
Update received for flightId=SWA2358, longitude=-81.20225994099997, latitude=32.09926344000007!
Update received for flightId=ASA2, longitude=-119.91909917099997, latitude=47.59454117000007!
Update received for flightId=ASA6, longitude=-116.68665487499999, latitude=34.39335798500008!
Update received for flightId=SWA510, longitude=-77.02741552499998, latitude=38.900886340000056!
Update received for flightId=SWA1568, longitude=-113.66455713799996, latitude=36.777755110000044!
Update received for flightId=SWA992, longitude=-82.06741414899994, latitude=39.63095683900008!
Update received for flightId=ASA3, longitude=-89.78130970899997, latitude=44.79860504800007!
Update received for flightId=SWA2706, longitude=-79.95414262199995, latitude=33.654397964000054!
```





