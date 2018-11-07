const express = require('express');
const kafka = require('kafka-node');
const bodyParser = require('body-parser')

const app = express()

app.use(bodyParser.json())          // to support JSON-encoded bodies
app.use(bodyParser.urlencoded({     // to support URL-encoded bodies
  extended: true
}))

const Producer = kafka.Producer
const client = new kafka.KafkaClient({kafkaHost: 'localhost:9092'})
const producer = new Producer(client)

producer.on('ready',  () => {
    console.log('Producer is ready');
});

producer.on('error', (err) => {
    console.log('Producer is in error state');
    console.log(err);
})


app.get('/', (req,res) => {
    res.json({greeting:'Kafka Producer'})
});

app.post('/send', (req,res) => {
  const sentMessage = JSON.stringify(req.body.message);
  const payload = [
      { topic: req.body.topic, messages:sentMessage , partition: 0 }
  ];
  console.log("Incoming message");
  console.log(payload)
  producer.send(payload, (err, data) => {
          res.json(data);
  });
})

app.listen(5001, () => {
    console.log('Kafka producer running at 5001')
})