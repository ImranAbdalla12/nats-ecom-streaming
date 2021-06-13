const nats = require("node-nats-streaming");

class Nats {
  constructor(client) {
    this.client = client;
  }
  connect(clusterID, clientID, url) {
    this.client = nats.connect(clusterID, clientID, { url });
    return new Promise((resolve, reject) => {
      this.client.on("connect", () => {
        console.log("Connected to NATS");
        resolve();
      });
      this.client.on("error", (err) => {
        reject(err);
      });
    });
  }
}

const newConnection = new Nats();

class Listener {
  constructor(subject, queueGroupName, ackWait, client) {
    this.subject = subject;
    this.queueGroupName = queueGroupName;
    this.ackWait = 5 * 1000;
    this.client = client;
  }

  subscriptionOptions() {
    this.client
      .subscriptionOptions()
      .setDeliverAllAvailable()
      .setManualAckMode()
      .setAckWait(this.ackWait)
      .setDurableName(this.queueGroupName);
  }
  listen() {
    const subsscription = this.client.subscribe(
      this.subject,
      this.queueGroupName,
      this.subscriptionOptions()
    );

    subsscription.on("message", (msg) => {
      console.log(`Message received ${this.subject} / ${this.queueGroupName}`);
      const parsedData = this.parseMessage(msg);
      this.onMessage(parsedData, msg);
    });
  }

  parseMessage(msg) {
    const data = msg.getData();
    return typeof data === String
      ? JSON.parse(data)
      : JSON.parse(data.toString("utf8"));
  }
  onMessage(data, msg) {
    console.log("Event data!", data);
    msg.ack();
  }
}

process.on("SIGINT", () => stan.close());
process.on("SIGTERM", () => stan.close());

class Publisher {
  constructor(subject, client) {
    this.subject = subject;
    this.client = client;
  }

  publish(data) {
    return new Promise((resolve, reject) => {
      this.client.publish(this.subject, data, (err) => {
        if (err) {
          return reject(err);
        }
        console.log("Event published to subject", this.subject);
        resolve();
      });
    });
  }
}

module.exports.newConnection = newConnection;
module.exports.Listener = Listener;
module.exports.Publisher = Publisher;
