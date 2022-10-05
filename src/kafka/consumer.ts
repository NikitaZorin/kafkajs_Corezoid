import { Consumer, ConsumerSubscribeTopics, EachBatchPayload, EachMessagePayload } from 'kafkajs';
import axios from 'axios';

interface corezoidConfig {
  url: string;
  method: string;
  data: {messages: object[]};
}

class ConsumerFactory {
  private consumer: Consumer;

  public constructor(consumer: Consumer) {
    this.consumer = consumer;
  }

  private shutdown() {
    this.consumer.disconnect()
  }

  public sendToCorezoid(corezoidConfig: corezoidConfig) {
    return new Promise(resolve => {
      axios(corezoidConfig)
          .then(function(response) {
              resolve(response.data);
          })
          .catch(function(error) {
              resolve(error);
          });
  });
  }

  public async startBatchConsumer(topicName: string, corezoidConfig: corezoidConfig) {
    const topic: ConsumerSubscribeTopics = {
      topics: [topicName],
      fromBeginning: true
    }

    try {
      await this.consumer.connect();
      await this.consumer.subscribe(topic);
      this.consumer.run({
        eachBatchAutoResolve: false,
        eachBatch: async (eachBatchPayload: EachBatchPayload) => {
          const { batch } = eachBatchPayload;
          const requestData: object[] = [];
          for (const message of batch.messages) {
            // JSON.parse(
            const value = message.value ? message.value.toString() : null;
            requestData.push({
              topic: batch.topic,
              partion: batch.partition,
              offset: message.offset,
              message: value,
              timestamp: message.timestamp
            });
          }
          corezoidConfig.data.messages = requestData;
          const result = await this.sendToCorezoid(corezoidConfig)
          console.log(result);
          this.shutdown();
        }
      })
    } catch (error) {
      this.shutdown();
      return error;
    }
  }
}

export { ConsumerFactory, corezoidConfig };
