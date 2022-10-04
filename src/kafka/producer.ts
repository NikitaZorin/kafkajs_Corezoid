import { TopicMessages, Producer, ProducerBatch, Admin } from 'kafkajs';

export default class ProducerFactory {
  producer: Producer;

  constructor(producer: Producer) {
    this.producer = producer;
  }

  private shutdown() {
    this.producer.disconnect();
  }

  async start(): Promise<void> {
    try {
      await this.producer.connect()
    } catch (error) {
      console.log('Error connecting the producer: ', error)
    }
  }

  async send(message: object, topic: string) {
    try {
      const topicMessages: TopicMessages = {
        topic: topic,
        messages: [{ value: JSON.stringify(message) }],
      };
  
     const result = await this.producer.send(topicMessages);
     console.log(result);
     this.shutdown();
    } catch (error) {
      return error;
    }
  }

  // public async sendBatch(messages: []): Promise<void> {
  //   const kafkaMessages = messages.map((message) => {
  //     return {
  //       value: JSON.stringify(message),
  //     };
  //   });

  //   const topicMessages: TopicMessages = {
  //     topic: 'producer-topic',
  //     messages: kafkaMessages,
  //   };

  //   const batch: ProducerBatch = {
  //     topicMessages: [topicMessages],
  //   };

  //   await this.producer.sendBatch(batch);
  // }
}
