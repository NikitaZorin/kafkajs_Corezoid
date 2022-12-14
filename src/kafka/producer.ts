import { TopicMessages, Producer } from 'kafkajs';

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
      await this.producer.connect();
    } catch (error) {
      console.log('Error connecting the producer: ', error)
    }
  }


  async send(messages: {key: string, value: any}[], topic: string) {
    try {
      const topicMessages: TopicMessages = {
        topic: topic,
        messages: messages,
        // messages: [{ value: JSON.stringify(message) }],
      };
  
     const result = await this.producer.send(topicMessages);
     this.shutdown();
     return result;
    } catch (error) {
      return error;
    }
  }
}
