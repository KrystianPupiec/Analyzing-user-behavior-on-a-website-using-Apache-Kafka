// Importowanie wymaganych modułów

// Express.js - framework do budowy aplikacji sieciowych
const express = require('express'); 
// kafkajs - biblioteka do komunikacji z Apache Kafka
const { Kafka } = require('kafkajs'); 
// axios - biblioteka do wykonywania żądań HTTP
const axios = require('axios'); 

// Inicjalizacja aplikacji Express.js
const app = express();

// Inicjalizacja klienta Kafka

// Identyfikator klienta Kafka
const kafka = new Kafka({
  clientId: 'my-app', 
  // Adresy brokerów Kafka
  brokers: ['localhost:9092'] 
});

// Inicjalizacja admina Kafka oraz producenta Kafka

// Admin - do operacji administracyjnych na Kafka
const admin = kafka.admin(); 
// Producent - do wysyłania wiadomości do Kafka
const producer = kafka.producer(); 

// Funkcja sprawdzająca i ewentualnie tworząca temat w Kafka
const createTopicIfNotExists = async (topic) => {
  // Nawiązanie połączenia z adminem Kafka
  await admin.connect(); 
  // Pobranie listy tematów z Kafka
  const topics = await admin.listTopics(); 
  // Sprawdzenie, czy temat już istnieje
  if (!topics.includes(topic)) { 
    // Tworzenie tematu, jeśli nie istnieje
    await admin.createTopics({ 
      topics: [{ topic }]
    });
  }
};

// Ustawienie nagłówków CORS
app.use((req, res, next) => {
  res.header("Access-Control-Allow-Origin", "*");
  res.header("Access-Control-Allow-Headers", "Origin, X-Requested-With, Content-Type, Accept");
  next();
});

// Połączenie z Kafka i uruchomienie serwera
(async () => {
  try {
    // Nawiązanie połączenia z producentem Kafka
    await producer.connect(); 
    // Nawiązanie połączenia z adminem Kafka
    await admin.connect(); 
    // Uruchomienie serwera Express.js
    await app.listen(3000, () => console.log('Serwer działa na porcie 3000')); 
  } catch (error) {
    // Obsługa błędów inicjalizacji serwera
    console.error('Błąd podczas inicjalizacji serwera:', error); 
  }
})();

// Obsługa zapytania GET
app.get('/sendToKafka', async (req, res) => {
  // Pobranie danych z zapytania GET
  const { age, city, gender, topic, id } = req.query; 
  
  // Sprawdzenie, czy ID to #home
  if (topic === 'home') {
    res.header("Access-Control-Allow-Origin", "*");
    // Pominięcie tworzenia tematu, jeśli ID=#home
    return res.send('Skipping topic creation for ID=#home'); 
  }
  else{
    console.log('Otrzymane dane:');
    console.log('Wiek:', age);
    console.log('Miasto:', city);
    console.log('Płeć:', gender);
    console.log('Topic:', topic);
    // Nazwa tematu dla wieku
    const ageTopic = `${topic}_age`;
    // Nazwa tematu dla miasta 
    const cityTopic = `${topic}_city`; 
    // Nazwa tematu dla płci
    const genderTopic = `${topic}_gender`; 
    // Nazwa tematu dla kliknięć
    const clicksTopic = `${topic}_clicks`; 

    try {
      // Sprawdzenie i ewentualne tworzenie tematów w Kafka
      await createTopicIfNotExists(ageTopic);
      await createTopicIfNotExists(cityTopic);
      await createTopicIfNotExists(genderTopic);
      await createTopicIfNotExists(clicksTopic);

      // Wysyłanie wiadomości do odpowiednich tematów Kafka
      await producer.send({
        topic: ageTopic,
        messages: [{ value: age.toString() }],
      });

      await producer.send({
        topic: cityTopic,
        messages: [{ value: city.toString() }],
      });

      await producer.send({
        topic: genderTopic,
        messages: [{ value: gender.toString() }],
      });

      await producer.send({
        topic: clicksTopic,
        messages: [{ value: '1' }],
      });

      // Dodanie nagłówka Access-Control-Allow-Origin
      res.header("Access-Control-Allow-Origin", "*");
      // Odpowiedź na zapytanie
      res.send('Wiadomość wysłana do Kafka'); 
    } catch (error) {
      // Obsługa błędów wysyłania do Kafka
      console.error('Błąd podczas wysyłania wiadomości do Kafka:', error); 
      // Wysłanie odpowiedzi błędu
      res.status(500).send('Wystąpił błąd podczas wysyłania wiadomości do Kafka'); 
    }
    }
  
});
