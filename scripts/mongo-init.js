// Create initial database and collection
db = db.getSiblingDB('testdb');

// Create a sample collection
db.createCollection('events');

// Insert a sample document to initialize the collection
db.events.insertOne({
  message: 'Initial document for CDC testing',
  timestamp: new Date(),
  type: 'sample'
});

print('Database initialized with sample data');