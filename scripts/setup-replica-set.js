// Initialize MongoDB replica set (required for change streams)
try {
  print('Initializing replica set...');
  
  rs.initiate({
    _id: "rs0",
    members: [
      {
        _id: 0,
        host: "mongo:27017"
      }
    ]
  });
  
  print('Replica set initialized successfully');
  
  // Wait for replica set to be ready
  while (rs.status().ok !== 1) {
    print('Waiting for replica set to be ready...');
    sleep(1000);
  }
  
  print('Replica set is ready');
  
} catch (error) {
  print('Error initializing replica set:', error);
}