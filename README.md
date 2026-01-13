# Documentation: Vehicle GPS Tracking

## System Overview
This is a Data Engineering Project to demonstrate data streaming using microservices in docker containers. Its a complete vehicle GPS tracking system with real-time streaming of timestamped GPS tracking data. The system uses Kafka and a microservices architecture with 3 independent services;

### Data generator 
Generates vehicle GPS telematics data continuously in real-time and pushes this data directly to Kafka. By default, 5 vehicles will be created and data will be generated at 100messages/second although in the docker-compose yml, vehicles count and messages per second can be configured

### Streaming data processor 
Performs aggregation of speed and distance (over 60s windows by default) of data pushed to Kafka from the data generator. The window period can also be configured in the docker-compose yml file.

### API-Service 
REST API with endpoints for;
- **List of vehicles**: `/api/vehicles`
- **Current vehicle location**: `/api/vehicles/<vehicle_id>/current`
- **Events stream**: `/api/stream`
- **Aggregated metrics**: `/api/aggregates`
- **Health check**: `/health`

These microservices are containerized using docker and linked to operate together using docker compose. Kafka uses event driven architecture making the system scalable and fault tolerant. Use of microservices allows independent scaling. Kafka topics are partitioned on vehicle_id allowing parallel processing per partition and hence horizontal scalability.

The Kafka architecture comprises Zookeeper (Coordination), Kafka Broker (Message broker), Kafka UI (Web interface for monitoring)


## System Flow

```
Data Generator → Kafka (gps-raw-data) 
                      ↓
              Stream Processor
                      ↓
              Kafka (gps-aggregated-data)
                      ↓
              API Service → Real-time Reports
```

## Prerequisites

- Docker and Docker Compose installed
- Python 3.11 recommended. Compatibility challenges with kafka-python can be faced with later versions
- At least 4GB RAM available for Docker containers


## Getting Started (Installation and Usage)

1. **Clone the repository** (or use this local project):
   ```bash
   git clone "https://github.com/ronnietee/vehicle-gps-tracking"
   cd data-engineering-project
   ```

2. **Set up environment variables**:
   The user has latitude to configure;
   - The number of vehicles to generate data for
   - Messages generated per second
   - Window period for aggregation of speed and distance
   Set these up in the docker-compose.yml file

3. **Start all services**:
   ```bash
   docker-compose up -d
   ```

4. **Check service status**:
   ```bash
   docker-compose ps
   ```

5. **View logs and monitor**:
   ```bash
   docker-compose logs -f [service-name]
   ```
   You can also open the kafka UI on "http://localhost:8080" to monitor kafka topics and messages

5. **Make API calls to retrieve aggregated or raw metrics**:
   base_url: "http://localhost:8000"
   Available endpoints:
        - **List of vehicles**: `/api/vehicles`
        - **Current vehicle location**: `/api/vehicles/<vehicle_id>/current`
        - **Events stream**: `/api/stream`
        - **Aggregated metrics**: `/api/aggregates`
        - **Health check**: `/health`
