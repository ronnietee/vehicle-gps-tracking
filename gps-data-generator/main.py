import json
import os
import random
import time
from datetime import datetime, timedelta, timezone
from kafka import KafkaProducer
from kafka.errors import KafkaError


class GPSDataGenerator:
    def __init__(
        self,
        kafka_bootstrap_servers,
        topic_name='gps-raw-data'
    ):
        self.producer = KafkaProducer(
            bootstrap_servers=kafka_bootstrap_servers,
            value_serializer=lambda v: json.dumps(v).encode('utf-8'),
            key_serializer=lambda k: k.encode('utf-8') if k else None
        )
        self.topic_name = topic_name
        self.vehicles = []
        self.running = True

    def initialize_vehicles(self, count=5):
        """
        Initialize vehicle fleet with starting positions. Default is five vehicles
        more vehicles can be added for simulation
        """
        # South Africa, Johannesburg coordinates
        base_lat = -26.20410
        base_lon = 28.04731
        
        for i in range(1, count + 1):
            vehicle_id = f"V{i:03d}"
            self.vehicles.append({
                'id': vehicle_id,
                'lat': base_lat + random.uniform(-0.5, 0.5),
                'lon': base_lon + random.uniform(-0.5, 0.5),
                'speed': random.uniform(30, 70),  # km/h
                'heading': random.uniform(0, 360),
                'altitude': random.uniform(0, 200)
            })
        
        print(f"Initialized {len(self.vehicles)} vehicles")

    def generate_gps_point(self, vehicle):
        """
        Generate a single GPS data point for a vehicle. This has been 
        randomly generated for this project so it will not coincide with 
        actual Joburg roads. Also, movements may not be 100% convincing of an actual
        car movement despite the effort made in this method to achieve this
        """
        # Simulate movement
        vehicle['lat'] += random.uniform(-0.001, 0.001)
        vehicle['lon'] += random.uniform(-0.001, 0.001)
        
        # Keep within reasonable bounds (J'burg area). 
        vehicle['lat'] = max(-27.0, min(-25.0, vehicle['lat']))
        vehicle['lon'] = max(26.0, min(30.0, vehicle['lon']))
        
        # Vary speed
        speed_variance = 10
        vehicle['speed'] = max(0, vehicle['speed'] + random.uniform(-speed_variance, speed_variance))
        
        # Update heading slightly
        vehicle['heading'] = (vehicle['heading'] + random.uniform(-5, 5)) % 360
        
        # Create GPS data point
        gps_data = {
            'vehicle_id': vehicle['id'],
            'timestamp': datetime.now(timezone.utc).isoformat().replace('+00:00', 'Z'),
            'latitude': round(vehicle['lat'], 6),
            'longitude': round(vehicle['lon'], 6),
            'speed': round(vehicle['speed'], 2),
            'heading': round(vehicle['heading'], 2),
            'altitude': round(vehicle['altitude'], 2)
        }
        
        return gps_data

    def send_to_kafka(self, gps_data):
        """Send GPS data to Kafka topic."""
        try:
            future = self.producer.send(
                self.topic_name,
                key=gps_data['vehicle_id'],
                value=gps_data
            )

        except KafkaError as e:
            print(f"Error sending to Kafka: {e}")

    def run_continuous(self, messages_per_second=100):
        """
        Continuously generate and send GPS data. Default is at 100messages per sec
        """
        print(f"Starting continuous data generation at {messages_per_second} messages/second")
        
        interval = 1.0 / messages_per_second
        message_count = 0
        
        try:
            while self.running:
                start_time = time.time()
                
                # Generate data for a random vehicle
                vehicle = random.choice(self.vehicles)
                gps_data = self.generate_gps_point(vehicle)
                self.send_to_kafka(gps_data)
                
                message_count += 1
                
                if message_count % 1000 == 0:
                    print(f"Generated {message_count} messages...")
                
                # Sleep to maintain rate
                elapsed = time.time() - start_time
                sleep_time = max(0, interval - elapsed)
                time.sleep(sleep_time)
                
        except KeyboardInterrupt:
            print("\nStopping data generator...")
        finally:
            self.producer.close()
            print(f"Total messages generated: {message_count}")


def main():
    kafka_bootstrap = os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'localhost:9092')
    vehicles_count = int(os.getenv('VEHICLES_COUNT', '5'))
    messages_per_second = int(os.getenv('MESSAGES_PER_SECOND', '100'))
    
    print(f"Kafka Bootstrap Servers: {kafka_bootstrap}")
    
    generator = GPSDataGenerator(kafka_bootstrap)
    generator.initialize_vehicles(vehicles_count)
    
    # Wait for Kafka to be ready
    print("Waiting for Kafka to be ready...")
    time.sleep(10)

    # begin generating tracking events 
    generator.run_continuous(messages_per_second)

if __name__ == '__main__':
    main()
