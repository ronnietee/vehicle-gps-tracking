import json
import os
import time
from collections import defaultdict
from datetime import datetime, timedelta, timezone
from kafka import KafkaConsumer, KafkaProducer
from kafka.errors import KafkaError


class GPSStreamProcessor:
    def __init__(
        self,
        kafka_bootstrap_servers,
        input_topic,
        output_topic,
        window_seconds=60
    ):
        self.consumer = KafkaConsumer(
            input_topic,
            bootstrap_servers=kafka_bootstrap_servers,
            value_deserializer=lambda m: json.loads(m.decode('utf-8')),
            key_deserializer=lambda k: k.decode('utf-8') if k else None,
            auto_offset_reset='latest',
            enable_auto_commit=True,
            group_id='gps-stream-processor'
        )
        
        self.producer = KafkaProducer(
            bootstrap_servers=kafka_bootstrap_servers,
            value_serializer=lambda v: json.dumps(v).encode('utf-8'),
            key_serializer=lambda k: k.encode('utf-8') if k else None
        )
        
        self.output_topic = output_topic
        self.window_seconds = window_seconds
        
        # Windowed data storage: {vehicle_id: {window_start: [data_points]}}
        self.windows = defaultdict(lambda: defaultdict(list))
        
        # Last known positions for distance calculation
        self.last_positions = {}

    def calculate_distance(self, lat1, lon1, lat2, lon2):
        """Calculate distance between two GPS points using Haversine formula."""
        from math import radians, sin, cos, sqrt, atan2
        
        R = 6371  # Earth radius in km
        
        lat1_rad = radians(lat1)
        lat2_rad = radians(lat2)
        delta_lat = radians(lat2 - lat1)
        delta_lon = radians(lon2 - lon1)
        
        a = (sin(delta_lat / 2) ** 2 +
             cos(lat1_rad) * cos(lat2_rad) * sin(delta_lon / 2) ** 2)
        c = 2 * atan2(sqrt(a), sqrt(1 - a))
        
        return R * c

    def get_window_start(self, timestamp_str):
        """Get window start time for a given timestamp."""
        dt = datetime.fromisoformat(timestamp_str.replace('Z', '+00:00'))
        window_start = dt.replace(second=0, microsecond=0)
        # Round down to window_seconds
        seconds = (dt.second // self.window_seconds) * self.window_seconds
        window_start = window_start.replace(second=seconds)
        return window_start

    def process_gps_data(self, gps_data):
        """Process a single GPS data point."""
        vehicle_id = gps_data['vehicle_id']
        timestamp = gps_data['timestamp']
        window_start = self.get_window_start(timestamp)
        
        # Store in window
        self.windows[vehicle_id][window_start].append(gps_data)
        
        # Calculate distance if we have previous position
        if vehicle_id in self.last_positions:
            last = self.last_positions[vehicle_id]
            distance = self.calculate_distance(
                last['latitude'], last['longitude'],
                gps_data['latitude'], gps_data['longitude']
            )
            gps_data['distance_from_last'] = distance
        else:
            gps_data['distance_from_last'] = 0.0
        
        # Update last position
        self.last_positions[vehicle_id] = {
            'latitude': gps_data['latitude'],
            'longitude': gps_data['longitude'],
            'timestamp': timestamp
        }

    def aggregate_window(self, vehicle_id, window_start, data_points):
        """Aggregate data points for a time window."""
        if not data_points:
            return None
        
        speeds = [dp['speed'] for dp in data_points]
        distances = [dp.get('distance_from_last', 0) for dp in data_points]
        
        window_end = window_start + timedelta(seconds=self.window_seconds)
        
        aggregated = {
            'vehicle_id': vehicle_id,
            'window_start': window_start.isoformat() + 'Z',
            'window_end': window_end.isoformat() + 'Z',
            'avg_speed': round(sum(speeds) / len(speeds), 2),
            'max_speed': round(max(speeds), 2),
            'min_speed': round(min(speeds), 2),
            'distance_traveled': round(sum(distances), 2),
            'data_points_count': len(data_points),
            'first_latitude': data_points[0]['latitude'],
            'first_longitude': data_points[0]['longitude'],
            'last_latitude': data_points[-1]['latitude'],
            'last_longitude': data_points[-1]['longitude']
        }
        
        return aggregated

    def process_completed_windows(self):
        """Process and emit completed time windows."""
        current_time = datetime.now(timezone.utc)
        cutoff_time = current_time - timedelta(seconds=self.window_seconds * 2)
        
        completed_windows = []
        
        for vehicle_id, vehicle_windows in list(self.windows.items()):
            for window_start, data_points in list(vehicle_windows.items()):
                # Consider window complete if it's old enough
                if window_start < cutoff_time:
                    aggregated = self.aggregate_window(vehicle_id, window_start, data_points)
                    if aggregated:
                        completed_windows.append((vehicle_id, aggregated))
                    # Remove processed window
                    del vehicle_windows[window_start]
        
        # Emit aggregated data
        for vehicle_id, aggregated in completed_windows:
            try:
                self.producer.send(
                    self.output_topic,
                    key=vehicle_id,
                    value=aggregated
                )
            except KafkaError as e:
                print(f"Error publishing aggregated data: {e}")
        
        if completed_windows:
            self.producer.flush()
            print(f"Processed and emitted {len(completed_windows)} completed windows")

    def run(self):
        """Main processing loop."""
        print(f"Starting stream processor...")
        print(f"Input topic: {self.consumer.config['group_id']}")
        print(f"Output topic: {self.output_topic}")
        print(f"Window size: {self.window_seconds} seconds")
        
        message_count = 0
        
        try:
            for message in self.consumer:
                try:
                    gps_data = message.value
                    self.process_gps_data(gps_data)
                    message_count += 1
                    
                    if message_count % 1000 == 0:
                        print(f"Processed {message_count} messages...")
                    
                    # Process completed windows every 100 messages
                    if message_count % 100 == 0:
                        self.process_completed_windows()
                        
                except Exception as e:
                    print(f"Error processing message: {e}")
                    continue
                    
        except KeyboardInterrupt:
            print("\nStopping stream processor...")
        finally:
            # Process any remaining windows
            print("Processing remaining windows...")
            for vehicle_id, vehicle_windows in self.windows.items():
                for window_start, data_points in vehicle_windows.items():
                    aggregated = self.aggregate_window(vehicle_id, window_start, data_points)
                    if aggregated:
                        try:
                            self.producer.send(
                                self.output_topic,
                                key=vehicle_id,
                                value=aggregated
                            )
                        except KafkaError as e:
                            print(f"Error publishing: {e}")
            
            self.producer.flush()
            self.consumer.close()
            self.producer.close()
            print(f"Total messages processed: {message_count}")


def main():
    kafka_bootstrap = os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'localhost:9092')
    input_topic = os.getenv('INPUT_TOPIC', 'gps-raw-data')
    output_topic = os.getenv('OUTPUT_TOPIC', 'gps-aggregated-data')
    window_seconds = int(os.getenv('AGGREGATION_WINDOW_SECONDS', '60'))
    
    print(f"Kafka Bootstrap Servers: {kafka_bootstrap}")
    print(f"Input Topic: {input_topic}")
    print(f"Output Topic: {output_topic}")
    
    # Wait for Kafka to be ready
    print("Waiting for Kafka to be ready...")
    time.sleep(10)
    
    processor = GPSStreamProcessor(
        kafka_bootstrap,
        input_topic,
        output_topic,
        window_seconds
    )
    
    processor.run()


if __name__ == '__main__':
    main()
