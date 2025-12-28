import json
import os
import time
from collections import defaultdict
from datetime import datetime, timezone
from kafka import KafkaConsumer
from flask import Flask, jsonify, Response, stream_with_context
from flask_cors import CORS


# create an instance of a Flask web application
app = Flask(__name__)
CORS(app)

# In-memory cache for latest data
latest_aggregated = {}
latest_raw = defaultdict(list)
vehicle_stats = defaultdict(dict)

# Global consumer instance
kafka_consumer = None

class KafkaDataConsumer:
    def __init__(
        self,
        kafka_bootstrap_servers,
        aggregated_topic,
        raw_topic
    ):
        self.aggregated_consumer = KafkaConsumer(
            aggregated_topic,
            bootstrap_servers=kafka_bootstrap_servers,
            value_deserializer=lambda m: json.loads(m.decode('utf-8')),
            key_deserializer=lambda k: k.decode('utf-8') if k else None,
            auto_offset_reset='latest',
            enable_auto_commit=True,
            group_id='api-service-aggregated'
        )
        
        self.raw_consumer = KafkaConsumer(
            raw_topic,
            bootstrap_servers=kafka_bootstrap_servers,
            value_deserializer=lambda m: json.loads(m.decode('utf-8')),
            key_deserializer=lambda k: k.decode('utf-8') if k else None,
            auto_offset_reset='latest',
            enable_auto_commit=True,
            group_id='api-service-raw'
        )

    def start_consuming_aggregated(self):
        """Consume aggregated data and update cache."""
        for message in self.aggregated_consumer:
            try:
                data = message.value
                vehicle_id = data['vehicle_id']
                latest_aggregated[vehicle_id] = data
                vehicle_stats[vehicle_id] = {
                    'last_update': datetime.now(timezone.utc).isoformat().replace('+00:00', 'Z'),
                    'avg_speed': data.get('avg_speed', 0),
                    'distance_traveled': data.get('distance_traveled', 0),
                    'data_points': data.get('data_points_count', 0)
                }
            except Exception as e:
                print(f"Error processing aggregated message: {e}")
    
    def start_consuming_raw(self):
        """Consume raw data and update cache (keep last 100 per vehicle)."""
        for message in self.raw_consumer:
            try:
                data = message.value
                vehicle_id = data['vehicle_id']
                latest_raw[vehicle_id].append(data)
                # Keep only last 100 points per vehicle
                if len(latest_raw[vehicle_id]) > 100:
                    latest_raw[vehicle_id] = latest_raw[vehicle_id][-100:]
            except Exception as e:
                print(f"Error processing raw message: {e}")

@app.route('/health', methods=['GET'])
def health():
    """Health check endpoint."""
    return jsonify({
        'status': 'healthy',
        'timestamp': datetime.now(timezone.utc).isoformat().replace('+00:00', 'Z'),
        'vehicles_tracked': len(latest_aggregated)
    })


@app.route('/api/vehicles', methods=['GET'])
def get_vehicles():
    """Get list of all tracked vehicles."""
    vehicles = list(latest_aggregated.keys())
    return jsonify({
        'vehicles': vehicles,
        'count': len(vehicles)
    })


@app.route('/api/vehicles/<vehicle_id>/current', methods=['GET'])
def get_vehicle_current(vehicle_id):
    """Get current location and status of a vehicle."""
    if vehicle_id not in latest_raw or not latest_raw[vehicle_id]:
        return jsonify({'error': 'Vehicle not found'}), 404
    
    latest = latest_raw[vehicle_id][-1]
    return jsonify({
        'vehicle_id': vehicle_id,
        'latitude': latest['latitude'],
        'longitude': latest['longitude'],
        'speed': latest['speed'],
        'heading': latest['heading'],
        'altitude': latest['altitude'],
        'timestamp': latest['timestamp']
    })


@app.route('/api/vehicles/<vehicle_id>/stats', methods=['GET'])
def get_vehicle_stats(vehicle_id):
    """Get aggregated statistics for a vehicle."""
    if vehicle_id not in latest_aggregated:
        return jsonify({'error': 'Vehicle not found'}), 404
    
    aggregated = latest_aggregated[vehicle_id]
    stats = vehicle_stats.get(vehicle_id, {})
    
    return jsonify({
        'vehicle_id': vehicle_id,
        'last_update': stats.get('last_update'),
        'current_window': {
            'window_start': aggregated['window_start'],
            'window_end': aggregated['window_end'],
            'avg_speed': aggregated['avg_speed'],
            'max_speed': aggregated['max_speed'],
            'min_speed': aggregated['min_speed'],
            'distance_traveled': aggregated['distance_traveled'],
            'data_points': aggregated['data_points_count']
        },
        'location': {
            'start': {
                'latitude': aggregated['first_latitude'],
                'longitude': aggregated['first_longitude']
            },
            'end': {
                'latitude': aggregated['last_latitude'],
                'longitude': aggregated['last_longitude']
            }
        }
    })


@app.route('/api/aggregates', methods=['GET'])
def get_aggregates():
    """Get aggregated statistics across all vehicles."""
    if not latest_aggregated:
        return jsonify({
            'total_vehicles': 0,
            'aggregates': {}
        })
    
    all_speeds = []
    total_distance = 0
    total_data_points = 0
    
    for vehicle_id, data in latest_aggregated.items():
        all_speeds.append(data['avg_speed'])
        total_distance += data['distance_traveled']
        total_data_points += data['data_points_count']
    
    return jsonify({
        'total_vehicles': len(latest_aggregated),
        'aggregates': {
            'avg_speed_all_vehicles': round(sum(all_speeds) / len(all_speeds), 2) if all_speeds else 0,
            'max_speed_all_vehicles': round(max([d['max_speed'] for d in latest_aggregated.values()]), 2) if latest_aggregated else 0,
            'total_distance_traveled': round(total_distance, 2),
            'total_data_points': total_data_points
        },
        'timestamp': datetime.now(timezone.utc).isoformat().replace('+00:00', 'Z')
    })


@app.route('/api/stream', methods=['GET'])
def stream_updates():
    """Server-sent events stream for real-time updates."""
    def generate():
        last_vehicle_count = 0
        
        while True:
            current_count = len(latest_aggregated)
            
            # Send update if there are new vehicles or every 5 seconds
            if current_count != last_vehicle_count or True:
                data = {
                    'vehicles': list(latest_aggregated.keys()),
                    'count': current_count,
                    'timestamp': datetime.now(timezone.utc).isoformat().replace('+00:00', 'Z')
                }
                yield f"data: {json.dumps(data)}\n\n"
                last_vehicle_count = current_count
            
            time.sleep(5)
    
    return Response(stream_with_context(generate()), mimetype='text/event-stream')


@app.route('/api/vehicles/<vehicle_id>/recent', methods=['GET'])
def get_vehicle_recent(vehicle_id):
    """Get recent GPS points for a vehicle."""
    limit = int(os.getenv('RECENT_POINTS_LIMIT', '50'))
    
    if vehicle_id not in latest_raw:
        return jsonify({'error': 'Vehicle not found'}), 404
    
    recent_points = latest_raw[vehicle_id][-limit:]
    return jsonify({
        'vehicle_id': vehicle_id,
        'points': recent_points,
        'count': len(recent_points)
    })


def start_kafka_consumers():
    """Start background threads for Kafka consumers."""
    import threading
    
    def consume_aggregated():
        kafka_consumer.start_consuming_aggregated()
    
    def consume_raw():
        kafka_consumer.start_consuming_raw()
    
    thread1 = threading.Thread(target=consume_aggregated, daemon=True)
    thread2 = threading.Thread(target=consume_raw, daemon=True)
    
    thread1.start()
    thread2.start()
    
    print("Kafka consumers started")


def main():
    global kafka_consumer
    
    kafka_bootstrap = os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'localhost:9092')
    aggregated_topic = os.getenv('AGGREGATED_TOPIC', 'gps-aggregated-data')
    raw_topic = os.getenv('RAW_TOPIC', 'gps-raw-data')
    api_port = int(os.getenv('API_PORT', '8000'))
    
    print(f"Kafka Bootstrap Servers: {kafka_bootstrap}")
    print(f"Aggregated Topic: {aggregated_topic}")
    print(f"Raw Topic: {raw_topic}")
    
    # Wait for Kafka to be ready
    print("Waiting for Kafka to be ready...")
    time.sleep(10)
    
    kafka_consumer = KafkaDataConsumer(kafka_bootstrap, aggregated_topic, raw_topic)
    start_kafka_consumers()
    
    print(f"Starting API service on port {api_port}...")
    app.run(host='0.0.0.0', port=api_port, debug=False, threaded=True)


if __name__ == '__main__':
    main()
