# Companion SocketIO Handler

WebSocket gateway for the Companion ride-sharing app. Manages real-time connections from mobile apps, authenticates via JWT, routes events through Kafka.

## Architecture

```
Mobile App ←→ [SocketIO Handler] ←→ Kafka
                    ↑
              JWT from auth-service
```

- Authenticates connections using JWT access tokens (shared `JWT_SECRET_KEY` with auth-service)
- Publishes client events (location updates, ride requests) to Kafka topics
- Consumes Kafka topics (ride updates, driver assignments) and pushes to connected clients
- Manages Socket.IO rooms per ride for grouped broadcasting

## Socket.IO Events

### Client → Server

| Event | Who | Payload | Produces to |
|-------|-----|---------|-------------|
| `connect` | both | `{token: "jwt..."}` in auth | — |
| `location_update` | both | `{latitude, longitude, timestamp, accuracy}` | `driver-locations` / `passenger-locations` |
| `ride_request` | passenger | `{pickup_lat, pickup_lon, dropoff_lat, dropoff_lon}` | `ride-requests` |
| `ride_cancel` | both | `{ride_id, reason}` | `ride-requests` |

### Server → Client (from Kafka)

| Event | To | Kafka topic |
|-------|-----|-------------|
| `ride_update` | ride room | `ride-updates` |
| `driver_assigned` | passenger | `driver-assigned` |
| `driver_location` | passenger | `driver-location-to-passenger` |

## Setup

### Prerequisites

- Python 3.13+
- [uv](https://docs.astral.sh/uv/)
- Kafka running (see root `docker-compose.yml`)

### Installation

```bash
uv sync
cp .env.example .env
# Set JWT_SECRET_KEY to match auth-service
```

### Run

```bash
# Start Kafka first
docker compose -f ../docker-compose.yml up -d

# Start the service
uv run uvicorn app.main:app --reload --port 8001
```

Health check: http://localhost:8001/health

## Connecting

```javascript
import { io } from "socket.io-client";

const socket = io("http://localhost:8001", {
  auth: { token: "your-jwt-access-token" }
});

socket.on("connect", () => console.log("Connected"));
socket.on("driver_assigned", (data) => console.log("Driver:", data));
socket.on("ride_update", (data) => console.log("Ride:", data));

// Send location
socket.emit("location_update", {
  latitude: 43.238,
  longitude: 76.945,
  timestamp: new Date().toISOString(),
  accuracy: 5.0
}, (ack) => console.log(ack));
```

## Environment Variables

| Variable | Description | Default |
|----------|-------------|---------|
| `JWT_SECRET_KEY` | Must match auth-service | — |
| `JWT_ALGORITHM` | JWT algorithm | `HS256` |
| `KAFKA_BOOTSTRAP_SERVERS` | Kafka brokers | `localhost:9092` |
| `RATE_LIMIT_LOCATION_PER_SECOND` | Max location updates/sec | `2` |
| `RATE_LIMIT_DEFAULT_PER_SECOND` | Max other events/sec | `5` |
