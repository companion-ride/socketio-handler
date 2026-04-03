from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env", env_file_encoding="utf-8")

    JWT_SECRET_KEY: str
    JWT_ALGORITHM: str = "HS256"

    KAFKA_BOOTSTRAP_SERVERS: str = "localhost:9092"
    KAFKA_CLIENT_ID: str = "socketio-handler"
    KAFKA_GROUP_ID: str = "socketio-handler-group"

    # Topics - produce
    KAFKA_TOPIC_DRIVER_LOCATIONS: str = "driver-locations"
    KAFKA_TOPIC_PASSENGER_LOCATIONS: str = "passenger-locations"
    KAFKA_TOPIC_RIDE_REQUESTS: str = "ride-requests"

    # Topics - consume
    KAFKA_TOPIC_RIDE_UPDATES: str = "ride-updates"
    KAFKA_TOPIC_RIDE_NOTIFICATIONS: str = "ride-notifications"
    KAFKA_TOPIC_DRIVER_ASSIGNED: str = "driver-assigned"
    KAFKA_TOPIC_DRIVER_LOCATION_TO_PASSENGER: str = "driver-location-to-passenger"

    CONFIG_SERVICE_URL: str = ""
    CONFIG_SERVICE_API_KEY: str = ""

    # Chat service
    CHAT_SERVICE_URL: str = "http://localhost:8083"

    # Chat Kafka topics (produce)
    KAFKA_TOPIC_CHAT_INBOUND: str = "chat-messages-inbound"

    # Chat Kafka topics (consume)
    KAFKA_TOPIC_CHAT_OUTBOUND: str = "chat-messages-outbound"
    KAFKA_TOPIC_CHAT_ROOM_EVENTS: str = "chat-room-events"

    RATE_LIMIT_LOCATION_PER_SECOND: int = 2
    RATE_LIMIT_DEFAULT_PER_SECOND: int = 5

    HOST: str = "0.0.0.0"
    PORT: int = 8001


settings = Settings()
