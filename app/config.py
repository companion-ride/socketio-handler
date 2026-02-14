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
    KAFKA_TOPIC_DRIVER_ASSIGNED: str = "driver-assigned"
    KAFKA_TOPIC_DRIVER_LOCATION_TO_PASSENGER: str = "driver-location-to-passenger"

    RATE_LIMIT_LOCATION_PER_SECOND: int = 2
    RATE_LIMIT_DEFAULT_PER_SECOND: int = 5

    HOST: str = "0.0.0.0"
    PORT: int = 8001


settings = Settings()
