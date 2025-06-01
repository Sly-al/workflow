# Workflow Task Manager

## Description
This is a workflow task management system built with Go, providing a robust API for handling task workflows. The system uses PostgreSQL for data storage and includes monitoring with Prometheus.

## Features
- Task workflow management
- RESTful API using FastHTTP
- PostgreSQL database integration
- Prometheus metrics
- Structured logging with logrus
- Environment-based configuration
- Docker support

## Prerequisites
- Go 1.24.0 or higher
- PostgreSQL
- Docker (optional)

## Project Structure
```
.
├── cmd/
│   └── main.go           # Application entry point
├── internal/
│   ├── app/             # Application core
│   ├── config/          # Configuration management
│   ├── handlers/        # HTTP handlers
│   ├── logger/          # Logging setup
│   ├── models/          # Data models
│   ├── repository/      # Database interactions
│   ├── service/         # Business logic
│   └── taskmanager/     # Task management logic
├── Dockerfile           # Docker configuration
├── go.mod              # Go module file
└── README.md           # This file
```

## Configuration
The application uses environment variables for configuration. Example configuration:

```env
DB_HOST=localhost
DB_PORT=5432
DB_USER=postgres
DB_PASSWORD=postgres
DB_NAME=workflow
DB_SSL_MODE=disable
HTTP_PORT=8080
```

## Running the Application

### Local Development
1. Clone the repository
2. Install dependencies:
   ```bash
   go mod download
   ```
3. Set up environment variables
4. Run the application:
   ```bash
   go run cmd/main.go
   ```

### Using Docker
1. Build the Docker image:
   ```bash
   docker build -t workflow .
   ```
2. Run the container:
   ```bash
   docker run -p 8080:8080 --env-file .env workflow
   ```

## API Documentation
The service provides the following endpoints:

- `POST /api/v1/tasks` - Create a new task
- `GET /api/v1/tasks` - List all tasks
- `GET /api/v1/tasks/{id}` - Get task by ID
- `PUT /api/v1/tasks/{id}` - Update task
- `DELETE /api/v1/tasks/{id}` - Delete task
- `GET /metrics` - Prometheus metrics endpoint

## Monitoring
The application exposes Prometheus metrics at the `/metrics` endpoint.