# Spark Docker Development Environment

A complete Apache Spark development environment running in Docker containers with JupyterLab integration, designed for data processing and analytics workflows.

## Overview

This project provides a containerized Apache Spark cluster with the following components:
- **Spark Master**: Cluster coordinator and resource manager
- **Spark Workers**: Distributed compute nodes (scalable)
- **Spark History Server**: Web UI for monitoring completed applications
- **JupyterLab**: Interactive development environment with PySpark integration

## Architecture

```
┌─────────────────┐    ┌─────────────────┐    ┌─────────────────┐
│   Spark Master  │    │  Spark Worker   │    │ History Server  │
│     (8080)      │◄──►│     (8081)      │    │     (18080)     │
└─────────────────┘    └─────────────────┘    └─────────────────┘
         ▲                       ▲                       ▲
         │                       │                       │
         └───────────────────────┼───────────────────────┘
                                 │
                    ┌─────────────────┐
                    │   JupyterLab    │
                    │     (8888)      │
                    └─────────────────┘
```

## Features

- 🚀 **Spark 3.5.0** with complete cluster setup
- 📊 **JupyterLab** with PySpark kernel pre-configured
- 📈 **History Server** for application monitoring
- 🔧 **Auto-scaling** worker nodes
- 💾 **Persistent storage** for data and notebooks
- 🎯 **Health checks** for all services
- ⚙️ **Configurable** via environment variables

## Quick Start

### Prerequisites

- Docker and Docker Compose installed
- At least 4GB RAM available
- Ports 7077, 8080, 8888, 18080 available

### 1. Clone and Start

```bash
# Clone the repository
git clone <repository-url>
cd spark-docker

# Start the cluster
docker-compose up -d
```

### 2. Access the Services

| Service | URL | Description |
|---------|-----|-------------|
| **JupyterLab** | http://localhost:8888 | Interactive development environment |
| **Spark Master UI** | http://localhost:8080 | Cluster management and monitoring |
| **History Server** | http://localhost:18080 | Completed applications history |

### 3. Verify Installation

```bash
# Check all services are running
docker-compose ps

# View logs
docker-compose logs -f
```

## Usage Examples

### Running Sample Applications

The project includes two sample PySpark applications:

#### 1. Pi Calculation
```bash
# Submit pi calculation job
docker-compose exec spark-master spark-submit \
  --master spark://spark-master:7077 \
  --deploy-mode client \
  /opt/bitnami/spark/apps/pi_calculation.py
```

#### 2. Word Count
```bash
# Submit word count job
docker-compose exec spark-master spark-submit \
  --master spark://spark-master:7077 \
  --deploy-mode client \
  /opt/bitnami/spark/apps/wordcount.py
```

### Using JupyterLab

1. Open JupyterLab at http://localhost:8888
2. Navigate to the `work` directory
3. Open `demo.ipynb` or create a new notebook
4. PySpark is pre-configured and ready to use:

```python
from pyspark.sql import SparkSession

# SparkSession is automatically configured to connect to the cluster
spark = SparkSession.builder \
    .appName("MyApp") \
    .getOrCreate()

# Your Spark code here
df = spark.range(1000).toDF("number")
df.show()
```

### Adding Your Own Applications

1. Place Python files in the `apps/` directory
2. Add data files to the `data/` directory  
3. Submit jobs using `spark-submit` or run interactively in JupyterLab

## Configuration

### Environment Variables

Key configuration options in `.env`:

```bash
# Cluster sizing
SPARK_WORKER_MEMORY=2G
SPARK_WORKER_CORES=2
SPARK_WORKER_REPLICAS=1

# Service ports
SPARK_MASTER_WEBUI_PORT=8080
JUPYTER_PORT=8888
SPARK_HISTORY_SERVER_PORT=18080

# Security (optional)
JUPYTER_TOKEN=your-secret-token
```

### Scaling Workers

```bash
# Scale to 3 worker nodes
docker-compose up -d --scale spark-worker=3

# Or set in .env file
SPARK_WORKER_REPLICAS=3
```

### Custom Spark Configuration

Edit `conf/spark-defaults.conf` to customize Spark settings:

```properties
spark.executor.memory               2g
spark.executor.cores                1
spark.sql.adaptive.enabled          true
spark.sql.shuffle.partitions        200
```

## Directory Structure

```
├── docker-compose.yml          # Service definitions
├── .env                        # Environment configuration
├── apps/                       # Spark applications
│   ├── pi_calculation.py       # Sample: Monte Carlo Pi calculation
│   └── wordcount.py           # Sample: Word counting
├── conf/                       # Spark configuration files
│   ├── spark-defaults.conf     # Main Spark configuration
│   ├── spark-defaults.client.conf  # Client-specific settings
│   ├── log4j2.properties      # Logging configuration
│   └── metrics.properties     # Metrics configuration
├── data/                       # Input/output data (mounted in containers)
├── events/                     # Spark event logs (for History Server)
├── notebooks/                  # Jupyter notebooks
│   └── demo.ipynb             # Sample notebook
└── jupyter/                    # Jupyter workspace persistence
```

## Monitoring and Debugging

### Service Health

```bash
# Check service health
docker-compose ps

# View service logs
docker-compose logs spark-master
docker-compose logs spark-worker
docker-compose logs jupyter-lab
```

### Spark Application Monitoring

1. **Live Applications**: http://localhost:8080
2. **Completed Applications**: http://localhost:18080
3. **Application Details**: Click on application IDs in the web UIs

### Common Issues

**Services not starting:**
```bash
# Check ports are available
netstat -tulpn | grep -E "(8080|8888|18080|7077)"

# Restart services
docker-compose down && docker-compose up -d
```

**Worker nodes not joining:**
```bash
# Check worker logs
docker-compose logs spark-worker

# Verify network connectivity
docker-compose exec spark-worker ping spark-master
```

## Performance Tuning

### Resource Allocation

```bash
# High-memory setup
SPARK_WORKER_MEMORY=8G
SPARK_WORKER_CORES=4
SPARK_EXECUTOR_MEMORY=4g
SPARK_DRIVER_MEMORY=2g

# Many small tasks
SPARK_WORKER_REPLICAS=4
SPARK_EXECUTOR_CORES=1
```

### Spark Configuration

```properties
# Adaptive query execution
spark.sql.adaptive.enabled=true
spark.sql.adaptive.coalescePartitions.enabled=true

# Memory management
spark.executor.memoryFraction=0.8
spark.sql.adaptive.shuffle.targetPostShuffleInputSize=64MB
```

## Development Workflow

1. **Development**: Use JupyterLab for interactive development
2. **Testing**: Run applications via `spark-submit` 
3. **Monitoring**: Check progress in Spark UI (port 8080)
4. **Analysis**: Review completed jobs in History Server (port 18080)
5. **Data**: Access shared data through `/data` volume

## Cleanup

```bash
# Stop services
docker-compose down

# Remove volumes (deletes data!)
docker-compose down -v

# Remove images
docker-compose down --rmi all
```

## Contributing

1. Fork the repository
2. Create a feature branch
3. Add your improvements
4. Test with the sample applications
5. Submit a pull request

## License

This project is open source and available under the [MIT License](LICENSE).

## Support

For issues and questions:
- Check the [troubleshooting section](#monitoring-and-debugging)
- Review Docker and Spark logs
- Create an issue in the repository

---

**Happy Spark Development! 🚀**
