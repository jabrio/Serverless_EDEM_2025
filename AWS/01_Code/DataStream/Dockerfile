FROM grafana/grafana:latest

RUN grafana-cli plugins install grafana-dynamodb-datasource

# Add Config
ENV GF_SECURITY_ADMIN_USER=admin
ENV GF_SECURITY_ADMIN_PASSWORD=edem_2025

# Expose Port
EXPOSE 3000
