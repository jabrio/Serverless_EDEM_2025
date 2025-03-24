FROM grafana/grafana:latest

RUN grafana-cli plugins install grafana-dynamodb-datasource

# Add Config
ENV GF_SECURITY_ADMIN_USER=jbg922
ENV GF_SECURITY_ADMIN_PASSWORD=Edem_2025

# Expose Port
EXPOSE 3000