FROM n8nio/n8n:latest

# Render sets PORT dynamically, n8n needs to listen on it
ENV N8N_PORT=${PORT:-5678}
ENV N8N_PROTOCOL=https
ENV GENERIC_TIMEZONE=Asia/Kolkata
ENV N8N_LOG_LEVEL=info

# Use webhook URL from environment (set in Render)
ENV WEBHOOK_URL=${WEBHOOK_URL}

# Basic auth
ENV N8N_BASIC_AUTH_ACTIVE=true
ENV N8N_BASIC_AUTH_USER=${N8N_USER:-admin}
ENV N8N_BASIC_AUTH_PASSWORD=${N8N_PASSWORD:-admin123}

# PostgreSQL for persistent storage (set DB_* env vars in Render)
ENV DB_TYPE=postgresdb
ENV DB_POSTGRESDB_HOST=${DB_HOST}
ENV DB_POSTGRESDB_PORT=${DB_PORT:-5432}
ENV DB_POSTGRESDB_DATABASE=${DB_NAME}
ENV DB_POSTGRESDB_USER=${DB_USER}
ENV DB_POSTGRESDB_PASSWORD=${DB_PASSWORD}

EXPOSE ${N8N_PORT}

# Start n8n on the PORT that Render assigns
CMD ["sh", "-c", "n8n start --tunnel"]
