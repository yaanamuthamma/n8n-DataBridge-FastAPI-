FROM n8nio/n8n:latest

ENV N8N_PROTOCOL=https
ENV GENERIC_TIMEZONE=Asia/Kolkata
ENV N8N_LOG_LEVEL=info
ENV N8N_DIAGNOSTICS_ENABLED=false
ENV DB_TYPE=postgresdb

# Copy startup script
COPY n8n-cloud/start.js /start.js

ENTRYPOINT ["node", "/start.js"]
