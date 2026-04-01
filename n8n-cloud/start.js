// Map Render env vars to n8n's expected env var names, then start n8n.
const { spawn } = require("child_process");

// Render assigns PORT dynamically
process.env.N8N_PORT = process.env.PORT || "5678";

// Map our DB_* vars to n8n's DB_POSTGRESDB_* vars
process.env.DB_POSTGRESDB_HOST = process.env.DB_HOST || "localhost";
process.env.DB_POSTGRESDB_PORT = process.env.DB_PORT || "5432";
process.env.DB_POSTGRESDB_DATABASE = process.env.DB_NAME || "n8n";
process.env.DB_POSTGRESDB_USER = process.env.DB_USER || "n8n_user";
process.env.DB_POSTGRESDB_PASSWORD = process.env.DB_PASSWORD || "";

// Map auth vars
process.env.N8N_BASIC_AUTH_USER = process.env.N8N_USER || "admin";
process.env.N8N_BASIC_AUTH_PASSWORD = process.env.N8N_PASSWORD || "admin123";

console.log(`Starting n8n on port ${process.env.N8N_PORT}...`);
console.log(`DB host: ${process.env.DB_POSTGRESDB_HOST}`);

const child = spawn("n8n", ["start"], { stdio: "inherit", env: process.env });
child.on("exit", (code) => process.exit(code));
