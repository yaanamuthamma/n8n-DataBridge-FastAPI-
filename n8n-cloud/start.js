// Render sets PORT env var. n8n reads N8N_PORT.
// This script bridges the two, then starts n8n.
const { execSync } = require("child_process");

const port = process.env.PORT || "5678";
process.env.N8N_PORT = port;

console.log(`Starting n8n on port ${port}...`);

const { spawn } = require("child_process");
const child = spawn("n8n", ["start"], { stdio: "inherit", env: process.env });
child.on("exit", (code) => process.exit(code));
