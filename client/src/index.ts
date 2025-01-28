import WebSocket from "ws";
import http from "http";
import { Command } from "commander";
import { createWriteStream } from "fs";

const program = new Command();
program.requiredOption("-p, --port <number>", "Local port").parse();

const { port } = program.opts();
const ws = new WebSocket(`wss://tunnel.cyno.one/_ws/tunnel?port=${port}`);

let fileWriter = createWriteStream(`received_${Date.now()}.bin`);

ws.on("message", (data: Buffer) => {
  if (data instanceof Buffer) {
    fileWriter.write(data);
  } else {
    const message = JSON.parse(data.toString());
    if (message.type === "TUNNEL_READY") {
      console.log(`Tunnel ready: ${message.data.publicUrl}`);
    }
  }
});

http
  .createServer((req, res) => {
    req.pipe(
      http.request(
        {
          host: "localhost",
          port: port,
          path: req.url,
          method: req.method,
          headers: req.headers,
        },
        (proxyRes) => {
          res.writeHead(proxyRes.statusCode || 500, proxyRes.headers);
          proxyRes.pipe(res);
        }
      )
    );
  })
  .listen(port);
