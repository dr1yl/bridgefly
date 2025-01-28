import fastify, { FastifyInstance } from "fastify";
import websocketServer from "@fastify/websocket";
import httpProxy from "@fastify/http-proxy";
import { pipeline } from "stream/promises";
import { v4 as uuidv4 } from "uuid";
import { chunkStream, processChunk } from "./utils/stream";
import { Readable, Writable } from "stream";

const server = fastify({
  logger: true,
  trustProxy: true,
  bodyLimit: 1024 * 1024 * 100, // 100MB
});

interface Tunnel {
  id: string;
  publicUrl: string;
  localUrl: string;
  ws: WebSocket;
}

const activeTunnels = new Map<string, Tunnel>();

async function tunnelService(fastify: FastifyInstance) {
  await fastify.register(websocketServer, {
    options: {
      maxPayload: 1024 * 1024 * 50, // 50MB
    },
  });

  const DOMAIN = process.env.DOMAIN || "tunnel.yourdomain.com";

  const generatePublicUrl = (tunnelId: string) =>
    `https://${tunnelId}.${DOMAIN}`;

  fastify.get(
    "/_ws/tunnel",
    {
      websocket: true,
    },
    (connection, req) => {
      const tunnelId = uuidv4();
      const publicUrl = generatePublicUrl(tunnelId);

      const tunnel: Tunnel = {
        id: tunnelId,
        publicUrl,
        //@ts-ignore
        localUrl: `http://localhost:${req.query.port}`,
        //@ts-ignore
        ws: connection,
      };

      activeTunnels.set(tunnelId, tunnel);

      // Обработка бинарных данных
      connection.on("message", (data: Buffer) => {
        handleIncomingData(tunnelId, data);
      });

      connection.send(
        JSON.stringify({
          type: "TUNNEL_READY",
          data: { publicUrl },
        })
      );

      connection.on("close", () => {
        activeTunnels.delete(tunnelId);
      });
    }
  );

  fastify.register(httpProxy, {
    upstream: "http://localhost:3000",
    websocket: true,
    replyOptions: {
      onResponse: (request, reply, res) => {
        // Потоковая передача больших файлов
        reply.send(res);
      },
    },
  });
}

async function handleIncomingData(tunnelId: string, data: Buffer) {
  const tunnel = activeTunnels.get(tunnelId);
  if (!tunnel) return;

  await pipeline(
    // 1. Создаем Readable поток из Buffer
    Readable.from(data),

    // 2. Разбиваем на чанки по 1MB (передаем только размер)
    chunkStream(1024 * 1024),

    // 3. Обрабатываем каждый чанк
    async function* (source) {
      for await (const chunk of source) {
        yield processChunk(chunk);
      }
    },

    // 4. Отправляем обратно клиенту через WebSocket
    new Writable({
      write(chunk, _enc, callback) {
        if (tunnel.ws.readyState === tunnel.ws.OPEN) {
          tunnel.ws.send(chunk);
        }
        callback();
      },
    })
  );
}

const start = async () => {
  try {
    await server.register(tunnelService);
    await server.listen({ port: 3001, host: "0.0.0.0" });
  } catch (err) {
    server.log.error(err);
    process.exit(1);
  }
};

start();
