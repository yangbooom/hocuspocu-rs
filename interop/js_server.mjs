// Reference upstream server for head-to-head benchmarking.
import { Server } from "@hocuspocus/server";
const port = parseInt(process.env.HP_PORT || "8089", 10);
const server = new Server({ port, quiet: true });
await server.listen(port);
console.log("js @hocuspocus/server listening on ws://127.0.0.1:" + port);
