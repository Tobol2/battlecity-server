const WebSocket = require('ws');

const PORT = process.env.PORT || 8080;
const wss = new WebSocket.Server({ port: PORT });

const rooms = new Map();
const playerRoom = new Map();
const connPlayer = new Map();

function log(msg) {
    console.log(`[${new Date().toISOString()}] ${msg}`);
}

function send(ws, data) {
    if (ws.readyState === WebSocket.OPEN) {
        ws.send(JSON.stringify(data));
    }
}

function broadcastRoom(room, data, excludeId = null) {
    const json = JSON.stringify(data);
    for (const [pid, player] of room.players) {
        if (pid !== excludeId && player.ws.readyState === WebSocket.OPEN) {
            player.ws.send(json);
            log(`Broadcasted to ${pid}: ${data.type || data.event || 'unknown'}`);
        }
    }
}

wss.on('connection', (ws) => {
    log('Client connected');

    ws.on('message', (raw) => {
        let data;
        try {
            data = JSON.parse(raw);
            log(`RECEIVED: ${JSON.stringify(data)}`);
        } catch (e) {
            log(`Failed to parse: ${raw}`);
            return;
        }

        const playerId = connPlayer.get(ws);

        switch (data.type) {
            case 'join': {
                const pid = data.playerId || ('p_' + Date.now());
                connPlayer.set(ws, pid);
                
                // Find or create room
                let room = null;
                for (const [id, r] of rooms) {
                    if (r.players.size < 2 && r.state === 'waiting') {
                        room = r;
                        break;
                    }
                }
                if (!room) {
                    const id = 'room_' + Date.now();
                    room = { id, state: 'waiting', players: new Map() };
                    rooms.set(id, room);
                    log(`New room: ${id}`);
                }
                
                const slot = room.players.size + 1;
                room.players.set(pid, { ws, id: pid, slot });
                playerRoom.set(pid, room.id);
                
                send(ws, { type: 'joined', roomId: room.id, playerId: pid, playersCount: room.players.size });
                log(`Player ${pid} slot=${slot} joined room ${room.id}`);
                
                if (room.players.size === 2) {
                    room.state = 'playing';
                    const slots = {};
                    for (const [id, p] of room.players) slots[id] = p.slot;
                    broadcastRoom(room, { type: 'game_start', room: room.id, slots });
                    log(`Room ${room.id} started!`);
                }
                break;
            }
            
            case 'state': {
                if (!playerId) break;
                const room = rooms.get(playerRoom.get(playerId));
                if (!room) break;
                const player = room.players.get(playerId);
                if (!player) break;
                broadcastRoom(room, {
                    type: 'player_update',
                    playerId,
                    slot: player.slot,
                    x: data.x,
                    y: data.y,
                    direction: data.direction,
                    alive: data.alive
                }, playerId);
                break;
            }
            
            case 'event': {
                if (!playerId) break;
                const room = rooms.get(playerRoom.get(playerId));
                if (!room) break;
                const player = room.players.get(playerId);
                log(`EVENT from slot ${player?.slot}: ${data.event}`);
                
                // Forward to all other players
                broadcastRoom(room, {
                    type: 'event',
                    event: data.event,
                    ...data
                }, playerId);
                break;
            }
            
            case 'ping':
                send(ws, { type: 'pong', ts: data.ts });
                break;
                
            case 'leave':
                if (playerId) {
                    const roomId = playerRoom.get(playerId);
                    if (roomId) {
                        const room = rooms.get(roomId);
                        if (room) {
                            broadcastRoom(room, { type: 'player_left', playerId }, playerId);
                            room.players.delete(playerId);
                            playerRoom.delete(playerId);
                            if (room.players.size === 0) {
                                rooms.delete(roomId);
                                log(`Room ${roomId} closed`);
                            }
                        }
                    }
                    connPlayer.delete(ws);
                }
                break;
                
            default:
                log(`Unknown message type: ${data.type}`);
        }
    });

    ws.on('close', () => {
        const pid = connPlayer.get(ws);
        if (pid) {
            const roomId = playerRoom.get(pid);
            if (roomId) {
                const room = rooms.get(roomId);
                if (room) {
                    broadcastRoom(room, { type: 'player_left', playerId: pid }, pid);
                    room.players.delete(pid);
                    playerRoom.delete(pid);
                    if (room.players.size === 0) {
                        rooms.delete(roomId);
                        log(`Room ${roomId} closed`);
                    }
                }
            }
        }
        connPlayer.delete(ws);
        log('Client disconnected');
    });
});

log(`Battle City WebSocket Server running on port ${PORT}`);
