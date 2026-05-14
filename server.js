const WebSocket = require('ws');

const PORT = process.env.PORT || 8080;
const wss  = new WebSocket.Server({ port: PORT });

const rooms      = new Map();
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

function findOrCreateRoom() {
    for (const [id, room] of rooms) {
        if (room.players.size < 2 && room.state === 'waiting') return room;
    }
    const id   = 'room_' + Date.now();
    const room = { id, state: 'waiting', players: new Map() };
    rooms.set(id, room);
    log(`New room: ${id}`);
    return room;
}

function broadcastRoom(room, data, excludeId = null) {
    const json = JSON.stringify(data);
    for (const [pid, player] of room.players) {
        if (pid !== excludeId && player.ws.readyState === WebSocket.OPEN) {
            player.ws.send(json);
        }
    }
}

function handleLeave(playerId) {
    const roomId = playerRoom.get(playerId);
    if (!roomId) return;
    const room = rooms.get(roomId);
    if (!room) return;

    broadcastRoom(room, { type: 'player_left', playerId });
    room.players.delete(playerId);
    playerRoom.delete(playerId);
    log(`Player ${playerId} left room ${roomId} (${room.players.size} left)`);

    if (room.players.size === 0) {
        rooms.delete(roomId);
        log(`Room ${roomId} closed`);
    }
}

wss.on('connection', (ws) => {
    log('New connection');

    ws.on('message', (raw) => {
        let data;
        try { data = JSON.parse(raw); } catch { return; }

        const playerId = connPlayer.get(ws);

        switch (data.type) {

            case 'join': {
                const pid  = data.playerId || ('p_' + Date.now());
                connPlayer.set(ws, pid);

                const room = findOrCreateRoom();
                const slot = room.players.size + 1;
                room.players.set(pid, { ws, id: pid, slot, x: 0, y: 0, direction: 'UP', alive: true });
                playerRoom.set(pid, room.id);

                send(ws, {
                    type:         'joined',
                    roomId:       room.id,
                    playerId:     pid,
                    playersCount: room.players.size,
                });
                log(`Player ${pid} joined room ${room.id} (${room.players.size}/2)`);

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
                const roomId = playerRoom.get(playerId);
                const room   = rooms.get(roomId);
                if (!room) break;
                const player = room.players.get(playerId);
                if (!player) break;
                if (data.x         !== undefined) player.x         = data.x;
                if (data.y         !== undefined) player.y         = data.y;
                if (data.direction !== undefined) player.direction = data.direction;
                if (data.alive     !== undefined) player.alive     = data.alive;
                broadcastRoom(room, {
                    type:      'player_update',
                    playerId,
                    slot:      player.slot,
                    x:         player.x,
                    y:         player.y,
                    direction: player.direction,
                    alive:     player.alive,
                }, playerId);
                break;
            }

            case 'event': {
                if (!playerId) break;
                const roomId = playerRoom.get(playerId);
                const room   = rooms.get(roomId);
                if (!room) break;
                const player = room.players.get(playerId);
                broadcastRoom(room, {
                    ...data,
                    fromPlayer: playerId,
                    fromSlot:   player?.slot || 0,
                }, playerId);
                break;
            }

            case 'ping':
                send(ws, { type: 'pong', ts: data.ts });
                break;

            case 'leave':
                if (playerId) handleLeave(playerId);
                break;
        }
    });

    ws.on('close', () => {
        const pid = connPlayer.get(ws);
        if (pid) handleLeave(pid);
        connPlayer.delete(ws);
        log('Connection closed');
    });

    ws.on('error', (err) => {
        log(`Error: ${err.message}`);
    });
});

log(`Battle City WebSocket Server running on port ${PORT}`);
