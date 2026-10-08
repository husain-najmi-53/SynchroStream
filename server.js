// server.js
const express = require('express');
const http = require('http');
const { Server } = require('socket.io');
const path = require('path');

const app = express();
const server = http.createServer(app);

const io = new Server(server, {
  cors: {
    origin: '*',
    methods: ['GET', 'POST'],
  },
});

const PORT = process.env.PORT || 3000;
app.use(express.static(path.join(__dirname)));

const activeSessions = new Map();

function normalizeSessionId(sessionId) {
  if (typeof sessionId !== 'string') return null;
  const normalized = sessionId.trim();
  if (!normalized || normalized.length > 64) return null;
  return normalized;
}

function getSession(sessionId) {
  return activeSessions.get(sessionId);
}

function removeSocketFromSession(socket) {
  const sessionId = socket.data.sessionId;
  if (!sessionId) return;

  const session = getSession(sessionId);
  if (!session) {
    socket.data.sessionId = null;
    socket.data.role = null;
    return;
  }

  if (session.host === socket.id) {
    io.to(sessionId).emit('session-ended', {
      message: 'The host has left the session.',
    });
    activeSessions.delete(sessionId);
  } else if (session.guest === socket.id) {
    session.guest = null;
    socket.to(sessionId).emit('peer-left');
  }

  socket.leave(sessionId);
  socket.data.sessionId = null;
  socket.data.role = null;

  if (activeSessions.has(sessionId)) {
    const remaining = io.sockets.adapter.rooms.get(sessionId);
    if (!remaining || remaining.size === 0) {
      activeSessions.delete(sessionId);
    }
  }
}

io.on('connection', (socket) => {
  console.log('socket connected', socket.id);

  socket.on('join-session', ({ sessionId } = {}) => {
    const normalizedId = normalizeSessionId(sessionId);

    if (!normalizedId) {
      socket.emit('session-error', { message: 'Invalid session ID.' });
      return;
    }

    if (socket.data.sessionId) {
      removeSocketFromSession(socket);
    }

    let session = getSession(normalizedId);

    if (!session) {
      session = {
        host: socket.id,
        guest: null,
      };
      activeSessions.set(normalizedId, session);
    } else if (session.host === socket.id || session.guest === socket.id) {
      socket.emit('role', {
        role: session.host === socket.id ? 'host' : 'guest',
      });
      return;
    } else if (!session.guest) {
      session.guest = socket.id;
    } else {
      socket.emit('session-full');
      return;
    }

    socket.join(normalizedId);
    socket.data.sessionId = normalizedId;
    socket.data.role = session.host === socket.id ? 'host' : 'guest';

    socket.emit('role', { role: socket.data.role });

    if (socket.data.role === 'guest') {
      socket.to(normalizedId).emit('peer-ready');
    }

    console.log(
      `${socket.id} joined session ${normalizedId} as ${socket.data.role}`
    );
  });

  socket.on('webrtc-signal', (data = {}) => {
    const sessionId = normalizeSessionId(data.sessionId);
    if (!sessionId || socket.data.sessionId !== sessionId) return;

    socket.to(sessionId).emit('webrtc-signal', data);
  });

  socket.on('control', (data = {}) => {
    const sessionId = normalizeSessionId(data.sessionId);
    if (!sessionId || socket.data.sessionId !== sessionId) return;

    socket.to(sessionId).emit('control', data);
  });

  socket.on('chat-message', (data = {}) => {
    const sessionId = normalizeSessionId(data.sessionId);
    if (!sessionId || socket.data.sessionId !== sessionId) return;

    io.to(sessionId).emit('chat-message', {
      sessionId,
      text: typeof data.text === 'string' ? data.text.slice(0, 1000) : '',
      sender: socket.data.role === 'host' ? 'Host' : 'Guest',
    });
  });

  socket.on('leave-session', () => {
    removeSocketFromSession(socket);
  });

  socket.on('disconnect', () => {
    console.log('socket disconnected', socket.id);
    removeSocketFromSession(socket);
  });
});

server.listen(PORT, () => {
  console.log(`Server is running on port ${PORT}`);
});
