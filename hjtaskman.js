// hjtaskman.js
// Durable Object for "h" version task manager (HjTaskman).
// - Exposes HTTP endpoints: /hsubmit, /hgivemeatask, /htaskreturn, /htaskstate, /htakeandover, /htotalstate, /hrequestpatrol, /hfreeoverslots
// - Exposes WebSocket endpoint: /hws  (runner and client bind here)
// - Persists raw logs to KV (H_TASKLOGS_KV or TASKLOGS_KV) in chunked keys: h:tasklogs:<taskId>:<timestamp>
// - Uses hj_taskpool and hj_lastPatrolTime storage keys (separate from gjtaskman)
// Required env bindings (examples):
//   - H_TASKLOGS_KV (KV namespace)  -- recommended for H-version logs
//   - TASKLOGS_KV (KV namespace)    -- fallback if H_TASKLOGS_KV not set
//   - USERVIDEO_KV (KV namespace)   -- used by _persistFinalResult (shared or separate)
//   - DB (D1 database binding)      -- D1 for UserVideoState and DownloadedFiles
//   - LOG_FLUSH_SIZE, LOG_TRUNCATE_LEN optional env vars
//
// Note: runner authentication is out-of-scope per your spec (runner actively grabs tasks).
//       For frontend WS auth, issue short-lived token via a Pages Function or let DO validate JWT if you add JWT_SECRET.

const POOL_SIZE = 50;
const PATROL_INTERVAL_MS = 15000; // 15 seconds

const DEFAULT_LOG_FLUSH_SIZE = 50;
const DEFAULT_LOG_TRUNCATE_LEN = 1024;

export class HjTaskman {
  constructor(state, env) {
    this.state = state;
    this.env = env;

    // persisted core state
    this.taskpool = [];
    this.lastPatrolTime = 0;

    // ephemeral runtime state
    this.runnerSockets = new Map(); // taskId -> server-side WebSocket
    this.clientSockets = new Map(); // taskId -> server-side WebSocket (single client per task)
    this.logBuffers = new Map();    // taskId -> Array<{ts,line}>

    // config
    this.LOG_FLUSH_SIZE = Number(env.LOG_FLUSH_SIZE) || DEFAULT_LOG_FLUSH_SIZE;
    this.LOG_TRUNCATE_LEN = Number(env.LOG_TRUNCATE_LEN) || DEFAULT_LOG_TRUNCATE_LEN;
    this.TASKLOGS_KV = env.TASKLOGS_KV || null;
    this.USERVIDEO_KV = env.USERVIDEO_KV || null;
    this.DB = env.DB || null;

    // init
    this.initializePromise = this.initialize();
  }

  async initialize() {
    let pool = await this.state.storage.get("hj_taskpool");
    if (pool === undefined || pool === null) {
      pool = Array.from({ length: POOL_SIZE }, (_, i) => this._createFreeSlot(i));
      await this.state.storage.put("hj_taskpool", pool);
    }
    this.taskpool = pool;
    this.lastPatrolTime = await this.state.storage.get("hj_lastPatrolTime") || 0;
  }

  async ensureInitialized() {
    await this.initializePromise;
  }

  _createFreeSlot(id) {
    return { slotId: id, isFree: true, taskId: null, state: null };
  }

  _findSlotIndexByTaskId(taskid) {
    return this.taskpool.findIndex(s => s.taskId === taskid);
  }

  // Basic redaction/filter for client-forwarded logs
  _filterLogLine(line) {
    if (!line || typeof line !== 'string') return '';
    let filtered = line.replace(/([A-Za-z]:)?(\/|\\)[\w\-./\\]+/g, '<REDACTED_PATH>');
    filtered = filtered.replace(/(\?.*?)(?=\s|$)/g, '?<REDACTED>');
    if (filtered.length > this.LOG_TRUNCATE_LEN) {
      filtered = filtered.slice(0, this.LOG_TRUNCATE_LEN) + ' ... (truncated)';
    }
    return filtered;
  }

  // Persist a chunk of raw logs to KV (chunk keyed by timestamp)
  async _flushLogBufferToKV(taskId, buffer) {
    if (!this.TASKLOGS_KV) return;
    if (!buffer || buffer.length === 0) return;
    try {

 

      
      const chunkKey = `h:tasklogs:${taskId}:${Date.now()}`;
      await this.TASKLOGS_KV.put(chunkKey, JSON.stringify(buffer));
      const metaKey = `h:tasklogs:${taskId}:meta`;
      let meta = await this.TASKLOGS_KV.get(metaKey, { type: 'json' });
      if (!meta) meta = { chunks: [] };
      meta.chunks.push(chunkKey);
      await this.TASKLOGS_KV.put(metaKey, JSON.stringify(meta));
    } catch (e) {
      console.error(`HjTaskman: Failed to flush logs for ${taskId} to KV:`, e);
    }
  }

  // Append raw log to in-memory buffer and flush if threshold reached
  async _appendRawLog(taskId, rawLine) {
    if (!rawLine) return;
    let buf = this.logBuffers.get(taskId);
    if (!buf) {
      buf = [];
      this.logBuffers.set(taskId, buf);
    }
    const storedLine = typeof rawLine === 'string' ? rawLine.slice(0, this.LOG_TRUNCATE_LEN) : String(rawLine);
    buf.push({ ts: Date.now(), line: storedLine });
    if (buf.length >= this.LOG_FLUSH_SIZE) {
      const toFlush = buf.splice(0, buf.length);
      this._flushLogBufferToKV(taskId, toFlush).catch(err => console.error('HjTaskman flushLog err', err));
    }
  }

  // Send JSON message to bound client for a task (single client model)
  _broadcastToClient(taskId, msgObj) {
    const client = this.clientSockets.get(taskId);
    if (!client) return;
    try {
      client.send(JSON.stringify(msgObj));
    } catch (e) {
      console.warn(`HjTaskman: failed send to client for ${taskId}`, e);
      try { client.close(); } catch(_) {}
      this.clientSockets.delete(taskId);
    }
  }

  // Idempotent final result handling (runner sends via WS or HTTP /htaskreturn)
  async _handleRunnerResult(taskId, resultObj) {
    const slotIndex = this._findSlotIndexByTaskId(taskId);
    if (slotIndex === -1) {
      console.warn(`HjTaskman: Runner result for unknown task ${taskId}`);
      return;
    }
    const slot = this.taskpool[slotIndex];
    if (slot.result && slot.result._persisted) {
      // broadcast summary to client anyway
      this._broadcastToClient(taskId, { from: 'do', action: 'result', taskId, success: !!resultObj.success });
      return;
    }

    slot.state = 'finished';
    slot.completionTime = Date.now();
    slot.result = resultObj;

    if (resultObj.success && resultObj.resultjson) {
      try {
        await this._persistFinalResult(slot, resultObj.resultjson);
        slot.result._persisted = true;
      } catch (err) {
        console.error(`HjTaskman: CRITICAL PERSISTENCE FAILURE for ${taskId}:`, err);
        slot.result.persistenceError = err.message || String(err);
      }
    }

    await this.state.storage.put("hj_taskpool", this.taskpool);

    const summary = (resultObj.success && resultObj.resultjson) ? { type: slot.type, videoId: slot.videoId } : null;
    this._broadcastToClient(taskId, { from: 'do', action: 'result', taskId, success: !!resultObj.success, summary, detailAvailable: true });
  }

  // WebSocket upgrade handler for /hws
  async _handleWebSocketUpgrade(request) {
    const pair = new WebSocketPair();
    const [clientSide, serverSide] = Object.values(pair);
    serverSide.accept();

    serverSide.addEventListener('message', async (evt) => {
      try {
        const dataRaw = typeof evt.data === 'string' ? evt.data : new TextDecoder().decode(evt.data);
        const msg = JSON.parse(dataRaw);
        const action = msg.action;
        if (!action) return;

        if (action === 'bind') {
          const role = msg.role;
          const taskId = msg.taskId;
          if (!taskId) {
            serverSide.send(JSON.stringify({ action: 'bind_ack', success: false, reason: 'missing taskId' }));
            return;
          }
          if (role === 'runner') {
            this.runnerSockets.set(taskId, serverSide);
            const idx = this._findSlotIndexByTaskId(taskId);
            if (idx !== -1) {
              const slot = this.taskpool[idx];
              if (slot.state === 'waiting' || slot.state === null) {
                slot.state = 'running';
                slot.startTime = slot.startTime || Date.now();
                await this.state.storage.put("hj_taskpool", this.taskpool);
              }
            }
            serverSide.send(JSON.stringify({ action: 'bind_ack', success: true, role: 'runner', taskId }));
            return;
          } else if (role === 'client') {
            const existing = this.clientSockets.get(taskId);
            if (existing && existing !== serverSide) {
              try { existing.close(); } catch (e) {}
            }
            this.clientSockets.set(taskId, serverSide);
            const buf = this.logBuffers.get(taskId) || [];
            const preview = buf.slice(-20).map(e => ({ ts: e.ts, short: this._filterLogLine(e.line) }));
            serverSide.send(JSON.stringify({ action: 'bind_ack', success: true, role: 'client', taskId, preview }));
            return;
          } else {
            serverSide.send(JSON.stringify({ action: 'bind_ack', success: false, reason: 'invalid role' }));
            return;
          }
        }

        // non-bind messages require taskId
        const taskId = msg.taskId;
        if (!taskId) return;

        switch (action) {
          case 'raw_log': {
            // { action:'raw_log', taskId, line }
            await this._appendRawLog(taskId, msg.line);
            const filtered = this._filterLogLine(msg.line);
            this._broadcastToClient(taskId, { from: 'do', action: 'log', taskId, level: 'info', short: filtered, ts: msg.ts || Date.now() });
            break;
          }
          case 'log': {
            // { action:'log', taskId, level, msg, raw? }
            if (msg.raw) await this._appendRawLog(taskId, msg.raw);
            const short = this._filterLogLine(msg.msg || msg.raw || '');
            this._broadcastToClient(taskId, { from: 'do', action: 'log', taskId, level: msg.level || 'info', short, ts: msg.ts || Date.now() });
            break;
          }
          case 'status': {
            // { action:'status', taskId, status, progress, meta }
            const idx = this._findSlotIndexByTaskId(taskId);
            if (idx !== -1) {
              const slot = this.taskpool[idx];
              slot.progress = msg.progress ?? slot.progress;
              slot.statusNote = msg.meta ?? slot.statusNote;
            }
            this._broadcastToClient(taskId, { from: 'do', action: 'progress', taskId, status: msg.status, percent: msg.progress || 0, meta: msg.meta || null });
            break;
          }
          case 'result': {
            // { action:'result', taskId, result: {...} }
            await this._handleRunnerResult(taskId, msg.result || {});
            break;
          }
          case 'ping': {
            serverSide.send(JSON.stringify({ action: 'pong', ts: Date.now() }));
            break;
          }
          default:
            console.warn('HjTaskman WS unknown action:', action);
        }
      } catch (err) {
        console.error('HjTaskman WS message handler error:', err);
      }
    });

    serverSide.addEventListener('close', (evt) => {
      for (const [taskId, ws] of this.runnerSockets.entries()) {
        if (ws === serverSide) {
          this.runnerSockets.delete(taskId);
        }
      }
      for (const [taskId, ws] of this.clientSockets.entries()) {
        if (ws === serverSide) {
          this.clientSockets.delete(taskId);
        }
      }
    });

    serverSide.addEventListener('error', (evt) => {
      console.warn('HjTaskman WS error', evt);
    });

    // Return the client-side socket as the upgrade response
    return new Response(null, { status: 101, webSocket: clientSide });
  }

  // main fetch
  async fetch(request) {
    await this.ensureInitialized();
    const url = new URL(request.url);

    // WebSocket upgrade (hws)
    if (url.pathname === '/hws' && request.headers.get('upgrade') && request.headers.get('upgrade').toLowerCase() === 'websocket') {
      return await this._handleWebSocketUpgrade(request);
    }

    // HTTP endpoints (h-prefixed to avoid collision)
    switch (url.pathname) {
      case "/hsubmit": return this.submit(request);
      case "/hgivemeatask": return this.givemeatask(request);
      case "/htaskreturn": return this.taskreturn(request);
      case "/htaskstate": return this.taskstate(request);
      case "/htakeandover": return this.takeandover(request);
      case "/htotalstate": return this.totalstate(request);
      case "/hrequestpatrol": return this.requestPatrol(request);
      case "/hfreeoverslots": return this.freeOverSlots(request);
      default: return new Response("Not Found in HjTaskman DO", { status: 404 });
    }
  }

  /**
   * hsubmit: similar to gj submit but uses hj_taskpool
   * expects X-User-Context header (injected by middleware)
   */
  async submit(request) {
    const userContextHeader = request.headers.get('X-User-Context');
    const user = userContextHeader ? JSON.parse(userContextHeader) : null;
    if (!user) return new Response(JSON.stringify({ error: "Unauthorized" }), { status: 401 });

    const { videoid, taskcmd, type } = await request.json();

    // instant clone route
    if (taskcmd && taskcmd.startsWith('l@')) {
      return this.handleInstantCloneTask(user, videoid, taskcmd);
    }

    const freeSlotIndex = this.taskpool.findIndex(s => s.isFree);
    if (freeSlotIndex === -1) {
      return new Response(JSON.stringify({ error: "System busy" }), { status: 429 });
    }

    const taskId = `t_${crypto.randomUUID()}`;
    this.taskpool[freeSlotIndex] = {
      slotId: freeSlotIndex,
      isFree: false,
      taskId,
      userId: user.id,
      videoId: videoid,
      taskcmd,
      type,
      state: "waiting",
      submitTime: Date.now(),
      result: null,
    };

    await this.state.storage.put("hj_taskpool", this.taskpool);
    return new Response(JSON.stringify({ id: taskId }), { status: 202 });
  }

  // handle instant clone (same semantics as gj clone but using hj keys)
  async handleInstantCloneTask(user, videoId, taskcmd) {
    const freeSlotIndex = this.taskpool.findIndex(s => s.isFree);
    if (freeSlotIndex === -1) {
      return new Response(JSON.stringify({ error: "System is busy, cannot store clone result." }), { status: 429 });
    }

    const taskId = `t_${crypto.randomUUID()}`;
    let resultPayload = {};

    try {
      const originalUserId = taskcmd.split('@')[1];
      const originalVideoId = videoId;

      const targetKey = `user:${user.id}:${originalVideoId}`;
      const originalKey = `user:${originalUserId}:${originalVideoId}`;

      const originalDataStr = await this.USERVIDEO_KV.get(originalKey);
      if (!originalDataStr) throw new Error('Original project not found.');

      const clonedData = JSON.parse(originalDataStr);
      clonedData.isArchived = false;
      clonedData.category = null;

      await this.USERVIDEO_KV.put(targetKey, JSON.stringify(clonedData));

      const now = Date.now();
      if (!this.DB) throw new Error('D1 DB binding not configured for HjTaskman.');
      await this.DB.prepare(
        `INSERT INTO UserVideoState (userId, videoId, isArchived, lastInteractionAt) VALUES (?, ?, 0, ?)
         ON CONFLICT(userId, videoId) DO UPDATE SET lastInteractionAt = excluded.lastInteractionAt, isArchived = 0`
      ).bind(user.id, originalVideoId, now).run();

      resultPayload = {
        success: true,
        resultjson: {
          ...clonedData,
          result_type: 'clone'
        }
      };
    } catch (e) {
      console.error(`HjTaskman clone failed for user ${user.id}:`, e);
      resultPayload = { success: false, logs: [{ src: 'HjTaskman.Clone', type: 'ERROR', payload: e.message }] };
    }

    this.taskpool[freeSlotIndex] = {
      slotId: freeSlotIndex,
      isFree: false,
      taskId,
      userId: user.id,
      videoId: videoId,
      taskcmd,
      type: 'clone',
      state: "finished",
      submitTime: Date.now(),
      completionTime: Date.now(),
      result: resultPayload,
    };

    await this.state.storage.put("hj_taskpool", this.taskpool);
    return new Response(JSON.stringify({ id: taskId }), { status: 202 });
  }

  async givemeatask(request) {
    const waitingTasks = this.taskpool.filter(s => s.state === "waiting");
    if (waitingTasks.length === 0) {
      return new Response(JSON.stringify({ error: "No waiting tasks available" }), { status: 404 });
    }
    waitingTasks.sort((a, b) => a.submitTime - b.submitTime);
    const taskToRun = waitingTasks[0];

    taskToRun.state = "running";
    taskToRun.startTime = Date.now();
    await this.state.storage.put("hj_taskpool", this.taskpool);

    // hint runner to connect to /hws (same domain)
    return new Response(JSON.stringify({ id: taskToRun.taskId, taskcmd: taskToRun.taskcmd, wsPath: "/hws" }));
  }

  // runner HTTP fallback result reporting
  async taskreturn(request) {
    const { taskid, result } = await request.json();
    const slotIndex = this.taskpool.findIndex(s => s.taskId === taskid && s.state === 'running');
    if (slotIndex === -1) {
      const anyIndex = this.taskpool.findIndex(s => s.taskId === taskid);
      if (anyIndex === -1) return new Response(JSON.stringify({ error: "Task not found" }), { status: 404 });
      // continue if slot exists (may be finished already)
    }

    const slot = this.taskpool.find(s => s.taskId === taskid);
    if (!slot) return new Response(JSON.stringify({ error: "Task not found" }), { status: 404 });

    if (slot.result && slot.result._persisted) {
      return new Response(JSON.stringify({ success: true, note: 'already persisted' }));
    }

    slot.state = "finished";
    slot.completionTime = Date.now();
    slot.result = result;

    if (result.success && result.resultjson) {
      try {
        await this._persistFinalResult(slot, result.resultjson);
        slot.result._persisted = true;
      } catch (error) {
        console.error(`HjTaskman: Persistence failure for ${taskid}:`, error);
        slot.result.persistenceError = error.message;
      }
    }

    await this.state.storage.put("hj_taskpool", this.taskpool);
    this._broadcastToClient(taskid, { from: 'do', action: 'result', taskId: taskid, success: !!result.success });

    return new Response(JSON.stringify({ success: true }));
  }

  async taskstate(request) {
    const { taskid } = await request.json();
    if (!taskid) return new Response(JSON.stringify({ error: "taskid is required" }), { status: 400 });

    const slot = this.taskpool.find(s => s.taskId === taskid);
    if (!slot) return new Response(JSON.stringify({ error: "Task not found (may have been cleared)" }), { status: 404 });

    return new Response(JSON.stringify({ status: slot.state, result: slot.result }), { headers: { 'Content-Type': 'application/json' } });
  }

  async takeandover(request) {
    const { taskid } = await request.json();
    if (!taskid) return new Response(JSON.stringify({ error: "taskid is required" }), { status: 400 });

    const slotIndex = this.taskpool.findIndex(s => s.taskId === taskid && s.state === 'finished');
    if (slotIndex === -1) {
      return this.taskstate(request);
    }

    const slot = this.taskpool[slotIndex];
    slot.state = "over";
    await this.state.storage.put("hj_taskpool", this.taskpool);

    return new Response(JSON.stringify({ status: "finished", result: slot.result }), { headers: { 'Content-Type': 'application/json' } });
  }

  async totalstate() {
    const states = this.taskpool.reduce((acc, slot) => {
      if (slot.isFree) acc.free = (acc.free || 0) + 1;
      else acc[slot.state] = (acc[slot.state] || 0) + 1;
      return acc;
    }, {});
    return new Response(JSON.stringify({ poolSize: POOL_SIZE, ...states }), { headers: { 'Content-Type': 'application/json' } });
  }

  // Persist final user-visible data to USERVIDEO_KV and D1 (same schema as gj version)
  async _persistFinalResult(taskInfo, fullProfile) {
    const now = Date.now();
    if (!this.USERVIDEO_KV) throw new Error('USERVIDEO_KV not bound to HjTaskman env.');
    if (!this.DB) throw new Error('D1 DB not bound to HjTaskman env.');

    if (taskInfo.type === 'profile') {
      const snapshot = {
        title: fullProfile.video_info.title,
        channel: fullProfile.video_info.channel,
        thumbnail_url: fullProfile.video_info.thumbnail_url,
      };
      const userVideoData = {
        profileSnapshot: snapshot, isArchived: false, category: null, videoId: taskInfo.videoId,
        downloadedFiles: [], fullProfile: fullProfile,
      };

      await this.USERVIDEO_KV.put(`user:${taskInfo.userId}:${taskInfo.videoId}`, JSON.stringify(userVideoData));
      await this.DB.prepare(
        `INSERT INTO UserVideoState (userId, videoId, isArchived, lastInteractionAt) VALUES (?, ?, 0, ?)
         ON CONFLICT(userId, videoId) DO UPDATE SET lastInteractionAt = excluded.lastInteractionAt`
      ).bind(taskInfo.userId, taskInfo.videoId, now).run();
    } else if (taskInfo.type === 'download' || taskInfo.type === 'combine') {
      const fileInfo = fullProfile.file_info;
      const kvKey = `user:${taskInfo.userId}:${taskInfo.videoId}`;
      let existingData = await this.USERVIDEO_KV.get(kvKey, { type: 'json' });

      if (existingData) {
        if (!existingData.downloadedFiles) existingData.downloadedFiles = [];
        existingData.downloadedFiles.push(fileInfo);
        await this.USERVIDEO_KV.put(kvKey, JSON.stringify(existingData));
      }

      await this.DB.prepare(`UPDATE UserVideoState SET lastInteractionAt = ? WHERE userId = ? AND videoId = ?`)
        .bind(now, taskInfo.userId, taskInfo.videoId).run();

      const fileId = `${taskInfo.videoId}/${fileInfo.filename}`;
      const expiresAt = now + (30 * 24 * 60 * 60 * 1000);
      await this.DB.prepare(`INSERT INTO DownloadedFiles (fileId, videoId, downloadedAt, expiresAt, isArchived) VALUES (?, ?, ?, ?, 0)`)
        .bind(fileId, taskInfo.videoId, now, expiresAt).run();
    }
  }

  // Patrol / requestPatrol (atomic lock + snapshot)
  async requestPatrol() {
    const now = Date.now();
    if (now - this.lastPatrolTime < PATROL_INTERVAL_MS) {
      return new Response(JSON.stringify({ eligible: false, message: `Patrol cooldown` }), { status: 429 });
    }
    this.lastPatrolTime = now;
    await this.state.storage.put("hj_lastPatrolTime", this.lastPatrolTime);
    return new Response(JSON.stringify({ eligible: true, patrolTimestamp: this.lastPatrolTime, poolSnapshot: this.taskpool }), { headers: { 'Content-Type': 'application/json' } });
  }

  // freeOverSlots clears slots in 'over' state if included in provided taskIds
  async freeOverSlots(request) {
    const { taskIds } = await request.json();
    let freedCount = 0;
    if (taskIds && Array.isArray(taskIds) && taskIds.length > 0) {
      let modified = false;
      this.taskpool.forEach((slot, index) => {
        if (slot.state === 'over' && taskIds.includes(slot.taskId)) {
          this.taskpool[index] = this._createFreeSlot(index);
          freedCount++;
          modified = true;
        }
      });
      if (modified) await this.state.storage.put("hj_taskpool", this.taskpool);
    }
    return new Response(JSON.stringify({ success: true, freedCount }));
  }
}

// default fetch (for worker health check)
export default {
  async fetch(request, env, ctx) {
    return new Response("HjTaskman Worker active.");
  }
};
