// /gjtaskman-worker/GjTaskman.js
// A simplified, robust Durable Object for task management based on a fixed-size pool.

const POOL_SIZE = 50;
const PATROL_INTERVAL_MS = 15000; // 15 seconds

export class GjTaskman {
  constructor(state, env) {
    this.state = state;
    this.env = env;
    this.taskpool = [];
    this.lastPatrolTime = 0; // New state for patrol lock    
    this.initializePromise = this.initialize();
  }

  async initialize() {
    // --- CORRECTED INITIALIZATION LOGIC ---
    let pool = await this.state.storage.get("gj_taskpool");

    // Only initialize the pool IF AND ONLY IF it has never been created before.
    if (pool === undefined || pool === null) {
        console.log("GjTaskman: Initializing a brand new task pool.");
        pool = Array.from({ length: POOL_SIZE }, (_, i) => this._createFreeSlot(i));
        await this.state.storage.put("gj_taskpool", pool);
    }
    
    this.taskpool = pool;
    this.lastPatrolTime = await this.state.storage.get("lastPatrolTime") || 0;
  }

  async ensureInitialized() {
    await this.initializePromise;
  }

  _createFreeSlot(id) {
    return { slotId: id, isFree: true, taskId: null, state: null };
  }

  async fetch(request) {
    await this.ensureInitialized();
    const url = new URL(request.url);
    switch (url.pathname) {
      case "/submit": return this.submit(request);
      case "/givemeatask": return this.givemeatask(request);
      case "/taskreturn": return this.taskreturn(request);
      case "/taskstate": return this.taskstate(request);
      case "/takeandover": return this.takeandover(request); // New endpoint for frontend
      case "/totalstate": return this.totalstate(request);
      // --- NEW ADMIN ENDPOINTS ---
      case "/requestpatrol": return this.requestPatrol(request);
      case "/freeoverslots": return this.freeOverSlots(request);
      // ---------------------------

        
      default: return new Response("Not Found in GjTaskman DO", { status: 404 });
    }
  }

  /**
   * Submits a new task. Fails if the pool is full.
   */
  async submit(request) {
    const userContextHeader = request.headers.get('X-User-Context');
    const user = userContextHeader ? JSON.parse(userContextHeader) : null;
    if (!user) return new Response(JSON.stringify({ error: "Unauthorized" }), { status: 401 });

    const { videoid, taskcmd, type } = await request.json();

    const freeSlotIndex = this.taskpool.findIndex(s => s.isFree);

    if (freeSlotIndex === -1) {
      return new Response(JSON.stringify({
        error: "System is busy, please try again shortly. Consider upgrading for higher concurrency.",
      }), { status: 429 }); // 429 Too Many Requests
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

    await this.state.storage.put("gj_taskpool", this.taskpool);
    return new Response(JSON.stringify({ id: taskId }), { status: 202 });
  }

  /**
   * A task runner gets a task to execute.
   */
  async givemeatask(request) {
    const waitingTasks = this.taskpool.filter(s => s.state === "waiting");
    if (waitingTasks.length === 0) {
      return new Response(JSON.stringify({ error: "No waiting tasks available" }), { status: 404 });
    }

    waitingTasks.sort((a, b) => a.submitTime - b.submitTime);
    const taskToRun = waitingTasks[0];

    taskToRun.state = "running";
    taskToRun.startTime = Date.now();
    await this.state.storage.put("gj_taskpool", this.taskpool);

    return new Response(JSON.stringify({ id: taskToRun.taskId, taskcmd: taskToRun.taskcmd }));
  }

  /**
   * A task runner returns the result. The slot is NOT freed but marked as 'finished'.
   */
  async taskreturn(request) {
    const { taskid, result } = await request.json();
    const slotIndex = this.taskpool.findIndex(s => s.taskId === taskid && s.state === 'running');
    if (slotIndex === -1) {
      return new Response(JSON.stringify({ error: "Task not found or not running" }), { status: 404 });
    }

    const slot = this.taskpool[slotIndex];
    slot.state = "finished";
    slot.completionTime = Date.now();
    slot.result = result; // Store the full result from the runner

    // --- CRITICAL: This is the ONLY persistence of the FINAL USER DATA ---
    if (result.success && result.resultjson) {
        try {
            await this._persistFinalResult(slot, result.resultjson);
        } catch (error) {
            console.error(`CRITICAL PERSISTENCE FAILURE for successful task ${taskid}. Error: ${error.stack}`);
            slot.result.persistenceError = error.message; // Log persistence error in the task result
        }
    }
    
    await this.state.storage.put("gj_taskpool", this.taskpool);
    return new Response(JSON.stringify({ success: true }));
  }

  /**
   * Client polls this to get task status and results.
   */
  async taskstate(request) {
    const { taskid } = await request.json();
    if (!taskid) return new Response(JSON.stringify({ error: "taskid is required" }), { status: 400 });

    const slot = this.taskpool.find(s => s.taskId === taskid);
    if (!slot) {
      return new Response(JSON.stringify({ error: "Task not found (may have been cleared)" }), { status: 404 });
    }

    // Return a subset of the slot data, including status and result.
    const responsePayload = {
      status: slot.state,
      result: slot.result,
    };

    return new Response(JSON.stringify(responsePayload), {
      headers: { 'Content-Type': 'application/json' }
    });
  }
  
  /**
   * NEW: Frontend calls this to fetch the result AND mark the task as "over".
   */
  async takeandover(request) {
    const { taskid } = await request.json();
    if (!taskid) return new Response(JSON.stringify({ error: "taskid is required" }), { status: 400 });

    const slotIndex = this.taskpool.findIndex(s => s.taskId === taskid && s.state === 'finished');
    if (slotIndex === -1) {
      // Maybe the frontend is polling an old task or taskstate already got the result.
      // Let's check taskstate logic first.
      return this.taskstate(request);
    }
    
    const slot = this.taskpool[slotIndex];
    slot.state = "over"; // Mark as "over"
    
    await this.state.storage.put("gj_taskpool", this.taskpool);

    const responsePayload = {
      status: "finished", // Still report "finished" to the client
      result: slot.result,
    };

    return new Response(JSON.stringify(responsePayload), {
      headers: { 'Content-Type': 'application/json' }
    });
  }

  /**
   * Returns a high-level state of the pool for monitoring.
   */
  async totalstate() {
    const states = this.taskpool.reduce((acc, slot) => {
        if (slot.isFree) {
            acc.free = (acc.free || 0) + 1;
        } else {
            acc[slot.state] = (acc[slot.state] || 0) + 1;
        }
        return acc;
    }, {});
    
    return new Response(JSON.stringify({
        poolSize: POOL_SIZE,
        ...states
    }));
  }

  // This private helper remains unchanged
 // --- PRIVATE HELPERS ---

  /**
   * Persists the successful result of a task to the final destinations (DB and user-facing KV).
   * This logic is replicated from the previous implementation to ensure data compatibility.
   */
  async _persistFinalResult(taskInfo, fullProfile) {
    const now = Date.now();
    
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
      
      await this.env.USERVIDEO_KV.put(`user:${taskInfo.userId}:${taskInfo.videoId}`, JSON.stringify(userVideoData));
      await this.env.DB.prepare(
        `INSERT INTO UserVideoState (userId, videoId, isArchived, lastInteractionAt) VALUES (?, ?, 0, ?)
         ON CONFLICT(userId, videoId) DO UPDATE SET lastInteractionAt = excluded.lastInteractionAt`
      ).bind(taskInfo.userId, taskInfo.videoId, now).run();

    } else if (taskInfo.type === 'download' || taskInfo.type === 'combine') {
      const fileInfo = fullProfile.file_info;
      const kvKey = `user:${taskInfo.userId}:${taskInfo.videoId}`;
      let existingData = await this.env.USERVIDEO_KV.get(kvKey, { type: 'json' });

      if (existingData) {
        if (!existingData.downloadedFiles) existingData.downloadedFiles = [];
        existingData.downloadedFiles.push(fileInfo);
        await this.env.USERVIDEO_KV.put(kvKey, JSON.stringify(existingData));
      }
      
      await this.env.DB.prepare(`UPDATE UserVideoState SET lastInteractionAt = ? WHERE userId = ? AND videoId = ?`)
        .bind(now, taskInfo.userId, taskInfo.videoId).run();
      
      const fileId = `${taskInfo.videoId}/${fileInfo.filename}`;
      const expiresAt = now + (30 * 24 * 60 * 60 * 1000);
      await this.env.DB.prepare(`INSERT INTO DownloadedFiles (fileId, videoId, downloadedAt, expiresAt, isArchived) VALUES (?, ?, ?, ?, 0)`)
        .bind(fileId, taskInfo.videoId, now, expiresAt).run();
    }
  }

 
  // --- NEW: Atomic patrol lock and snapshot provider (CORRECTED) ---
  async requestPatrol() {
      const now = Date.now();
      if (now - this.lastPatrolTime < PATROL_INTERVAL_MS) {
          // Not yet time to patrol, return a signal.
          return new Response(JSON.stringify({
              eligible: false,
              message: `Patrol is on cooldown. Try again in ${Math.round(((this.lastPatrolTime + PATROL_INTERVAL_MS) - now) / 1000)}s.`
          }), { status: 429 });
      }

      // It's time. Grant the lock by updating the timestamp immediately.
      this.lastPatrolTime = now;
      // --- CRITICAL FIX: Await the storage operation to guarantee persistence ---
      await this.state.storage.put("lastPatrolTime", this.lastPatrolTime);

      // Return eligibility along with the current timestamp and the full pool snapshot.
      return new Response(JSON.stringify({
          eligible: true,
          patrolTimestamp: this.lastPatrolTime,
          poolSnapshot: this.taskpool
      }), { headers: { 'Content-Type': 'application/json' }});
  }



  
  // --- NEW: Cleans up slots that have been successfully archived ---
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
          if (modified) {
              await this.state.storage.put("gj_taskpool", this.taskpool);
          }
      }
      return new Response(JSON.stringify({ success: true, freedCount }));
  }


  
}

export default {
  async fetch(request, env, ctx) {
    return new Response("GjTaskman Worker is active.");
  }
};
