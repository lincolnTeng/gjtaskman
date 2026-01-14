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

    // --- NEW LOGIC: Route task based on command ---
    if (taskcmd && taskcmd.startsWith('l@')) {
        // This is an instant clone task. Handle it directly.
        return this.handleInstantCloneTask(user, videoid, taskcmd);
    }

    
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

  // *** NEW PRIVATE HELPER FUNCTION ***
  /**
   * Handles a clone task instantly and places a 'finished' record in the task pool.
   */
  async handleInstantCloneTask(user, videoId, taskcmd) {
    const freeSlotIndex = this.taskpool.findIndex(s => s.isFree);
    if (freeSlotIndex === -1) {
        return new Response(JSON.stringify({ error: "System is busy, cannot store clone result." }), { status: 429 });
    }

    const taskId = `t_${crypto.randomUUID()}`;
    let resultPayload = {};

    try {
        // 1. Execute the clone logic directly (migrated from clone.js)
      //  const [originalUserId, originalVideoId] = taskcmd.split('@')[1].split('+');

            // 1. Parse the command to get the originalUserId
            // command is "l@originalUserId@videoId"
            const originalUserId = taskcmd.split('@')[1]; 

            // 2. The videoId is passed directly as an argument to this function.
            const originalVideoId = videoId;
      
        const targetKey = `user:${user.id}:${originalVideoId}`;
        const originalKey = `user:${originalUserId}:${originalVideoId}`;

        const originalDataStr = await this.env.USERVIDEO_KV.get(originalKey);
        if (!originalDataStr) throw new Error('Original project not found.');
        
        const clonedData = JSON.parse(originalDataStr);
        clonedData.isArchived = false;
        clonedData.category = null;

        await this.env.USERVIDEO_KV.put(targetKey, JSON.stringify(clonedData));

        const now = Date.now();
        await this.env.DB.prepare(
            `INSERT INTO UserVideoState (userId, videoId, isArchived, lastInteractionAt) VALUES (?, ?, 0, ?)
             ON CONFLICT(userId, videoId) DO UPDATE SET lastInteractionAt = excluded.lastInteractionAt, isArchived = 0`
        ).bind(user.id, originalVideoId, now).run();

        // 2. Construct a result payload that is compatible with the frontend
        resultPayload = {
            success: true,
            resultjson: {
                ...clonedData,
                result_type: 'clone' // new task type
            }
        };

    } catch (e) {
        console.error(`Instant clone task failed for user ${user.id}:`, e.stack);
        resultPayload = {
            success: false,
            logs: [{ src: 'GjTaskman.Clone', type: 'ERROR', payload: e.message }]
        };
    }

    // 3. Place a pre-completed task record into the pool
    this.taskpool[freeSlotIndex] = {
        slotId: freeSlotIndex,
        isFree: false,
        taskId,
        userId: user.id,
        videoId: videoId,
        taskcmd,
        type: 'clone',
        state: "finished", // Key difference: state starts as 'finished'
        submitTime: Date.now(),
        completionTime: Date.now(),
        result: resultPayload,
    };

    // 4. Persist the pool and return the taskId for the frontend to poll
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
    await this.state.storage.put("gj_taskpool", this.taskpool);

    
    // --- CRITICAL: This is the ONLY persistence of the FINAL USER DATA ---
     if (result.success && result.resultjson) {

         this.state.waitUntil((async () => {
                try {
                    // 原来的 _persistFinalResult 逻辑放在这里
                    await this._persistFinalResult(slot, result.resultjson);
                } catch (err) {
                    console.error(`Async persistence failed for ${taskid}:`, err);
                    // 就算这里失败了，前端也已经拿到结果了，影响仅限于“历史记录”里缺了一条
                }
          })());

       
        /*try {
            await this._persistFinalResult(slot, result.resultjson);
        } catch (error) {
            console.error(`CRITICAL PERSISTENCE FAILURE for successful task ${taskid}. Error: ${error.stack}`);
            slot.result.persistenceError = error.message; // Log persistence error in the task result
        }*/
    }
    

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
    //const responsePayload = {      status: slot.state,      result: slot.result,  };
    // --- 修改开始：返回更多用于 UI 展示的字段 ---
    const responsePayload = {
        // 基础状态
        status: slot.state, 
        result: slot.result,
        // UI 元数据
        taskId: slot.taskId,
        slotId: slot.slotId,
        videoId: slot.videoId,
        userId: slot.userId,
        type: slot.type,
        taskcmd: slot.taskcmd,
        submitTime: slot.submitTime,
        startTime: slot.startTime
    };
    // --- 修改结束 ---



    
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
    const activeIds = []; 
    const states = this.taskpool.reduce((acc, slot) => {
        if (slot.isFree) {
            acc.free = (acc.free || 0) + 1;
        } else {
            acc[slot.state] = (acc[slot.state] || 0) + 1;
            if (slot.taskId) activeIds.push(slot.taskId);
          
        }
        return acc;
    }, {});
    
    return new Response(JSON.stringify({
        poolSize: POOL_SIZE,
        ...states,
        activeIds 
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
