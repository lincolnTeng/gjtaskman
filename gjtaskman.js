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
      case "/killtask": return this.killtask(request);
        
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
  try {
    const { taskid, result } = await request.json();
    const slot = this.taskpool.find(s => s.taskId === taskid && s.state === 'running');
    
    if (!slot) return new Response("Task not found", { status: 404 });

    // 1. 瘦身逻辑：将全量结果存入 KV
    const kvKey = `task_res:${taskid}`;
    this.state.waitUntil(this.env.USERVIDEO_KV.put(kvKey, JSON.stringify(result), { 
      expirationTtl: 86400 
    }));

    // 2. DO 内存只保留轻量索引
    slot.state = "finished";
    slot.completionTime = Date.now();
    slot.result = {
      success: result.success,
      task_context: slot.task_context, // 必须保留，用于前端定位 Card
      kvKey: kvKey, // 记录去哪里取回全量数据
    };

    // 3. 异步持久化到 D1 (原有逻辑不变)
    if (result.success && result.resultjson) {
      this.state.waitUntil(this._persistFinalResult(slot, result.resultjson));
    }

    await this.state.storage.put("gj_taskpool", this.taskpool);
    return new Response(JSON.stringify({ success: true }));
  } catch (err) {
    return new Response(err.message, { status: 500 });
  }
}


  
async taskreturn4(request) {
  try {
    const { taskid, result } = await request.json();
    // 查找正在运行的槽位
    const slotIndex = this.taskpool.findIndex(s => s.taskId === taskid && s.state === 'running');
    
    if (slotIndex === -1) {
      return new Response(JSON.stringify({ error: "Task not found or not running" }), { status: 404 });
    }

    const slot = this.taskpool[slotIndex];

    // --- 1. 数据存入 KV (减重核心) ---
    // 原始巨大的 result 对象存入 KV，有效期 24 小时
    const kvKey = `task_res:${taskid}`;
    this.state.waitUntil(this.env.USERVIDEO_KV.put(kvKey, JSON.stringify(result), {
      expirationTtl: 86400 
    }));

    // --- 2. 内存状态更新 (适配前端逻辑) ---
    slot.completionTime = Date.now();
    slot.state = "finished";
    
    // 重要：这里的 result 必须包含 task_context，否则前端 handleResultPayload 会卡死
    slot.result = {
      success: result.success,
      result_type: slot.type, // 'profile' 或 'download'
      task_context: slot.task_context, // 还原上下文，确保包含 video_id 和 vdir
      kvKey: kvKey, // 告知前端/后续逻辑去哪里拿大数据
      summary: result.resultjson?.video_info?.title || "Task Completed"
    };

    // 同步保存到 DO 存储
    await this.state.storage.put("gj_taskpool", this.taskpool);

    // --- 3. 异步持久化 D1 数据库 ---
    if (result.success && result.resultjson) {
      this.state.waitUntil((async () => {
        try {
          await this._persistFinalResult(slot, result.resultjson);
          console.log(`D1 Persistence Success: ${taskid}`);
        } catch (e) {
          console.error("D1 Persistence Failed:", e);
        }
      })());
    }

    return new Response(JSON.stringify({ success: true }), {
      headers: { 'Content-Type': 'application/json' }
    });

  } catch (err) {
    console.error("Critical error in taskreturn:", err);
    return new Response(JSON.stringify({ error: err.message }), { status: 500 });
  }
}



  
async taskreturn3(request) {
    try {
      const { taskid, result } = await request.json();
      const slotIndex = this.taskpool.findIndex(s => s.taskId === taskid && s.state === 'running');
      
      if (slotIndex === -1) return new Response("Task not found", { status: 404 });
      const slot = this.taskpool[slotIndex];

      // --- 关键改进 1: 数据存入 KV，给 DO 减重 ---
      const kvKey = `task_res:${taskid}`;
      // 这里的 env.USERVIDEO_KV 确保已在 wrangler.toml 绑定
      this.state.waitUntil(this.env.USERVIDEO_KV.put(kvKey, JSON.stringify(result), {
        expirationTtl: 86400 // 24小时自动过期，清理战场
      }));

      // --- 关键改进 2: DO 内存只保留轻量索引 ---
      slot.completionTime = Date.now();
      slot.state = "finished";
      
      // 我们只存一个极小的 result 摘要在 DO storage 里
      slot.result = {
        success: result.success,
        kvKey: kvKey, // 记录去哪里取数据
        summary: result.resultjson?.video_info?.title || "Task Completed"
      };

      // 此时 put 的数据极小，彻底解决 128KB 限制问题
      await this.state.storage.put("gj_taskpool", this.taskpool);

      // --- 关键改进 3: 异步持久化 D1 ---
      if (result.success && result.resultjson) {
        this.state.waitUntil((async () => {
          try {
            await this._persistFinalResult(slot, result.resultjson);
          } catch (e) {
            console.error("D1 Persistence Failed:", e);
          }
        })());
      }

      return new Response(JSON.stringify({ success: true }));

    } catch (err) {
      console.error("taskreturn error:", err);
      return new Response(err.message, { status: 500 });
    }
  }


  
async taskreturn2(request) {
  try {
    const { taskid, result } = await request.json();
    const slotIndex = this.taskpool.findIndex(s => s.taskId === taskid && s.state === 'running');
    
    if (slotIndex === -1) {
      return new Response(JSON.stringify({ error: "Task not found" }), { status: 404 });
    }

    const slot = this.taskpool[slotIndex];
    slot.completionTime = Date.now();
    slot.result = result;

    // --- 关键修改 1: 立即同步修改状态并保存 ---
    // 先标记为 finished，确保 DO 即使现在崩溃，状态也是对的
    slot.state = "finished"; 
    await this.state.storage.put("gj_taskpool", this.taskpool);

    // --- 关键修改 2: 只把耗时的数据库/KV 写入放进 waitUntil ---
    if (result.success && result.resultjson) {
      this.state.waitUntil((async () => {
        try {
          // 这里只负责 DB 和 KV 的持久化
          await this._persistFinalResult(slot, result.resultjson);
          console.log(`Async persistence success for ${taskid}`);
        } catch (err) {
          console.error(`Async persistence CRITICAL FAILURE for ${taskid}:`, err.message);
          // 可以在这里考虑是否要把错误存入 slot.result.error 以便前端知晓
        }
      })());
    }

    // 立即返回成功，不再等待数据库
    return new Response(JSON.stringify({ success: true }), {
        headers: { 'Content-Type': 'application/json' }
    });

  } catch (globalErr) {
    console.error("Critical 500 error in taskreturn:", globalErr.stack);
    return new Response(JSON.stringify({ error: globalErr.message }), { status: 500 });
  }
}
  
  
  async taskreturn_slow(request) {
    const { taskid, result } = await request.json();
    const slotIndex = this.taskpool.findIndex(s => s.taskId === taskid && s.state === 'running');
    if (slotIndex === -1) {
      return new Response(JSON.stringify({ error: "Task not found or not running" }), { status: 404 });
    }

    const slot = this.taskpool[slotIndex];
    //slot.state = "finished";
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
           slot.state = "finished";
          })());

       
        /*try {
            await this._persistFinalResult(slot, result.resultjson);
        } catch (error) {
            console.error(`CRITICAL PERSISTENCE FAILURE for successful task ${taskid}. Error: ${error.stack}`);
            slot.result.persistenceError = error.message; // Log persistence error in the task result
        }*/
    }else { slot.state = "finished"; }
    

    return new Response(JSON.stringify({ success: true }));
  }

  /**
   * Client polls this to get task status and results.
   */
async taskstate(request) {
  const { taskid } = await request.json();
  const slot = this.taskpool.find(s => s.taskId === taskid);

  if (!slot) return new Response(JSON.stringify({ error: "Task missing" }), { status: 404 });

  return new Response(JSON.stringify({
    taskid: taskid,
    status: slot.state, // 只要返回 finished，前端就会触发 takeandover
    // 轮询时不需要给大数据，给个摘要即可
    result: (slot.state === 'finished' || slot.state === 'over') ? slot.result : null
  }));
}


  
async taskstate4(request) {
  try {
    const { taskid } = await request.json();
    const slot = this.taskpool.find(s => s.taskId === taskid);

    if (!slot) {
      return new Response(JSON.stringify({ error: "Task not found" }), { status: 404 });
    }

    const responsePayload = {
      taskid: taskid,
      status: slot.state, // 'running', 'finished', 'over'
    };

    // 如果任务已完成或已接管，返回我们在 taskreturn 中构造的瘦 result
    if (slot.state === 'finished' || slot.state === 'over') {
      responsePayload.result = slot.result;
    }

    return new Response(JSON.stringify(responsePayload), {
      headers: { 'Content-Type': 'application/json' }
    });
  } catch (err) {
    return new Response(JSON.stringify({ error: err.message }), { status: 500 });
  }
}



  
async taskstate3(request) {
    const { taskid } = await request.json();
    const slot = this.taskpool.find(s => s.taskId === taskid);
    
    if (!slot) return new Response(JSON.stringify({ error: "Not found" }), { status: 404 });

    let finalResult = slot.result;

    // 如果任务完成了，且我们存的是 KV 指针，则实时从 KV 取回完整数据给前端
    if (slot.state === 'finished' && slot.result?.kvKey) {
      const cached = await this.env.USERVIDEO_KV.get(slot.result.kvKey);
      if (cached) {
        finalResult = JSON.parse(cached);
      }
    }

    return new Response(JSON.stringify({
      status: slot.state,
      result: finalResult, // 这里的结构和原来一模一样，前端无感知
      taskId: slot.taskId,
      videoId: slot.videoId,
      type: slot.type
    }));
  }


  
  async taskstate2(request) {
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
  try {
    const { taskid } = await request.json();
    const slotIndex = this.taskpool.findIndex(s => s.taskId === taskid && s.state === 'finished');

    if (slotIndex === -1) {
      // 如果内存里找不到 finished，可能已经处理过，直接回退到查询逻辑
      return this.taskstate(request);
    }

    const slot = this.taskpool[slotIndex];
    const kvKey = slot.result.kvKey;

    // --- 关键设计复原：从 KV 中读取被瘦身的大数据 ---
    let fullResult = null;
    if (kvKey) {
      const kvData = await this.env.USERVIDEO_KV.get(kvKey);
      if (kvData) {
        fullResult = JSON.parse(kvData);
      }
    }

    // 标记状态为 over，准备从 pool 卸载
    slot.state = "over";
    await this.state.storage.put("gj_taskpool", this.taskpool);

    // 拼装给前端的完整包：状态 + 从 KV 复原的 result + 原始 context
    const responsePayload = {
      status: "finished",
      // 这里要把 KV 里的数据和内存里的 context 重新拼起来
      result: {
        ...(fullResult || {}), 
        task_context: slot.task_context // 确保这个字段在最外层，前端秒读
      }
    };

    return new Response(JSON.stringify(responsePayload), {
      headers: { 'Content-Type': 'application/json' }
    });

  } catch (err) {
    console.error("takeandover recovery failed:", err);
    return new Response(JSON.stringify({ error: err.message }), { status: 500 });
  }
}



  
  async takeandover4(request) {
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


/**
   * [ADMIN] Force resets a specific slot to "Free" state.
   * 用于手动清除僵尸任务，无论任务当前处于什么状态。
   */
  async killtask(request) {
    try {
        const { slotId } = await request.json();

        // 1. 校验参数
        // POOL_SIZE 是文件顶部的常量 (50)
        if (slotId === undefined || slotId === null || slotId < 0 || slotId >= POOL_SIZE) {
            return new Response(JSON.stringify({ error: "Invalid Slot ID" }), { status: 400 });
        }

        console.log(`[Admin] Force killing Slot #${slotId}`);

        // 2. 核心操作：直接用全新的空 Slot 覆盖当前位置
        // _createFreeSlot 是你现有的辅助函数，它会生成 { slotId: id, isFree: true, ... }
        this.taskpool[slotId] = this._createFreeSlot(slotId);

        // 3. 立即持久化 (这是最关键的一步，防止 DO 重启后回滚)
        await this.state.storage.put("gj_taskpool", this.taskpool);

        return new Response(JSON.stringify({ 
            success: true, 
            message: `Slot ${slotId} has been force freed.` 
        }));

    } catch (err) {
        return new Response(JSON.stringify({ error: err.message }), { status: 500 });
    }
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
