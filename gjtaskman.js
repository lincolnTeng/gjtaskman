/**
 * GjTaskman.js
 * A Cloudflare Durable Object for managing a pool of asynchronous, long-running tasks.
 *
 * Core Philosophy: "Minimalist Traffic Director"
 * - Manages a fixed-size pool of "active" tasks to control concurrency.
 * - Acts as a lock to enforce a business rule: only one active task per (userId, videoId).
 * - Offloads tasks to an overflow queue in KV when the active pool is full.
 * - Does NOT persist the history or intermediate state of tasks within its own data structures.
 * - Its primary responsibility is scheduling and concurrency control, not data archiving.
 * - Prioritizes system availability by releasing locks even if final data persistence fails, preventing deadlocks.
 */

// Define constants for the task pool.
const POOL_SIZE = 50;

export class GjTaskman {
  constructor(state, env) {
    this.state = state;
    this.env = env;
    this.taskpool = [];

    // The initializePromise ensures that the DO is fully hydrated from storage
    // before any request is processed.
    this.initializePromise = this.initialize();
  }

  /**
   * Initializes the DO state, creating or loading the fixed-size task pool.
   */
  async initialize() {
    // Load the task pool from persistent storage.
    this.taskpool = await this.state.storage.get("gj_taskpool") || [];

    // If the pool is empty or its size has changed, recreate it.
    // This ensures the pool always matches the configured POOL_SIZE.
    if (this.taskpool.length !== POOL_SIZE) {
      this.taskpool = [];
      for (let i = 0; i < POOL_SIZE; i++) {
        this.taskpool.push(this._createFreeSlot(i));
      }
      // Persist the newly created pool structure.
      await this.state.storage.put("gj_taskpool", this.taskpool);
    }
  }
  
  /**
   * A helper to ensure the constructor's async initialization is complete.
   */
  async ensureInitialized() {
    await this.initializePromise;
  }

  /**
   * Creates a standardized object representing a free slot in the pool.
   * @param {number} id - The index of the slot in the pool array.
   */
  _createFreeSlot(id) {
    return {
      slotId: id,
      isFree: true,
      taskId: null,
      userId: null,
      videoId: null,
      taskcmd: null,
      type: null,
      state: null,
      submitTime: null,
    };
  }

  // --- PUBLIC API ENDPOINTS ---

  /**
   * Handles all incoming HTTP requests and routes them to the appropriate method.
   */
  async fetch(request) {
    await this.ensureInitialized();
    const url = new URL(request.url);
    switch (url.pathname) {
      case "/submit":
        return this.submit(request);
      case "/givemeatask":
        return this.givemeatask(request);
      case "/taskreturn":
        return this.taskreturn(request);
      case "/totalstate":
        return this.totalstate(request);
      // An administrative endpoint to clear stuck tasks might be useful.
      // case "/clearpool":
      //   return this.clearpool(request);
      default:
        return new Response("Not Found in GjTaskman DO", { status: 404 });
    }
  }

  /**
   * Submits a new task. The task is either placed into a free slot in the
   * active pool or added to the overflow queue if the pool is full.
   * Enforces the "one active task per (userId, videoId)" rule.
   */
  async submit(request) {
    const userContextHeader = request.headers.get('X-User-Context');
    const user = userContextHeader ? JSON.parse(userContextHeader) : null;
    if (!user) {
      return new Response(JSON.stringify({ error: "Unauthorized" }), { status: 401 });
    }

    const { videoid, taskcmd, type } = await request.json();

    // BUSINESS RULE: Check if a task for this user and video is already active.
    const existingTask = this.taskpool.find(s => !s.isFree && s.userId === user.id && s.videoId === videoid);
    if (existingTask) {
      return new Response(JSON.stringify({
        error: "An operation on this video is already in progress.",
        taskId: existingTask.taskId,
      }), { status: 409 }); // 409 Conflict
    }

    const taskId = `t_${crypto.randomUUID()}`;
    const taskInfo = {
      id: taskId,
      actionId: `act_${crypto.randomUUID()}`, // For detailed logging if needed
      userId: user.id,
      name: user.name,
      videoid,
      taskcmd,
      type,
      submitTime: Date.now(),
    };

    // Find the first available free slot in the pool.
    const freeSlotIndex = this.taskpool.findIndex(s => s.isFree);

    if (freeSlotIndex !== -1) {
      // --- POOL HAS SPACE ---
      this.taskpool[freeSlotIndex] = {
        slotId: freeSlotIndex,
        isFree: false,
        taskId: taskInfo.id,
        userId: taskInfo.userId,
        videoId: taskInfo.videoid,
        taskcmd: taskInfo.taskcmd,
        type: taskInfo.type,
        state: "waiting", // Ready to be picked up by a runner
        submitTime: taskInfo.submitTime,
      };
      await this.state.storage.put("gj_taskpool", this.taskpool);
      return new Response(JSON.stringify({ id: taskId }), { status: 202 });

    } else {
      // --- POOL IS FULL: Add to overflow queue ---
      await this._addToOverflowQueue(taskInfo);
      return new Response(JSON.stringify({
        id: taskId,
        message: "System is busy, your task has been queued.",
      }), { status: 202 });
    }
  }

  /**
   * A task runner calls this endpoint to get a task to execute.
   * It provides the oldest "waiting" task to ensure fairness (FIFO).
   */
  async givemeatask(request) {
    const waitingTasks = this.taskpool.filter(s => s.state === "waiting");

    if (waitingTasks.length === 0) {
      return new Response(JSON.stringify({ error: "No waiting tasks available" }), { status: 404 });
    }

    // Sort by submit time to find the oldest task.
    waitingTasks.sort((a, b) => a.submitTime - b.submitTime);
    const taskToRun = waitingTasks[0];

    // Update the task's state to "running" in the main pool.
    const slotIndex = taskToRun.slotId;
    this.taskpool[slotIndex].state = "running";
    await this.state.storage.put("gj_taskpool", this.taskpool);

    return new Response(JSON.stringify({ id: taskToRun.taskId, taskcmd: taskToRun.taskcmd }));
  }

  /**
   * A task runner calls this endpoint to return the result of a completed task.
   * This method handles final data persistence and, crucially, releases the lock (slot).
   */
  async taskreturn(request) {
    const { taskid, result } = await request.json();

    const slotIndex = this.taskpool.findIndex(s => s.taskId === taskid);
    if (slotIndex === -1) {
      // This can happen if a task result is returned after a DO reset or if it's a duplicate.
      // It's safe to ignore it.
      return new Response(JSON.stringify({ error: "Task not found in active pool, it may have already been cleared." }), { status: 404 });
    }
    const taskInfo = { ...this.taskpool[slotIndex] };

    // If the runner reports success, attempt to persist the final data.
    if (result.success && result.resultjson) {
      try {
        await this._persistFinalResult(taskInfo, result.resultjson);
      } catch (error) {
        // CRITICAL: The final write failed, but the runner thinks the task succeeded.
        // We log this critical error but proceed to release the lock to prevent a permanent deadlock.
        // This prioritizes system availability over strong consistency in this rare failure case.
        console.error(`CRITICAL PERSISTENCE FAILURE for successful task ${taskid}. Releasing lock to prevent deadlock. Manual data verification may be required. Error: ${error.stack}`);
      }
    }

    // Unconditionally release the slot (the lock). This is the most important step.
    this.taskpool[slotIndex] = this._createFreeSlot(slotIndex);
    await this.state.storage.put("gj_taskpool", this.taskpool);

    // Now that a slot is free, try to pull a waiting task from the overflow queue.
    await this._pullFromOverflowQueue();

    return new Response(JSON.stringify({ success: true }));
  }
  
  /**
   * Returns the current state of the task pool and overflow queue for monitoring.
   */
  async totalstate() {
    const running = this.taskpool.filter(s => s.state === 'running').length;
    const waiting = this.taskpool.filter(s => s.state === 'waiting').length;
    const free = POOL_SIZE - running - waiting;
    
    // Get queue length for a complete picture of system load.
    const queueLength = await this.env.USERVIDEO_KV.get("queue:length", { type: 'json' }) || 0;

    return new Response(JSON.stringify({
      poolSize: POOL_SIZE,
      activeTasks: running + waiting,
      running,
      waitingInPool: waiting,
      freeSlots: free,
      waitingInOverflowQueue: queueLength
    }));
  }

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

  /**
   * Adds a task to the KV-based linked-list overflow queue.
   */
  async _addToOverflowQueue(taskInfo) {
    const overflowTask = { ...taskInfo, next_taskId: null };
    await this.env.USERVIDEO_KV.put(`overflow:${taskInfo.id}`, JSON.stringify(overflowTask));
    
    const oldTailId = await this.env.USERVIDEO_KV.get("queue:tail_taskId");
    await this.env.USERVIDEO_KV.put("queue:tail_taskId", taskInfo.id);

    if (oldTailId) {
      // If the queue was not empty, link the old tail to the new tail.
      // This GET-then-PUT is safe because only the DO writes to the overflow queue.
      const oldTailTaskStr = await this.env.USERVIDEO_KV.get(`overflow:${oldTailId}`);
      if (oldTailTaskStr) {
        const oldTailTask = JSON.parse(oldTailTaskStr);
        oldTailTask.next_taskId = taskInfo.id;
        await this.env.USERVIDEO_KV.put(`overflow:${oldTailId}`, JSON.stringify(oldTailTask));
      }
    } else {
      // If the queue was empty, the new task is both the head and the tail.
      await this.env.USERVIDEO_KV.put("queue:head_taskId", taskInfo.id);
    }
    
    // Increment the queue length counter.
    const currentLength = await this.env.USERVIDEO_KV.get("queue:length", { type: 'json' }) || 0;
    await this.env.USERVIDEO_KV.put("queue:length", currentLength + 1);
  }

  /**
   * Pulls the next task from the overflow queue to fill a newly freed slot in the active pool.
   */
  async _pullFromOverflowQueue() {
    const freeSlotIndex = this.taskpool.findIndex(s => s.isFree);
    if (freeSlotIndex === -1) return; // No space, do nothing.

    const headId = await this.env.USERVIDEO_KV.get("queue:head_taskId");
    if (!headId) return; // Queue is empty, do nothing.

    const headTaskStr = await this.env.USERVIDEO_KV.get(`overflow:${headId}`);
    if (!headTaskStr) {
      // This indicates a data inconsistency. Reset the queue head.
      await this.env.USERVIDEO_KV.delete("queue:head_taskId");
      return;
    }
    const headTask = JSON.parse(headTaskStr);

    // Update the queue head to point to the next task.
    if (headTask.next_taskId) {
      await this.env.USERVIDEO_KV.put("queue:head_taskId", headTask.next_taskId);
    } else {
      // This was the last item in the queue.
      await this.env.USERVIDEO_KV.delete("queue:head_taskId");
      await this.env.USERVIDEO_KV.delete("queue:tail_taskId");
    }

    // Fill the free slot with the dequeued task's info.
    this.taskpool[freeSlotIndex] = {
      slotId: freeSlotIndex,
      isFree: false,
      taskId: headTask.id,
      userId: headTask.userId,
      videoId: headTask.videoid,
      taskcmd: headTask.taskcmd,
      type: headTask.type,
      state: "waiting",
      submitTime: headTask.submitTime,
    };
    await this.state.storage.put("gj_taskpool", this.taskpool);

    // Clean up the overflow task entry from KV.
    await this.env.USERVIDEO_KV.delete(`overflow:${headId}`);
    
    // Decrement the queue length counter.
    const currentLength = await this.env.USERVIDEO_KV.get("queue:length", { type: 'json' }) || 1;
    await this.env.USERVIDEO_KV.put("queue:length", currentLength - 1);
  }
}





export default {
  fetch(request, env, ctx) {
    // This root fetch handler is not used in our architecture because
    // all requests are routed via the Cloudflare Functions project (vdown).
    // It serves as a simple health check.
    return new Response("workervd is active.");
  }
};
