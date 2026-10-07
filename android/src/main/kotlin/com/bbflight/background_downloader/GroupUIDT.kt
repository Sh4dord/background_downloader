package com.bbflight.background_downloader

import android.Manifest
import android.app.job.JobInfo
import android.app.job.JobScheduler
import android.content.ComponentName
import android.content.Context
import android.content.SharedPreferences
import android.content.pm.PackageManager
import android.os.Build
import android.os.PersistableBundle
import android.util.Log
import androidx.core.app.NotificationManagerCompat
import androidx.core.content.ContextCompat
import androidx.core.content.edit
import androidx.preference.PreferenceManager
import kotlinx.coroutines.sync.Mutex
import kotlinx.coroutines.sync.withLock
import kotlinx.serialization.InternalSerializationApi
import kotlinx.serialization.Serializable
import java.util.concurrent.ConcurrentHashMap

/**
 * Persisted state of one group UIDT job
 *
 * Tasks are referenced by taskId only: the [Task] itself is already persisted in
 * the tasks map (see [BDPlugin.keyTasksMap]) by [BDPlugin.doEnqueue], which keeps
 * this state small even for groups of thousands of tasks.
 */
@Serializable
@OptIn(InternalSerializationApi::class)
data class GroupUIDTJobState(
    val notificationConfigJsonString: String,
    val requiresWiFi: Boolean,
    val pending: MutableList<String> = mutableListOf(),
    val running: MutableList<String> = mutableListOf(),
    val resumeData: MutableMap<String, ResumeData> = mutableMapOf()
)

/**
 * Group User Initiated Data Transfer (Android 14+)
 *
 * When activated via `Config.groupUIDT`, all eligible tasks that share a group
 * notification (a `NotificationConfig` with a `groupNotificationId`) and are
 * enqueued with priority 0 run inside a **single** UIDT job, instead of one
 * UIDT job per task. This:
 * - keeps the number of scheduled jobs at one per group (JobScheduler limits an
 *   app to 150 scheduled jobs)
 * - allows tasks to be added to the group while the app is in the background, as
 *   long as the job is running (a UIDT job can only be *scheduled* while the
 *   app is visible to the user)
 * - attaches the group notification to the job, so the user sees one notification
 *   for the whole group
 *
 * Tasks that require WiFi run in a separate job from tasks that don't, because a
 * job has a single network constraint. Both jobs share the same group notification.
 *
 * Concurrency within the job is governed by the [HoldingQueue] if configured, and
 * is otherwise capped at [maxConcurrentPerJob].
 */
object GroupUIDT {
    private const val TAG = "GroupUIDT"
    private const val keyJobStates = "com.bbflight.background_downloader.groupUIDT.jobStates"
    const val keyJobKey = "groupUIDTJobKey"

    /** Safety cap on the number of tasks running concurrently in one job */
    const val maxConcurrentPerJob = 10

    private val supportedTaskTypes = setOf(
        "DownloadTask", "UriDownloadTask", "UploadTask", "UriUploadTask",
        "MultiUploadTask", "DataTask"
    )

    /** Guards [jobStates], [tasks] and [activeJobs] transitions */
    internal val mutex = Mutex()
    private var jobStates: MutableMap<String, GroupUIDTJobState>? = null
    private val tasks = ConcurrentHashMap<String, Task>() // by taskId, pending and running

    /** Jobs currently executing, by job key */
    internal val activeJobs = ConcurrentHashMap<String, GroupUIDTJobService.JobHandle>()

    /**
     * Job keys of jobs scheduled by this process that have not started yet
     *
     * Distinguishes a job waiting to start (which will pick up newly added tasks) from
     * a job that is finishing (which won't), as JobScheduler reports both as pending
     */
    private val scheduledJobKeys = mutableSetOf<String>()

    /**
     * Returns true if the [task] with [notificationConfigJsonString] must run in a
     * group UIDT job
     */
    fun isEligible(context: Context, task: Task, notificationConfigJsonString: String?): Boolean {
        if (Build.VERSION.SDK_INT < 34 || task.priority != 0 || notificationConfigJsonString == null) {
            return false
        }
        if (task.taskType !in supportedTaskTypes || task.group == TaskRunner.chunkGroup) {
            return false
        }
        val prefs = PreferenceManager.getDefaultSharedPreferences(context)
        if (!prefs.getBoolean(BDPlugin.keyConfigGroupUIDT, false)) {
            return false
        }
        val notificationConfig = try {
            bdJson.decodeFromString<NotificationConfig>(notificationConfigJsonString)
        } catch (_: Exception) {
            return false
        }
        if (notificationConfig.groupNotificationId.isEmpty() || notificationConfig.running == null) {
            return false
        }
        if (ContextCompat.checkSelfPermission(
                context,
                Manifest.permission.RUN_USER_INITIATED_JOBS
            ) != PackageManager.PERMISSION_GRANTED
        ) {
            Log.w(TAG, "RUN_USER_INITIATED_JOBS permission not granted, not using group UIDT")
            return false
        }
        return true
    }

    /**
     * Adds the [task] to the group UIDT job for its group notification, and
     * schedules that job if it is not already scheduled or running
     *
     * Returns false if the job could not be scheduled, in which case the task is
     * not added and the caller should fall back to another execution method
     */
    suspend fun enqueue(
        context: Context,
        task: Task,
        notificationConfigJsonString: String,
        resumeData: ResumeData?,
        requiresWiFi: Boolean
    ): Boolean {
        val notificationConfig =
            bdJson.decodeFromString<NotificationConfig>(notificationConfigJsonString)
        val jobKey = jobKey(notificationConfig.groupNotificationId, requiresWiFi)
        val jobId = jobId(jobKey)
        val prefs = PreferenceManager.getDefaultSharedPreferences(context)
        val jobScheduler =
            context.getSystemService(Context.JOB_SCHEDULER_SERVICE) as JobScheduler
        return mutex.withLock {
            val states = loadedStates(prefs)
            val state = states.getOrPut(jobKey) {
                GroupUIDTJobState(notificationConfigJsonString, requiresWiFi)
            }
            state.pending.remove(task.taskId)
            state.running.remove(task.taskId)
            state.pending.add(task.taskId)
            if (resumeData != null) {
                state.resumeData[task.taskId] = resumeData
            } else {
                state.resumeData.remove(task.taskId)
            }
            tasks[task.taskId] = task
            val handle = activeJobs[jobKey]
            val scheduled = when {
                handle != null && !handle.isClosed -> {
                    handle.wake()
                    true
                }

                scheduledJobKeys.contains(jobKey) && jobScheduler.getPendingJob(jobId) != null ->
                    true // will pick up the task when it starts

                else -> schedule(context, jobScheduler, jobKey, jobId, requiresWiFi).also {
                    if (it) scheduledJobKeys.add(jobKey)
                }
            }
            if (!scheduled) {
                state.pending.remove(task.taskId)
                state.resumeData.remove(task.taskId)
                tasks.remove(task.taskId)
                if (state.pending.isEmpty() && state.running.isEmpty()) {
                    states.remove(jobKey)
                }
            }
            persist(prefs)
            scheduled
        }
    }

    /**
     * Schedules the UIDT job for [jobKey]. Must be called while holding the [mutex]
     */
    private fun schedule(
        context: Context,
        jobScheduler: JobScheduler,
        jobKey: String,
        jobId: Int,
        requiresWiFi: Boolean
    ): Boolean {
        if (Build.VERSION.SDK_INT < 34) return false
        return try {
            val jobInfo = JobInfo.Builder(
                jobId,
                ComponentName(context, GroupUIDTJobService::class.java)
            )
                .setUserInitiated(true)
                .setRequiredNetworkType(
                    if (requiresWiFi) JobInfo.NETWORK_TYPE_UNMETERED else JobInfo.NETWORK_TYPE_ANY
                )
                .setEstimatedNetworkBytes(
                    JobInfo.NETWORK_BYTES_UNKNOWN.toLong(),
                    JobInfo.NETWORK_BYTES_UNKNOWN.toLong()
                )
                .setExtras(PersistableBundle().apply { putString(keyJobKey, jobKey) })
                .build()
            val result = jobScheduler.schedule(jobInfo)
            if (result != JobScheduler.RESULT_SUCCESS) {
                Log.w(TAG, "Unable to schedule group UIDT job $jobKey")
            }
            result == JobScheduler.RESULT_SUCCESS
        } catch (e: Exception) {
            // Typically thrown when the app is not visible to the user
            Log.w(TAG, "Exception scheduling group UIDT job $jobKey: $e")
            false
        }
    }

    /**
     * Called by the [GroupUIDTJobService] when the job for [handle] starts
     *
     * Tasks that were running when the process died are moved back to the front of
     * the pending list, so they are restarted
     *
     * Returns false if there is no state for this job, i.e. nothing to do
     */
    internal suspend fun jobStarted(context: Context, handle: GroupUIDTJobService.JobHandle): Boolean {
        val prefs = PreferenceManager.getDefaultSharedPreferences(context)
        return mutex.withLock {
            scheduledJobKeys.remove(handle.jobKey)
            val state = loadedStates(prefs)[handle.jobKey] ?: return@withLock false
            if (state.running.isNotEmpty()) {
                Log.i(TAG, "Restarting ${state.running.size} interrupted tasks in ${handle.jobKey}")
                state.pending.addAll(0, state.running)
                state.running.clear()
                persist(prefs)
            }
            activeJobs[handle.jobKey] = handle
            true
        }
    }

    /**
     * Moves up to [capacity] pending tasks of the job for [handle] to the running list
     * and returns them, with their notification config and resume data
     *
     * If there is nothing pending and nothing running, the [handle] is closed and
     * unregistered (atomically, so that a concurrent [enqueue] schedules a new job)
     * and null is returned
     */
    internal suspend fun nextTasks(
        context: Context,
        handle: GroupUIDTJobService.JobHandle,
        capacity: Int
    ): List<Triple<Task, String, ResumeData?>>? {
        val prefs = PreferenceManager.getDefaultSharedPreferences(context)
        return mutex.withLock {
            val states = loadedStates(prefs)
            val state = states[handle.jobKey]
            if (state == null || (state.pending.isEmpty() && handle.runningCount == 0)) {
                if (state != null && state.running.isEmpty()) {
                    states.remove(handle.jobKey)
                    persist(prefs)
                }
                handle.isClosed = true
                activeJobs.remove(handle.jobKey, handle)
                return@withLock null
            }
            val result = mutableListOf<Triple<Task, String, ResumeData?>>()
            while (result.size < capacity && state.pending.isNotEmpty()) {
                val taskId = state.pending.removeAt(0)
                val task = tasks[taskId] ?: getTaskMap(prefs)[taskId]
                if (task == null) {
                    Log.w(TAG, "Could not find task $taskId, skipping")
                    state.resumeData.remove(taskId)
                    continue
                }
                tasks[taskId] = task
                state.running.add(taskId)
                result.add(Triple(task, state.notificationConfigJsonString, state.resumeData.remove(taskId)))
            }
            persist(prefs)
            result
        }
    }

    /** Called by the job when the task with [taskId] has finished running */
    internal suspend fun taskFinished(context: Context, jobKey: String, taskId: String) {
        val prefs = PreferenceManager.getDefaultSharedPreferences(context)
        mutex.withLock {
            val states = loadedStates(prefs)
            states[jobKey]?.let { state ->
                state.running.remove(taskId)
                if (state.running.isEmpty() && state.pending.isEmpty() && activeJobs[jobKey] == null) {
                    states.remove(jobKey)
                }
            }
            tasks.remove(taskId)
            persist(prefs)
        }
    }

    /**
     * Called by the job service when the system stops the job for [handle]
     *
     * Running tasks are stopped by the caller and report their own (failed) status,
     * consistent with tasks stopped by the system in other execution modes, so they
     * are removed from the job state. Pending tasks stay in the state.
     *
     * Returns true if the job should be rescheduled, i.e. if tasks are still pending
     */
    internal suspend fun jobStopped(context: Context, handle: GroupUIDTJobService.JobHandle): Boolean {
        val prefs = PreferenceManager.getDefaultSharedPreferences(context)
        return mutex.withLock {
            handle.isClosed = true
            activeJobs.remove(handle.jobKey, handle)
            val states = loadedStates(prefs)
            val state = states[handle.jobKey] ?: return@withLock false
            state.running.forEach { tasks.remove(it) }
            state.running.clear()
            val reschedule = state.pending.isNotEmpty()
            if (!reschedule) {
                states.remove(handle.jobKey)
            }
            persist(prefs)
            reschedule
        }
    }

    /**
     * Cancels the task with [taskId] if it is managed by a group UIDT job
     *
     * Returns true if the task was found (and canceled), false otherwise
     */
    suspend fun cancelTask(context: Context, taskId: String): Boolean {
        val prefs = PreferenceManager.getDefaultSharedPreferences(context)
        var jobKey: String? = null
        var task: Task? = null
        var wasRunning = false
        mutex.withLock {
            val states = loadedStates(prefs)
            for ((key, state) in states) {
                val inPending = state.pending.remove(taskId)
                val inRunning = !inPending && state.running.contains(taskId)
                if (inPending || inRunning) {
                    jobKey = key
                    wasRunning = inRunning
                    state.resumeData.remove(taskId)
                    // a task no longer in the tasks map has already reached a final state
                    task = getTaskMap(prefs)[taskId]
                    if (inPending) {
                        tasks.remove(taskId)
                    }
                    break
                }
            }
            if (jobKey != null) {
                persist(prefs)
            }
        }
        val canceledTask = task ?: return jobKey != null
        BDPlugin.canceledTaskIds.add(taskId)
        TaskRunner.processStatusUpdate(
            canceledTask,
            TaskStatus.canceled,
            prefs,
            context = context
        )
        BDPlugin.holdingQueue?.taskFinished(canceledTask)
        // remove outstanding notification for task or update the group notification
        val notificationGroup = NotificationService.groupNotificationWithTaskId(taskId)
        if (notificationGroup == null) {
            NotificationManagerCompat.from(context).cancel(taskId.hashCode())
        } else {
            NotificationService.createUpdateNotificationWorker(
                context,
                bdJson.encodeToString(canceledTask),
                bdJson.encodeToString(notificationGroup.notificationConfig),
                TaskStatus.canceled.ordinal
            )
        }
        if (wasRunning) {
            jobKey?.let { activeJobs[it]?.stopTask(taskId) }
        }
        return true
    }

    /**
     * Returns all unfinished tasks managed by group UIDT jobs, optionally only for [group]
     *
     * A task is unfinished while it is in the tasks map: [TaskRunner.processStatusUpdate]
     * removes it from there when it reaches a final state, which happens before the
     * job removes it from its own state
     */
    suspend fun allTasks(context: Context, group: String?): List<Task> {
        val prefs = PreferenceManager.getDefaultSharedPreferences(context)
        return mutex.withLock {
            val states = loadedStates(prefs)
            if (states.isEmpty()) return@withLock emptyList()
            val tasksMap = getTaskMap(prefs)
            states.values.flatMap { it.running + it.pending }
                .mapNotNull { tasksMap[it] }
                .filter { group == null || it.group == group }
        }
    }

    /**
     * Returns all tasks managed by group UIDT jobs, without suspending
     *
     * Used by the [HoldingQueue] to count active tasks; only reflects the in-memory state
     */
    fun activeTasksSnapshot(): List<Task> = tasks.values.toList()

    /** Key identifying the job for a [groupNotificationId] and network requirement */
    private fun jobKey(groupNotificationId: String, requiresWiFi: Boolean) =
        "$groupNotificationId|${if (requiresWiFi) "wifi" else "any"}"

    /** JobScheduler job id for the [jobKey] */
    private fun jobId(jobKey: String) = "groupUIDT|$jobKey".hashCode()

    /** Returns the job states, loading them from [prefs] if needed. Requires the [mutex] */
    private fun loadedStates(prefs: SharedPreferences): MutableMap<String, GroupUIDTJobState> {
        jobStates?.let { return it }
        val loaded: MutableMap<String, GroupUIDTJobState> = try {
            bdJson.decodeFromString(prefs.getString(keyJobStates, "{}") ?: "{}")
        } catch (e: Exception) {
            Log.w(TAG, "Could not load group UIDT job states: $e")
            mutableMapOf()
        }
        jobStates = loaded
        return loaded
    }

    /** Persists the job states to [prefs]. Requires the [mutex] */
    private fun persist(prefs: SharedPreferences) {
        val states = jobStates ?: return
        prefs.edit {
            if (states.isEmpty()) {
                remove(keyJobStates)
            } else {
                putString(keyJobStates, bdJson.encodeToString(states))
            }
        }
    }
}
