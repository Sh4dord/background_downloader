package com.bbflight.background_downloader

import android.app.Notification
import android.app.job.JobParameters
import android.app.job.JobService
import android.content.Context
import android.os.Build
import android.util.Log
import androidx.annotation.RequiresApi
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.CoroutineStart
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.Job
import kotlinx.coroutines.NonCancellable
import kotlinx.coroutines.SupervisorJob
import kotlinx.coroutines.cancel
import kotlinx.coroutines.channels.Channel
import kotlinx.coroutines.launch
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.withContext
import java.util.concurrent.ConcurrentHashMap
import kotlin.math.max

/**
 * JobService running all tasks of one group UIDT job, see [GroupUIDT]
 *
 * The job runs until there are no more pending or running tasks for its job key.
 * Tasks added while the job runs are picked up without scheduling a new job.
 */
@RequiresApi(34)
class GroupUIDTJobService : JobService() {

    private val handles = ConcurrentHashMap<Int, JobHandle>() // by jobId

    override fun onStartJob(params: JobParameters?): Boolean {
        if (params == null) return false
        val jobKey = params.extras.getString(GroupUIDT.keyJobKey)
        if (jobKey == null) {
            Log.e(TAG, "Group UIDT job started without job key")
            return false
        }
        Log.d(TAG, "Starting group UIDT job $jobKey")
        val handle = JobHandle(this, params, jobKey)
        handles[params.jobId] = handle
        handle.scope.launch {
            if (GroupUIDT.jobStarted(applicationContext, handle)) {
                handle.run()
            } else {
                handle.isClosed = true
            }
            handles.remove(params.jobId, handle)
            if (!handle.isStopped) {
                Log.d(TAG, "Group UIDT job $jobKey finished")
                jobFinished(params, false)
            }
        }
        return true // work continues on a background thread
    }

    override fun onStopJob(params: JobParameters?): Boolean {
        if (params == null) return false
        val handle = handles.remove(params.jobId) ?: return false
        Log.i(
            TAG,
            "Stopping group UIDT job ${handle.jobKey}, stop reason ${params.stopReason}"
        )
        handle.stop()
        // Reschedule only if tasks are still pending. A job stopped by the user
        // (e.g. via the Task Manager) is not rescheduled.
        val hasPendingTasks = runBlocking { GroupUIDT.jobStopped(applicationContext, handle) }
        return hasPendingTasks && params.stopReason != JobParameters.STOP_REASON_USER
    }

    /**
     * State and execution of a single group UIDT job
     */
    class JobHandle(
        val service: JobService,
        val params: JobParameters,
        val jobKey: String
    ) {
        /** True when the job no longer accepts tasks (finished or stopped) */
        @Volatile
        var isClosed = false

        /** True when the job was stopped by the system */
        @Volatile
        var isStopped = false

        val scope = CoroutineScope(SupervisorJob() + Dispatchers.IO)
        private val wakeChannel = Channel<Unit>(Channel.CONFLATED)
        private val runningTasks = ConcurrentHashMap<String, Pair<Job, GroupUIDTTaskContext>>()
        private val estimatedBytes = ConcurrentHashMap<String, Pair<Long, Long>>()

        val runningCount: Int
            get() = runningTasks.size

        /** Signals the job loop that tasks were added or finished */
        fun wake() {
            wakeChannel.trySend(Unit)
        }

        /** Runs pending tasks until there are none left, or the job is stopped */
        suspend fun run() {
            val appContext = service.applicationContext
            while (!isStopped) {
                val capacity = max(0, GroupUIDT.maxConcurrentPerJob - runningCount)
                val next = GroupUIDT.nextTasks(appContext, this, capacity) ?: break
                if (next.isEmpty() && runningCount == 0) {
                    continue // pending tasks were skipped, re-evaluate
                }
                if (next.isNotEmpty()) {
                    Log.v(TAG, "Starting ${next.size} tasks in $jobKey, $runningCount running")
                }
                next.forEach { (task, notificationConfigJsonString, resumeData) ->
                    launchTask(task, notificationConfigJsonString, resumeData)
                }
                wakeChannel.receive()
            }
        }

        /** Launch the runner for [task] in its own coroutine */
        private fun launchTask(
            task: Task,
            notificationConfigJsonString: String,
            resumeData: ResumeData?
        ) {
            val taskContext = GroupUIDTTaskContext(this, task, notificationConfigJsonString, resumeData)
            val runner = when (task.taskType) {
                "DownloadTask", "UriDownloadTask" -> DownloadTaskRunner(taskContext)
                "UploadTask", "UriUploadTask", "MultiUploadTask" -> UploadTaskRunner(taskContext)
                "DataTask" -> DataTaskRunner(taskContext)
                else -> {
                    Log.e(TAG, "Unsupported task type for group UIDT: ${task.taskType}")
                    null
                }
            }
            val job = scope.launch(start = CoroutineStart.LAZY) {
                try {
                    runner?.run()
                } finally {
                    withContext(NonCancellable) {
                        // update the job state before the running count, so the job
                        // loop never sees a finished job with a task still in its state
                        GroupUIDT.taskFinished(service.applicationContext, jobKey, task.taskId)
                        runningTasks.remove(task.taskId)
                        estimatedBytes.remove(task.taskId)
                        Log.v(TAG, "Task ${task.taskId} finished in $jobKey, $runningCount running")
                        wake()
                    }
                }
            }
            runningTasks[task.taskId] = Pair(job, taskContext)
            job.start()
        }

        /** Stop the running task with [taskId], e.g. because it was canceled */
        fun stopTask(taskId: String) {
            runningTasks[taskId]?.let { (job, taskContext) ->
                taskContext.isStopped = true
                job.cancel()
            }
        }

        /** Stop the job and all its running tasks */
        fun stop() {
            isStopped = true
            isClosed = true
            runningTasks.values.forEach { (_, taskContext) -> taskContext.isStopped = true }
            wake()
            scope.cancel()
        }

        /** Attach [notification] to the job, as required for a UIDT job */
        fun setNotification(notificationId: Int, notification: Notification) {
            if (isStopped) return
            service.setNotification(
                params,
                notificationId,
                notification,
                JobService.JOB_END_NOTIFICATION_POLICY_DETACH
            )
        }

        /** Update the network bytes estimate of the job with the estimate for [taskId] */
        fun updateEstimatedNetworkBytes(taskId: String, downloadBytes: Long, uploadBytes: Long) {
            if (isStopped) return
            estimatedBytes[taskId] = Pair(downloadBytes, uploadBytes)
            val totals = estimatedBytes.values
            try {
                service.updateEstimatedNetworkBytes(
                    params,
                    totals.sumOf { it.first },
                    totals.sumOf { it.second })
            } catch (e: Exception) {
                Log.v(TAG, "Could not update estimated network bytes: $e")
            }
        }
    }

    companion object {
        const val TAG = "GroupUIDTJobService"
    }
}

/**
 * [TaskJobContext] for one task running inside a group UIDT job
 *
 * Foreground notifications are attached to the job: all tasks of the job share the
 * group notification, so they attach the same notification id.
 */
@RequiresApi(34)
class GroupUIDTTaskContext(
    private val handle: GroupUIDTJobService.JobHandle,
    override var task: Task,
    override var notificationConfigJsonString: String?,
    private val resumeData: ResumeData?
) : TaskJobContext {
    override var notificationConfig: NotificationConfig? =
        notificationConfigJsonString?.let { bdJson.decodeFromString<NotificationConfig>(it) }
    override var notificationId: Int = 0
    override var notificationProgress: Double = 2.0
    override var networkSpeed: Double = -1.0
    override var taskCanResume: Boolean = false
    override var runInForeground: Boolean = true // the UIDT job is user-initiated

    @Volatile
    var isStopped: Boolean = false

    override val appContext: Context
        get() = handle.service.applicationContext

    override val isTaskStopped: Boolean
        get() = isStopped || handle.isStopped

    override val isActive: Boolean
        get() = !isTaskStopped

    override fun getInputLong(key: String, defaultValue: Long): Long =
        when (key) {
            TaskRunner.keyStartByte -> resumeData?.requiredStartByte ?: defaultValue
            else -> defaultValue
        }

    override fun getInputString(key: String): String? =
        when (key) {
            TaskRunner.keyResumeDataData -> resumeData?.data
            TaskRunner.keyETag -> resumeData?.eTag
            TaskRunner.keyNotificationConfig -> notificationConfigJsonString
            TaskRunner.keyTask -> bdJson.encodeToString(task)
            else -> null
        }

    override suspend fun setForegroundNotification(
        notificationId: Int,
        notification: Notification,
        notificationType: Int
    ) {
        handle.setNotification(notificationId, notification)
    }

    override suspend fun updateNotification(
        task: Task,
        status: TaskStatus,
        progress: Double,
        timeRemaining: Long
    ) {
        NotificationService.updateNotification(this, status, progress, timeRemaining)
    }

    override fun updateEstimatedNetworkBytes(downloadBytes: Long, uploadBytes: Long) {
        handle.updateEstimatedNetworkBytes(task.taskId, downloadBytes, uploadBytes)
    }
}
