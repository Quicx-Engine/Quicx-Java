package dev.quicx;

import java.util.Optional;

/** How a task ended, as reported back to the producer that submitted it. */
public final class TaskResult {

    private final int taskId;
    private final String failureReason;   // null when the task is done

    private TaskResult(int taskId, String failureReason) {
        this.taskId = taskId;
        this.failureReason = failureReason;
    }

    static TaskResult done(int taskId) {
        return new TaskResult(taskId, null);
    }

    static TaskResult failed(int taskId, String reason) {
        return new TaskResult(taskId, reason == null ? "" : reason);
    }

    /** The id quicxd gave the task. */
    public int taskId() {
        return taskId;
    }

    /** True if the worker reported DONE. */
    public boolean isDone() {
        return failureReason == null;
    }

    /** True if the worker reported FAILED. */
    public boolean isFailed() {
        return failureReason != null;
    }

    /**
     * True if quicxd evicted the task before any worker ran it, to make room
     * for a more valuable task when its memory budget was full (quicxd 2.1
     * with {@code admission = value}). The task was not run; send it again
     * later if it still matters.
     */
    public boolean isEvicted() {
        return EVICTED.equals(failureReason);
    }

    /** The reason quicxd gives an evicted task. */
    static final String EVICTED = "evicted";

    /** The worker's reason (at most 63 bytes), if it reported FAILED, or "evicted". */
    public Optional<String> failureReason() {
        return Optional.ofNullable(failureReason);
    }

    @Override
    public String toString() {
        if (isDone()) return "TaskResult{" + taskId + " done}";
        if (isEvicted()) return "TaskResult{" + taskId + " evicted}";
        return "TaskResult{" + taskId + " failed: " + failureReason + "}";
    }
}
