/*
 * Copyright 2013 The Netty Project
 *
 * The Netty Project licenses this file to you under the Apache License,
 * version 2.0 (the "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at:
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations
 * under the License.
 */

package io.netty.util.concurrent;

import io.netty.util.internal.DefaultPriorityQueue;
import io.netty.util.internal.PriorityQueueNode;

import java.util.Queue;
import java.util.concurrent.Callable;
import java.util.concurrent.Delayed;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

@SuppressWarnings("ComparableImplementedButEqualsNotOverridden")
final class ScheduledFutureTask<V> extends PromiseTask<V> implements ScheduledFuture<V>, PriorityQueueNode {
    /**
     * 任务序号生成器，通过 AtomicLong 实现递增发号
     */
    private static final AtomicLong nextTaskId = new AtomicLong();
    /**
     * 定时任务时间起点, 定时任务的执行时间，都是基于 START_TIME 做相对时间。
     * 最大的好处是：改了系统时间也没关系，存的是距离下次调度还要多长时间，不受系统时间影响
     */
    private static final long START_TIME = System.nanoTime();

    /**
     * 获得当前时间，这个是相对 START_TIME 来算的。
     * @return
     */
    static long nanoTime() {
        return System.nanoTime() - START_TIME;
    }

    /**
     * 获得任务执行时间，这个也是相对 START_TIME 来算的。
     * @param delay 延迟时长，单位：纳秒
     * @return 获得任务执行时间，也是相对 {@link #START_TIME} 来算的。
     *         实际上，返回的结果，会用于 {@link #deadlineNanos} 字段
     */
    static long deadlineNanos(long delay) {
        long deadlineNanos = nanoTime() + delay;
        // Guard against overflow
        return deadlineNanos < 0 ? Long.MAX_VALUE : deadlineNanos;
    }

    /**
     * 任务编号
     */
    private final long id = nextTaskId.getAndIncrement();
    /**
     * 任务执行时间，即到了该时间，该任务就会被执行
     */
    private long deadlineNanos;
    /* 0 - no repeat, >0 - repeat at fixed rate, <0 - repeat with fixed delay */
    /**
     * 任务执行周期
     * =0 - 只执行一次
     * >0 - 按照计划执行时间计算
     * <0 - 按照实际执行时间计算
     */
    private final long periodNanos;

    /**
     * 队列编号
     */
    private int queueIndex = INDEX_NOT_IN_QUEUE;

    ScheduledFutureTask(
            AbstractScheduledEventExecutor executor,
            Runnable runnable, V result, long nanoTime) {

        this(executor, toCallable(runnable, result), nanoTime);
    }

    ScheduledFutureTask(
            AbstractScheduledEventExecutor executor,
            Callable<V> callable, long nanoTime, long period) {

        super(executor, callable);
        if (period == 0) {
            throw new IllegalArgumentException("period: 0 (expected: != 0)");
        }
        deadlineNanos = nanoTime;
        periodNanos = period;
    }

    ScheduledFutureTask(
            AbstractScheduledEventExecutor executor,
            Callable<V> callable, long nanoTime) {

        super(executor, callable);
        deadlineNanos = nanoTime;
        periodNanos = 0;
    }

    @Override
    protected EventExecutor executor() {
        return super.executor();
    }

    public long deadlineNanos() {
        return deadlineNanos;
    }

    /**
     * 距离当前时间，还要多久可执行。若为负数，直接返回 0
     * @return
     */
    public long delayNanos() {
        return Math.max(0, deadlineNanos() - nanoTime());
    }

    /**
     * 距离指定时间，还要多久可执行。若为负数，直接返回 0
     * @param currentTimeNanos 指定时间
     * @return
     */
    public long delayNanos(long currentTimeNanos) {
        return Math.max(0, deadlineNanos() - (currentTimeNanos - START_TIME));
    }

    @Override
    public long getDelay(TimeUnit unit) {
        return unit.convert(delayNanos(), TimeUnit.NANOSECONDS);
    }

    /**
     * 用于队列( ScheduledFutureTask 使用 PriorityQueue 作为优先级队列 )排序。
     * 按照 deadlineNanos、id 属性升序排序。
     * @param o
     * @return
     */
    @Override
    public int compareTo(Delayed o) {
        if (this == o) {
            return 0;
        }

        ScheduledFutureTask<?> that = (ScheduledFutureTask<?>) o;
        long d = deadlineNanos() - that.deadlineNanos();
        if (d < 0) {
            return -1;
        } else if (d > 0) {
            return 1;
        } else if (id < that.id) {
            return -1;
        } else if (id == that.id) {
            throw new Error();
        } else {
            return 1;
        }
    }

    /**
     * 执行定时任务
     */
    @Override
    public void run() {
        // 必须在 EventLoop 的线程中
        assert executor().inEventLoop();
        try {
            // 执行周期为“只执行一次”的定时任务。
            if (periodNanos == 0) {
                // 设置任务不可取消
                if (setUncancellableInternal()) {
                    // 执行任务
                    V result = task.call();
                    // 通知任务执行成功
                    setSuccessInternal(result);
                }
            } else { //执行周期为“固定周期”的定时任务。
                // 判断任务并未取消
                // check if is done as it may was cancelled
                if (!isCancelled()) {
                    // 执行任务
                    task.call();
                    if (!executor().isShutdown()) {
                        // 计算下次执行时间
                        long p = periodNanos;
                        if (p > 0) {
                            // 修改定时任务的 deadlineNanos 属性，从而变成新的定时任务执行时间。
                            deadlineNanos += p;
                        } else {
                            // 修改定时任务的 deadlineNanos 属性，从而变成新的定时任务执行时间。
                            deadlineNanos = nanoTime() - p;
                        }
                        // 判断任务并未取消
                        if (!isCancelled()) {
                            // 重新添加到任务队列，等待下次定时执行
                            // scheduledTaskQueue can never be null as we lazy init it before submit the task!
                            Queue<ScheduledFutureTask<?>> scheduledTaskQueue =
                                    ((AbstractScheduledEventExecutor) executor()).scheduledTaskQueue;
                            assert scheduledTaskQueue != null;
                            scheduledTaskQueue.add(this);
                        }
                    }
                }
            }
        } catch (Throwable cause) {
            // 发生异常，通知任务执行失败
            setFailureInternal(cause);
        }
    }

    /**
     * 取消定时任务，并从定时任务队列移除自己。
     *
     * {@inheritDoc}
     *
     * @param mayInterruptIfRunning this value has no effect in this implementation.
     */
    @Override
    public boolean cancel(boolean mayInterruptIfRunning) {
        boolean canceled = super.cancel(mayInterruptIfRunning);
        // 取消成功，移除出定时任务队列
        if (canceled) {
            ((AbstractScheduledEventExecutor) executor()).removeScheduled(this);
        }
        return canceled;
    }

    /**
     * 取消定时任务, 不从定时任务队列移除自己。
     * @param mayInterruptIfRunning
     * @return
     */
    boolean cancelWithoutRemove(boolean mayInterruptIfRunning) {
        return super.cancel(mayInterruptIfRunning);
    }

    @Override
    protected StringBuilder toStringBuilder() {
        StringBuilder buf = super.toStringBuilder();
        buf.setCharAt(buf.length() - 1, ',');

        return buf.append(" id: ")
                  .append(id)
                  .append(", deadline: ")
                  .append(deadlineNanos)
                  .append(", period: ")
                  .append(periodNanos)
                  .append(')');
    }

    /**
     * 获得queueIndex 属性。
     * @param queue
     * @return
     */
    @Override
    public int priorityQueueIndex(DefaultPriorityQueue<?> queue) {
        return queueIndex;
    }

    /**
     * 设置queueIndex 属性。
     * @param queue The queue for which the index is being set.
     * @param i The index as used by {@link DefaultPriorityQueue}.
     */
    @Override
    public void priorityQueueIndex(DefaultPriorityQueue<?> queue, int i) {
        queueIndex = i;
    }
}
