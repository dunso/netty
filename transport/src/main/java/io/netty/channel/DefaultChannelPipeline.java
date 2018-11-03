/*
 * Copyright 2012 The Netty Project
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
package io.netty.channel;

import io.netty.channel.Channel.Unsafe;
import io.netty.util.ReferenceCountUtil;
import io.netty.util.ResourceLeakDetector;
import io.netty.util.concurrent.EventExecutor;
import io.netty.util.concurrent.EventExecutorGroup;
import io.netty.util.concurrent.FastThreadLocal;
import io.netty.util.internal.ObjectUtil;
import io.netty.util.internal.StringUtil;
import io.netty.util.internal.UnstableApi;
import io.netty.util.internal.logging.InternalLogger;
import io.netty.util.internal.logging.InternalLoggerFactory;

import java.net.SocketAddress;
import java.util.ArrayList;
import java.util.IdentityHashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.WeakHashMap;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.atomic.AtomicReferenceFieldUpdater;

/**
 * 默认 ChannelPipeline 实现类。实际上，也只有这个实现类。
 *
 * pipeline 中的节点的数据结构是 ChannelHandlerContext 类。
 * 每个 ChannelHandlerContext 包含一个 ChannelHandler、它的上下节点( 从而形成 ChannelHandler 链 )、以及其他上下文。
 *
 * 默认情况下，pipeline 有 head 和 tail 节点，形成默认的 ChannelHandler 链。
 * 而我们可以在它们之间，加入自定义的 ChannelHandler 节点
 *
 * The default {@link ChannelPipeline} implementation.  It is usually created
 * by a {@link Channel} implementation when the {@link Channel} is created.
 */
public class DefaultChannelPipeline implements ChannelPipeline {

    static final InternalLogger logger = InternalLoggerFactory.getInstance(DefaultChannelPipeline.class);

    /**
     * {@link #head} 的名字
     */
    private static final String HEAD_NAME = generateName0(HeadContext.class);
    /**
     * {@link #tail} 的名字
     */
    private static final String TAIL_NAME = generateName0(TailContext.class);

    /**
     * 名字({@link AbstractChannelHandlerContext#name})缓存 ，基于 ThreadLocal ，用于生成在线程中唯一的名字。
     */
    private static final FastThreadLocal<Map<Class<?>, String>> nameCaches =
            new FastThreadLocal<Map<Class<?>, String>>() {
        @Override
        protected Map<Class<?>, String> initialValue() throws Exception {
            return new WeakHashMap<Class<?>, String>();
        }
    };

    /**
     * {@link #estimatorHandle} 的原子更新器
     */
    private static final AtomicReferenceFieldUpdater<DefaultChannelPipeline, MessageSizeEstimator.Handle> ESTIMATOR =
            AtomicReferenceFieldUpdater.newUpdater(
                    DefaultChannelPipeline.class, MessageSizeEstimator.Handle.class, "estimatorHandle");
    /**
     *  Head 节点
     */
    final AbstractChannelHandlerContext head;
    /**
     * Tail 节点
     */
    final AbstractChannelHandlerContext tail;

    /**
     * 所属 Channel 对象
     */
    private final Channel channel;

    /**
     * 成功的 Promise 对象
     */
    private final ChannelFuture succeededFuture;

    /**
     * 不进行通知的 Promise 对象
     * 用于一些方法执行，需要传入 Promise 类型的方法参数，但是不需要进行通知，就传入该值
     * @see io.netty.channel.AbstractChannel.AbstractUnsafe#safeSetSuccess(ChannelPromise)
     */
    private final VoidChannelPromise voidPromise;

    /**
     * DefaultChannelPipeline 字段用途
     */
    private final boolean touch = ResourceLeakDetector.isEnabled();

    /**
     * 子执行器集合。
     *
     * 默认情况下，ChannelHandler 使用 Channel 所在的 EventLoop 作为执行器。
     * 但是如果有需要，也可以自定义执行器。详细解析，见 {@link #childExecutor(EventExecutorGroup)} 。
     * 实际情况下，基本不会用到。
     */
    private Map<EventExecutorGroup, EventExecutor> childExecutors;

    /**
     * DefaultChannelPipeline 字段用途
     */
    private volatile MessageSizeEstimator.Handle estimatorHandle;

    /**
     * 是否首次注册
     */
    private boolean firstRegistration = true;

    /**
     * 准备添加 ChannelHandler 的回调
     *
     * This is the head of a linked list that is processed by {@link #callHandlerAddedForAllHandlers()} and so process
     * all the pending {@link #callHandlerAdded0(AbstractChannelHandlerContext)}.
     *
     * We only keep the head because it is expected that the list is used infrequently and its size is small.
     * Thus full iterations to do insertions is assumed to be a good compromised to saving memory and tail management
     * complexity.
     */
    private PendingHandlerCallback pendingHandlerCallbackHead;

    /**
     * 是否已注册
     *
     * Set to {@code true} once the {@link AbstractChannel} is registered.Once set to {@code true} the value will never
     * change.
     */
    private boolean registered;

    protected DefaultChannelPipeline(Channel channel) {
        this.channel = ObjectUtil.checkNotNull(channel, "channel");
        // succeededFuture 的创建
        succeededFuture = new SucceededChannelFuture(channel, null);
        // voidPromise 的创建
        voidPromise =  new VoidChannelPromise(channel, true);

        // 创建 Tail 节点
        tail = new TailContext(this);
        // 创建 Head 节点
        head = new HeadContext(this);

        // 相互指向
        head.next = tail;
        tail.prev = head;
    }

    final MessageSizeEstimator.Handle estimatorHandle() {
        MessageSizeEstimator.Handle handle = estimatorHandle;
        if (handle == null) {
            handle = channel.config().getMessageSizeEstimator().newHandle();
            if (!ESTIMATOR.compareAndSet(this, null, handle)) {
                handle = estimatorHandle;
            }
        }
        return handle;
    }

    final Object touch(Object msg, AbstractChannelHandlerContext next) {
        return touch ? ReferenceCountUtil.touch(msg, next) : msg;
    }

    /**
     * 创建 DefaultChannelHandlerContext 节点
     * @param group
     * @param name
     * @param handler 内嵌传入的 ChannelHandler 参数
     * @return
     */
    private AbstractChannelHandlerContext newContext(EventExecutorGroup group, String name, ChannelHandler handler) {
        // 创建子执行器
        return new DefaultChannelHandlerContext(this, childExecutor(group), name, handler);
    }

    private EventExecutor childExecutor(EventExecutorGroup group) {
        // 当不传入 EventExecutorGroup 时，不创建子执行器。
        // 即，使用 Channel 所注册的 EventLoop 作为执行器。对于我们日常使用，基本完全都是这种情况。
        if (group == null) {
            return null;
        }
        // 根据配置项 SINGLE_EVENTEXECUTOR_PER_GROUP ，每个 Channel 从 EventExecutorGroup 获得不同 EventExecutor 执行器
        Boolean pinEventExecutor = channel.config().getOption(ChannelOption.SINGLE_EVENTEXECUTOR_PER_GROUP);
        if (pinEventExecutor != null && !pinEventExecutor) {
            return group.next();
        }
        // 通过 childExecutors 缓存实现，一个 Channel 从 EventExecutorGroup 获得相同 EventExecutor 执行器
        Map<EventExecutorGroup, EventExecutor> childExecutors = this.childExecutors;
        if (childExecutors == null) {
            // Use size of 4 as most people only use one extra EventExecutor.
            childExecutors = this.childExecutors = new IdentityHashMap<EventExecutorGroup, EventExecutor>(4);
        }
        // Pin one of the child executors once and remember it so that the same child executor
        // is used to fire events for the same channel.
        EventExecutor childExecutor = childExecutors.get(group);
        // 缓存不存在，进行 从 EventExecutorGroup 获得 EventExecutor 执行器
        if (childExecutor == null) {
            childExecutor = group.next();
            // 进行缓存
            childExecutors.put(group, childExecutor);
        }
        return childExecutor;
    }
    @Override
    public final Channel channel() {
        return channel;
    }

    @Override
    public final ChannelPipeline addFirst(String name, ChannelHandler handler) {
        return addFirst(null, name, handler);
    }

    @Override
    public final ChannelPipeline addFirst(EventExecutorGroup group, String name, ChannelHandler handler) {
        final AbstractChannelHandlerContext newCtx;
        synchronized (this) {
            checkMultiplicity(handler);
            name = filterName(name, handler);

            newCtx = newContext(group, name, handler);

            addFirst0(newCtx);

            // If the registered is false it means that the channel was not registered on an eventloop yet.
            // In this case we add the context to the pipeline and add a task that will call
            // ChannelHandler.handlerAdded(...) once the channel is registered.
            if (!registered) {
                newCtx.setAddPending();
                callHandlerCallbackLater(newCtx, true);
                return this;
            }

            EventExecutor executor = newCtx.executor();
            if (!executor.inEventLoop()) {
                newCtx.setAddPending();
                executor.execute(new Runnable() {
                    @Override
                    public void run() {
                        callHandlerAdded0(newCtx);
                    }
                });
                return this;
            }
        }
        callHandlerAdded0(newCtx);
        return this;
    }

    private void addFirst0(AbstractChannelHandlerContext newCtx) {
        AbstractChannelHandlerContext nextCtx = head.next;
        newCtx.prev = head;
        newCtx.next = nextCtx;
        head.next = newCtx;
        nextCtx.prev = newCtx;
    }

    @Override
    public final ChannelPipeline addLast(String name, ChannelHandler handler) {
        return addLast(null, name, handler);
    }

    /**
     * 添加一个 ChannelHandler 对象到 pipeline 中
     *
     * @param group    the {@link EventExecutorGroup} which will be used to execute the {@link ChannelHandler}
     *                 methods
     * @param name     the name of the handler to append
     * @param handler  the handler to append
     *
     * @return
     */
    @Override
    public final ChannelPipeline addLast(EventExecutorGroup group, String name, ChannelHandler handler) {
        final AbstractChannelHandlerContext newCtx;
        // 同步，为了防止多线程并发操作 pipeline 底层的双向链表
        synchronized (this) {
            // 检查是否有重复 handler
            checkMultiplicity(handler);

            // 创建节点名
            // 创建节点
            newCtx = newContext(group, filterName(name, handler), handler);

            // 添加节点
            addLast0(newCtx);

            // <1> pipeline 暂未注册，添加回调。再注册完成后，执行回调。详细解析，见 {@link #invokeHandlerAddedIfNeeded} 方法。
            // 这种情况，发生于 ServerBootstrap 启动的过程中。在 ServerBootstrap#init(Channel channel) 方法中，
            // 会添加 ChannelInitializer 对象到 pipeline 中，恰好此时 Channel 并未注册。
            // If the registered is false it means that the channel was not registered on an eventloop yet.
            // In this case we add the context to the pipeline and add a task that will call
            // ChannelHandler.handlerAdded(...) once the channel is registered.
            if (!registered) {
                // 设置 AbstractChannelHandlerContext 准备添加中
                newCtx.setAddPending();
                // 添加 PendingHandlerCallback 回调
                callHandlerCallbackLater(newCtx, true);
                return this;
            }

            // 不在 EventLoop 的线程中，提交 EventLoop 中，执行回调用户方法
            EventExecutor executor = newCtx.executor();
            if (!executor.inEventLoop()) {
                // 设置 AbstractChannelHandlerContext 准备添加中
                newCtx.setAddPending();
                // 提交 EventLoop 中，执行回调 ChannelHandler added 事件
                executor.execute(new Runnable() {
                    @Override
                    public void run() {
                        // 执行回调 ChannelHandler 添加完成( added )事件。
                        callHandlerAdded0(newCtx);
                    }
                });
                return this;
            }
        }

        // ChannelHandler added 事件
        // 已经确认在 EventLoop 的线程中，所以不需要在 synchronized 中。
        // 执行回调 ChannelHandler 添加完成( added )事件。
        callHandlerAdded0(newCtx);
        return this;
    }

    /**
     * 添加到最后一个节点。注意，实际上，是添加到 tail 节点之前
     * @param newCtx
     */
    private void addLast0(AbstractChannelHandlerContext newCtx) {
        // 获得 tail 节点的前一个节点
        AbstractChannelHandlerContext prev = tail.prev;
        // 新节点，指向 prev 和 tail 节点
        newCtx.prev = prev;
        newCtx.next = tail;
        // 在 prev 和 tail ，指向新节点
        prev.next = newCtx;
        tail.prev = newCtx;
    }

    @Override
    public final ChannelPipeline addBefore(String baseName, String name, ChannelHandler handler) {
        return addBefore(null, baseName, name, handler);
    }

    @Override
    public final ChannelPipeline addBefore(
            EventExecutorGroup group, String baseName, String name, ChannelHandler handler) {
        final AbstractChannelHandlerContext newCtx;
        final AbstractChannelHandlerContext ctx;
        synchronized (this) {
            checkMultiplicity(handler);
            name = filterName(name, handler);
            ctx = getContextOrDie(baseName);

            newCtx = newContext(group, name, handler);

            addBefore0(ctx, newCtx);

            // If the registered is false it means that the channel was not registered on an eventloop yet.
            // In this case we add the context to the pipeline and add a task that will call
            // ChannelHandler.handlerAdded(...) once the channel is registered.
            if (!registered) {
                newCtx.setAddPending();
                callHandlerCallbackLater(newCtx, true);
                return this;
            }

            EventExecutor executor = newCtx.executor();
            if (!executor.inEventLoop()) {
                newCtx.setAddPending();
                executor.execute(new Runnable() {
                    @Override
                    public void run() {
                        callHandlerAdded0(newCtx);
                    }
                });
                return this;
            }
        }
        callHandlerAdded0(newCtx);
        return this;
    }

    private static void addBefore0(AbstractChannelHandlerContext ctx, AbstractChannelHandlerContext newCtx) {
        newCtx.prev = ctx.prev;
        newCtx.next = ctx;
        ctx.prev.next = newCtx;
        ctx.prev = newCtx;
    }

    /**
     * 获得 ChannelHandler 的名字
     * @param name
     * @param handler
     * @return
     */
    private String filterName(String name, ChannelHandler handler) {
        // 若未传入默认的名字 name ，则调用 #generateName(ChannelHandler) 方法，根据 ChannelHandler 生成一个唯一的名字。
        if (name == null) {
            return generateName(handler);
        }
        // 若已传入默认的名字 name ，则调用 #checkDuplicateName(String name) 方法，校验名字唯一。
        checkDuplicateName(name);
        return name;
    }

    @Override
    public final ChannelPipeline addAfter(String baseName, String name, ChannelHandler handler) {
        return addAfter(null, baseName, name, handler);
    }

    @Override
    public final ChannelPipeline addAfter(
            EventExecutorGroup group, String baseName, String name, ChannelHandler handler) {
        final AbstractChannelHandlerContext newCtx;
        final AbstractChannelHandlerContext ctx;

        synchronized (this) {
            checkMultiplicity(handler);
            name = filterName(name, handler);
            ctx = getContextOrDie(baseName);

            newCtx = newContext(group, name, handler);

            addAfter0(ctx, newCtx);

            // If the registered is false it means that the channel was not registered on an eventloop yet.
            // In this case we remove the context from the pipeline and add a task that will call
            // ChannelHandler.handlerRemoved(...) once the channel is registered.
            if (!registered) {
                newCtx.setAddPending();
                callHandlerCallbackLater(newCtx, true);
                return this;
            }
            EventExecutor executor = newCtx.executor();
            if (!executor.inEventLoop()) {
                newCtx.setAddPending();
                executor.execute(new Runnable() {
                    @Override
                    public void run() {
                        callHandlerAdded0(newCtx);
                    }
                });
                return this;
            }
        }
        callHandlerAdded0(newCtx);
        return this;
    }

    private static void addAfter0(AbstractChannelHandlerContext ctx, AbstractChannelHandlerContext newCtx) {
        newCtx.prev = ctx;
        newCtx.next = ctx.next;
        ctx.next.prev = newCtx;
        ctx.next = newCtx;
    }

    public final ChannelPipeline addFirst(ChannelHandler handler) {
        return addFirst(null, handler);
    }

    @Override
    public final ChannelPipeline addFirst(ChannelHandler... handlers) {
        return addFirst(null, handlers);
    }

    @Override
    public final ChannelPipeline addFirst(EventExecutorGroup executor, ChannelHandler... handlers) {
        if (handlers == null) {
            throw new NullPointerException("handlers");
        }
        if (handlers.length == 0 || handlers[0] == null) {
            return this;
        }

        int size;
        for (size = 1; size < handlers.length; size ++) {
            if (handlers[size] == null) {
                break;
            }
        }

        for (int i = size - 1; i >= 0; i --) {
            ChannelHandler h = handlers[i];
            addFirst(executor, null, h);
        }

        return this;
    }

    public final ChannelPipeline addLast(ChannelHandler handler) {
        return addLast(null, handler);
    }

    @Override
    public final ChannelPipeline addLast(ChannelHandler... handlers) {
        return addLast(null, handlers);
    }

    /**
     * 添加任意数量的 ChannelHandler 对象
     * @param executor
     * @param handlers  the handlers to insert last
     * @return
     */
    @Override
    public final ChannelPipeline addLast(EventExecutorGroup executor, ChannelHandler... handlers) {
        if (handlers == null) {
            throw new NullPointerException("handlers");
        }

        for (ChannelHandler h: handlers) {
            if (h == null) {
                break;
            }
            addLast(executor, null, h);
        }

        return this;
    }

    /**
     * 根据 ChannelHandler 生成一个唯一名字
     * @param handler
     * @return
     */
    private String generateName(ChannelHandler handler) {
        // 从缓存中查询，是否已经生成默认名字
        Map<Class<?>, String> cache = nameCaches.get();
        Class<?> handlerType = handler.getClass();
        String name = cache.get(handlerType);
        // 若未生成过，进行生成
        if (name == null) {
            name = generateName0(handlerType);
            cache.put(handlerType, name);
        }

        // 判断是否存在相同名字的节点
        // It's not very likely for a user to put more than one handler of the same type, but make sure to avoid
        // any name conflicts.  Note that we don't cache the names generated here.
        if (context0(name) != null) {
            // 若存在，则使用基础名字 + 编号，循环生成，直到一个是唯一的
            String baseName = name.substring(0, name.length() - 1); // Strip the trailing '0'.
            for (int i = 1;; i ++) {
                String newName = baseName + i;
                // 判断是否存在相同名字的节点
                if (context0(newName) == null) {
                    name = newName;
                    break;
                }
            }
        }
        return name;
    }

    /**
     * 即 HEAD_NAME = "HeadContext#0"，TAIL_NAME= "TailContext#0" 。
     * @param handlerType
     * @return
     */
    private static String generateName0(Class<?> handlerType) {
        return StringUtil.simpleClassName(handlerType) + "#0";
    }

    /**
     * 从 pipeline 移除指定的 ChannelHandler 对象
     *
     * @param  handler          the {@link ChannelHandler} to remove
     *
     * @return
     */
    @Override
    public final ChannelPipeline remove(ChannelHandler handler) {
        // 调用 #getContextOrDie(ChannelHandler handler) 方法，获得对应的 AbstractChannelHandlerContext 节点
        // 方法使用 Die 的原因是，获得不到节点的情况下，抛出 NoSuchElementException 异常。
        remove(getContextOrDie(handler));
        return this;
    }

    @Override
    public final ChannelHandler remove(String name) {
        return remove(getContextOrDie(name)).handler();
    }

    @SuppressWarnings("unchecked")
    @Override
    public final <T extends ChannelHandler> T remove(Class<T> handlerType) {
        return (T) remove(getContextOrDie(handlerType)).handler();
    }

    public final <T extends ChannelHandler> T removeIfExists(String name) {
        return removeIfExists(context(name));
    }

    public final <T extends ChannelHandler> T removeIfExists(Class<T> handlerType) {
        return removeIfExists(context(handlerType));
    }

    public final <T extends ChannelHandler> T removeIfExists(ChannelHandler handler) {
        return removeIfExists(context(handler));
    }

    @SuppressWarnings("unchecked")
    private <T extends ChannelHandler> T removeIfExists(ChannelHandlerContext ctx) {
        if (ctx == null) {
            return null;
        }
        return (T) remove((AbstractChannelHandlerContext) ctx).handler();
    }

    /**
     * 移除指定 AbstractChannelHandlerContext 节点
     * @param ctx
     * @return
     */
    private AbstractChannelHandlerContext remove(final AbstractChannelHandlerContext ctx) {
        assert ctx != head && ctx != tail;

        // 同步，为了防止多线程并发操作 pipeline 底层的双向链表
        synchronized (this) {
            // 从 pipeline 移除指定的 AbstractChannelHandlerContext 节点。
            remove0(ctx);

            // If the registered is false it means that the channel was not registered on an eventloop yet.
            // In this case we remove the context from the pipeline and add a task that will call
            // ChannelHandler.handlerRemoved(...) once the channel is registered.
            // pipeline 暂未注册，添加回调。在注册完成后，执行回调。详细解析，见 {@link #callHandlerCallbackLater} 方法。
            if (!registered) {
                // 添加 PendingHandlerRemovedTask 回调。在 Channel 注册完成后，执行该回调。
                callHandlerCallbackLater(ctx, false);
                return ctx;
            }

            EventExecutor executor = ctx.executor();
            // 不在 EventLoop 的线程中，提交 EventLoop 中，执行回调用户方法
            if (!executor.inEventLoop()) {
                // 提交 EventLoop 中，执行回调 ChannelHandler removed 事件
                executor.execute(new Runnable() {
                    @Override
                    public void run() {
                        // 执行回调 ChannelHandler 移除完成( removed )事件。
                        callHandlerRemoved0(ctx);
                    }
                });
                return ctx;
            }
        }
        // 回调 ChannelHandler removed 事件, 执行回调 ChannelHandler [移除完成( removed )]事件。
        // 已经确认在 EventLoop 的线程中，所以不需要在 synchronized 中。
        callHandlerRemoved0(ctx);
        return ctx;
    }

    /**
     * 从 pipeline 移除指定的 AbstractChannelHandlerContext 节点
     * @param ctx
     */
    private static void remove0(AbstractChannelHandlerContext ctx) {
        // 获得移除节点的前后节点
        AbstractChannelHandlerContext prev = ctx.prev;
        AbstractChannelHandlerContext next = ctx.next;
        // 前后节点互相指向
        prev.next = next;
        next.prev = prev;
    }

    @Override
    public final ChannelHandler removeFirst() {
        if (head.next == tail) {
            throw new NoSuchElementException();
        }
        return remove(head.next).handler();
    }

    @Override
    public final ChannelHandler removeLast() {
        if (head.next == tail) {
            throw new NoSuchElementException();
        }
        return remove(tail.prev).handler();
    }

    @Override
    public final ChannelPipeline replace(ChannelHandler oldHandler, String newName, ChannelHandler newHandler) {
        replace(getContextOrDie(oldHandler), newName, newHandler);
        return this;
    }

    @Override
    public final ChannelHandler replace(String oldName, String newName, ChannelHandler newHandler) {
        return replace(getContextOrDie(oldName), newName, newHandler);
    }

    @Override
    @SuppressWarnings("unchecked")
    public final <T extends ChannelHandler> T replace(
            Class<T> oldHandlerType, String newName, ChannelHandler newHandler) {
        return (T) replace(getContextOrDie(oldHandlerType), newName, newHandler);
    }

    private ChannelHandler replace(
            final AbstractChannelHandlerContext ctx, String newName, ChannelHandler newHandler) {
        assert ctx != head && ctx != tail;

        final AbstractChannelHandlerContext newCtx;
        synchronized (this) {
            checkMultiplicity(newHandler);
            if (newName == null) {
                newName = generateName(newHandler);
            } else {
                boolean sameName = ctx.name().equals(newName);
                if (!sameName) {
                    checkDuplicateName(newName);
                }
            }

            newCtx = newContext(ctx.executor, newName, newHandler);

            replace0(ctx, newCtx);

            // If the registered is false it means that the channel was not registered on an eventloop yet.
            // In this case we replace the context in the pipeline
            // and add a task that will call ChannelHandler.handlerAdded(...) and
            // ChannelHandler.handlerRemoved(...) once the channel is registered.
            if (!registered) {
                callHandlerCallbackLater(newCtx, true);
                callHandlerCallbackLater(ctx, false);
                return ctx.handler();
            }
            EventExecutor executor = ctx.executor();
            if (!executor.inEventLoop()) {
                executor.execute(new Runnable() {
                    @Override
                    public void run() {
                        // Invoke newHandler.handlerAdded() first (i.e. before oldHandler.handlerRemoved() is invoked)
                        // because callHandlerRemoved() will trigger channelRead() or flush() on newHandler and
                        // those event handlers must be called after handlerAdded().
                        callHandlerAdded0(newCtx);
                        callHandlerRemoved0(ctx);
                    }
                });
                return ctx.handler();
            }
        }
        // Invoke newHandler.handlerAdded() first (i.e. before oldHandler.handlerRemoved() is invoked)
        // because callHandlerRemoved() will trigger channelRead() or flush() on newHandler and those
        // event handlers must be called after handlerAdded().
        callHandlerAdded0(newCtx);
        callHandlerRemoved0(ctx);
        return ctx.handler();
    }

    private static void replace0(AbstractChannelHandlerContext oldCtx, AbstractChannelHandlerContext newCtx) {
        AbstractChannelHandlerContext prev = oldCtx.prev;
        AbstractChannelHandlerContext next = oldCtx.next;
        newCtx.prev = prev;
        newCtx.next = next;

        // Finish the replacement of oldCtx with newCtx in the linked list.
        // Note that this doesn't mean events will be sent to the new handler immediately
        // because we are currently at the event handler thread and no more than one handler methods can be invoked
        // at the same time (we ensured that in replace().)
        prev.next = newCtx;
        next.prev = newCtx;

        // update the reference to the replacement so forward of buffered content will work correctly
        oldCtx.prev = newCtx;
        oldCtx.next = newCtx;
    }

    /**
     * 校验是否重复的 ChannelHandler
     * @param handler
     */
    private static void checkMultiplicity(ChannelHandler handler) {
        if (handler instanceof ChannelHandlerAdapter) {
            ChannelHandlerAdapter h = (ChannelHandlerAdapter) handler;
            // 若已经添加，并且未使用 @Sharable 注解，则抛出异常
            // 在 pipeline 中，一个创建的 ChannelHandler 对象，如果不使用 Netty @Sharable 注解，
            // 则只能添加 Channel 到一个pipeline 中。所以，如果我们想要重用一个 ChannelHandler 对象( 例如在 Spring 环境中 )，
            // 则必须给这个 ChannelHandler 添加 @Sharable 注解。
            if (!h.isSharable() && h.added) {
                throw new ChannelPipelineException(
                        h.getClass().getName() +
                        " is not a @Sharable handler, so can't be added or removed multiple times.");
            }
            // 标记已经添加
            h.added = true;
        }
    }

    /**
     * 执行回调 ChannelHandler 添加完成( added )事件
     * @param ctx
     */
    private void callHandlerAdded0(final AbstractChannelHandlerContext ctx) {
        try {
            // We must call setAddComplete before calling handlerAdded. Otherwise if the handlerAdded method generates
            // any pipeline events ctx.handler() will miss them because the state will not allow it.
            // 设置 AbstractChannelHandlerContext 已添加
            ctx.setAddComplete();
            // 回调 ChannelHandler 添加完成( added )事件
            // 一般来说，通过这个方法，来初始化 ChannelHandler 。
            // 注意，因为这个方法的执行在 EventLoop 的线程中，所以要尽量避免执行时间过长。
            ctx.handler().handlerAdded(ctx);
        } catch (Throwable t) {
            // 发生异常，移除该节点
            boolean removed = false;
            try {
                // 移除
                remove0(ctx);
                try {
                    // 回调 ChannelHandler 移除完成( removed )事件
                    ctx.handler().handlerRemoved(ctx);
                } finally {
                    // 标记节点已移除
                    ctx.setRemoved();
                }
                // 标记移除成功
                removed = true;
            } catch (Throwable t2) {
                if (logger.isWarnEnabled()) {
                    logger.warn("Failed to remove a handler: " + ctx.name(), t2);
                }
            }

            // 触发异常的传播
            if (removed) {
                fireExceptionCaught(new ChannelPipelineException(
                        ctx.handler().getClass().getName() +
                        ".handlerAdded() has thrown an exception; removed.", t));
            } else {
                fireExceptionCaught(new ChannelPipelineException(
                        ctx.handler().getClass().getName() +
                        ".handlerAdded() has thrown an exception; also failed to remove.", t));
            }
        }
    }

    /**
     * 执行回调 ChannelHandler [移除完成( removed )]事件
     * @param ctx
     */
    private void callHandlerRemoved0(final AbstractChannelHandlerContext ctx) {
        // Notify the complete removal.
        try {
            try {
                // 回调 ChannelHandler 移除完成( removed )事件
                // 一般来说，通过这个方法，来释放 ChannelHandler 占用的资源。
                // 注意，因为这个方法的执行在 EventLoop 的线程中，所以要尽量避免执行时间过长。
                ctx.handler().handlerRemoved(ctx);
            } finally {
                // 设置 AbstractChannelHandlerContext 已移除
                ctx.setRemoved();
            }
        } catch (Throwable t) {
            // 触发异常的传播
            fireExceptionCaught(new ChannelPipelineException(
                    ctx.handler().getClass().getName() + ".handlerRemoved() has thrown an exception.", t));
        }
    }

    /**
     * 执行在 PendingHandlerCallback 中的 ChannelHandler 添加完成( added )事件
     * 它被两个方法所调用：
     * + AbstractUnsafe#register0(ChannelPromise promise) 方法
     * + HeadContext#channelRegistered(ChannelHandlerContext ctx) 方法。
     */
    final void invokeHandlerAddedIfNeeded() {
        // 必须在 EventLoop 的线程中
        assert channel.eventLoop().inEventLoop();
        // 仅有首次注册有效
        if (firstRegistration) {
            // 标记非首次注册
            firstRegistration = false;
            // We are now registered to the EventLoop. It's time to call the callbacks for the ChannelHandlers,
            // that were added before the registration was done.
            // 执行在 PendingHandlerCallback 中的 ChannelHandler [添加完成( added )]事件
            callHandlerAddedForAllHandlers();
        }
    }

    @Override
    public final ChannelHandler first() {
        ChannelHandlerContext first = firstContext();
        if (first == null) {
            return null;
        }
        return first.handler();
    }

    @Override
    public final ChannelHandlerContext firstContext() {
        AbstractChannelHandlerContext first = head.next;
        if (first == tail) {
            return null;
        }
        return head.next;
    }

    @Override
    public final ChannelHandler last() {
        AbstractChannelHandlerContext last = tail.prev;
        if (last == head) {
            return null;
        }
        return last.handler();
    }

    @Override
    public final ChannelHandlerContext lastContext() {
        AbstractChannelHandlerContext last = tail.prev;
        if (last == head) {
            return null;
        }
        return last;
    }

    @Override
    public final ChannelHandler get(String name) {
        ChannelHandlerContext ctx = context(name);
        if (ctx == null) {
            return null;
        } else {
            return ctx.handler();
        }
    }

    @SuppressWarnings("unchecked")
    @Override
    public final <T extends ChannelHandler> T get(Class<T> handlerType) {
        ChannelHandlerContext ctx = context(handlerType);
        if (ctx == null) {
            return null;
        } else {
            return (T) ctx.handler();
        }
    }

    @Override
    public final ChannelHandlerContext context(String name) {
        if (name == null) {
            throw new NullPointerException("name");
        }

        return context0(name);
    }

    @Override
    public final ChannelHandlerContext context(ChannelHandler handler) {
        if (handler == null) {
            throw new NullPointerException("handler");
        }

        AbstractChannelHandlerContext ctx = head.next;
        // 循环，获得指定 ChannelHandler 对象的节点
        for (;;) {

            if (ctx == null) {
                return null;
            }

            if (ctx.handler() == handler) { // ChannelHandler 相等
                return ctx;
            }

            ctx = ctx.next;
        }
    }

    @Override
    public final ChannelHandlerContext context(Class<? extends ChannelHandler> handlerType) {
        if (handlerType == null) {
            throw new NullPointerException("handlerType");
        }

        AbstractChannelHandlerContext ctx = head.next;
        for (;;) {
            if (ctx == null) {
                return null;
            }
            if (handlerType.isAssignableFrom(ctx.handler().getClass())) {
                return ctx;
            }
            ctx = ctx.next;
        }
    }

    @Override
    public final List<String> names() {
        List<String> list = new ArrayList<String>();
        AbstractChannelHandlerContext ctx = head.next;
        for (;;) {
            if (ctx == null) {
                return list;
            }
            list.add(ctx.name());
            ctx = ctx.next;
        }
    }

    @Override
    public final Map<String, ChannelHandler> toMap() {
        Map<String, ChannelHandler> map = new LinkedHashMap<String, ChannelHandler>();
        AbstractChannelHandlerContext ctx = head.next;
        for (;;) {
            if (ctx == tail) {
                return map;
            }
            map.put(ctx.name(), ctx.handler());
            ctx = ctx.next;
        }
    }

    @Override
    public final Iterator<Map.Entry<String, ChannelHandler>> iterator() {
        return toMap().entrySet().iterator();
    }

    /**
     * Returns the {@link String} representation of this pipeline.
     */
    @Override
    public final String toString() {
        StringBuilder buf = new StringBuilder()
            .append(StringUtil.simpleClassName(this))
            .append('{');
        AbstractChannelHandlerContext ctx = head.next;
        for (;;) {
            if (ctx == tail) {
                break;
            }

            buf.append('(')
               .append(ctx.name())
               .append(" = ")
               .append(ctx.handler().getClass().getName())
               .append(')');

            ctx = ctx.next;
            if (ctx == tail) {
                break;
            }

            buf.append(", ");
        }
        buf.append('}');
        return buf.toString();
    }

    @Override
    public final ChannelPipeline fireChannelRegistered() {
        AbstractChannelHandlerContext.invokeChannelRegistered(head);
        return this;
    }

    @Override
    public final ChannelPipeline fireChannelUnregistered() {
        AbstractChannelHandlerContext.invokeChannelUnregistered(head);
        return this;
    }

    /**
     * Removes all handlers from the pipeline one by one from tail (exclusive) to head (exclusive) to trigger
     * handlerRemoved().
     *
     * Note that we traverse up the pipeline ({@link #destroyUp(AbstractChannelHandlerContext, boolean)})
     * before traversing down ({@link #destroyDown(Thread, AbstractChannelHandlerContext, boolean)}) so that
     * the handlers are removed after all events are handled.
     *
     * See: https://github.com/netty/netty/issues/3156
     */
    private synchronized void destroy() {
        destroyUp(head.next, false);
    }

    private void destroyUp(AbstractChannelHandlerContext ctx, boolean inEventLoop) {
        final Thread currentThread = Thread.currentThread();
        final AbstractChannelHandlerContext tail = this.tail;
        for (;;) {
            if (ctx == tail) {
                destroyDown(currentThread, tail.prev, inEventLoop);
                break;
            }

            final EventExecutor executor = ctx.executor();
            if (!inEventLoop && !executor.inEventLoop(currentThread)) {
                final AbstractChannelHandlerContext finalCtx = ctx;
                executor.execute(new Runnable() {
                    @Override
                    public void run() {
                        destroyUp(finalCtx, true);
                    }
                });
                break;
            }

            ctx = ctx.next;
            inEventLoop = false;
        }
    }

    private void destroyDown(Thread currentThread, AbstractChannelHandlerContext ctx, boolean inEventLoop) {
        // We have reached at tail; now traverse backwards.
        final AbstractChannelHandlerContext head = this.head;
        for (;;) {
            if (ctx == head) {
                break;
            }

            final EventExecutor executor = ctx.executor();
            if (inEventLoop || executor.inEventLoop(currentThread)) {
                synchronized (this) {
                    remove0(ctx);
                }
                callHandlerRemoved0(ctx);
            } else {
                final AbstractChannelHandlerContext finalCtx = ctx;
                executor.execute(new Runnable() {
                    @Override
                    public void run() {
                        destroyDown(Thread.currentThread(), finalCtx, true);
                    }
                });
                break;
            }

            ctx = ctx.prev;
            inEventLoop = false;
        }
    }

    /**
     * 对于 ChannelInboundInvoker 接口方法的实现，
     * ChannelPipeline 对它的实现，会调用 AbstractChannelHandlerContext 的对应方法
     * @return
     */
    @Override
    public final ChannelPipeline fireChannelActive() {
        // 方法参数是 head, inbound 时间从 head 开始传递
        AbstractChannelHandlerContext.invokeChannelActive(head);
        return this;
    }

    @Override
    public final ChannelPipeline fireChannelInactive() {
        AbstractChannelHandlerContext.invokeChannelInactive(head);
        return this;
    }

    @Override
    public final ChannelPipeline fireExceptionCaught(Throwable cause) {
        AbstractChannelHandlerContext.invokeExceptionCaught(head, cause);
        return this;
    }

    @Override
    public final ChannelPipeline fireUserEventTriggered(Object event) {
        AbstractChannelHandlerContext.invokeUserEventTriggered(head, event);
        return this;
    }

    @Override
    public final ChannelPipeline fireChannelRead(Object msg) {
        AbstractChannelHandlerContext.invokeChannelRead(head, msg);
        return this;
    }

    @Override
    public final ChannelPipeline fireChannelReadComplete() {
        AbstractChannelHandlerContext.invokeChannelReadComplete(head);
        return this;
    }

    @Override
    public final ChannelPipeline fireChannelWritabilityChanged() {
        AbstractChannelHandlerContext.invokeChannelWritabilityChanged(head);
        return this;
    }

    @Override
    public final ChannelFuture bind(SocketAddress localAddress) {
        return tail.bind(localAddress);
    }

    @Override
    public final ChannelFuture connect(SocketAddress remoteAddress) {
        return tail.connect(remoteAddress);
    }

    @Override
    public final ChannelFuture connect(SocketAddress remoteAddress, SocketAddress localAddress) {
        return tail.connect(remoteAddress, localAddress);
    }

    @Override
    public final ChannelFuture disconnect() {
        return tail.disconnect();
    }

    @Override
    public final ChannelFuture close() {
        return tail.close();
    }

    @Override
    public final ChannelFuture deregister() {
        return tail.deregister();
    }

    @Override
    public final ChannelPipeline flush() {
        tail.flush();
        return this;
    }

    /**
     * 对于 ChannelOutboundInvoker 接口方法的实现，
     * ChannelPipeline 对它的实现，会调用 AbstractChannelHandlerContext 的对应方法
     * @param localAddress
     * @param promise
     * @return
     */
    @Override
    public final ChannelFuture bind(SocketAddress localAddress, ChannelPromise promise) {
        return tail.bind(localAddress, promise);
    }

    @Override
    public final ChannelFuture connect(SocketAddress remoteAddress, ChannelPromise promise) {
        return tail.connect(remoteAddress, promise);
    }

    @Override
    public final ChannelFuture connect(
            SocketAddress remoteAddress, SocketAddress localAddress, ChannelPromise promise) {
        return tail.connect(remoteAddress, localAddress, promise);
    }

    @Override
    public final ChannelFuture disconnect(ChannelPromise promise) {
        return tail.disconnect(promise);
    }

    @Override
    public final ChannelFuture close(ChannelPromise promise) {
        return tail.close(promise);
    }

    @Override
    public final ChannelFuture deregister(final ChannelPromise promise) {
        return tail.deregister(promise);
    }

    @Override
    public final ChannelPipeline read() {
        tail.read();
        return this;
    }

    @Override
    public final ChannelFuture write(Object msg) {
        return tail.write(msg);
    }

    @Override
    public final ChannelFuture write(Object msg, ChannelPromise promise) {
        return tail.write(msg, promise);
    }

    @Override
    public final ChannelFuture writeAndFlush(Object msg, ChannelPromise promise) {
        return tail.writeAndFlush(msg, promise);
    }

    @Override
    public final ChannelFuture writeAndFlush(Object msg) {
        return tail.writeAndFlush(msg);
    }

    @Override
    public final ChannelPromise newPromise() {
        return new DefaultChannelPromise(channel);
    }

    @Override
    public final ChannelProgressivePromise newProgressivePromise() {
        return new DefaultChannelProgressivePromise(channel);
    }

    @Override
    public final ChannelFuture newSucceededFuture() {
        return succeededFuture;
    }

    @Override
    public final ChannelFuture newFailedFuture(Throwable cause) {
        return new FailedChannelFuture(channel, null, cause);
    }

    @Override
    public final ChannelPromise voidPromise() {
        return voidPromise;
    }

    /**
     * 校验名字唯一
     * @param name
     */
    private void checkDuplicateName(String name) {
        // 获得指定名字的节点。若存在节点，意味着不唯一
        if (context0(name) != null) {
            throw new IllegalArgumentException("Duplicate handler name: " + name);
        }
    }

    /**
     * 判断是否存在相同名字的节点
     * @param name
     * @return
     */
    private AbstractChannelHandlerContext context0(String name) {
        AbstractChannelHandlerContext context = head.next;
        // 顺序向下遍历节点，判断是否有指定名字的节点。如果有，则返回该节点。
        while (context != tail) {
            if (context.name().equals(name)) {
                return context;
            }
            context = context.next;
        }
        return null;
    }

    private AbstractChannelHandlerContext getContextOrDie(String name) {
        AbstractChannelHandlerContext ctx = (AbstractChannelHandlerContext) context(name);
        if (ctx == null) {
            throw new NoSuchElementException(name);
        } else {
            return ctx;
        }
    }

    private AbstractChannelHandlerContext getContextOrDie(ChannelHandler handler) {
        AbstractChannelHandlerContext ctx = (AbstractChannelHandlerContext) context(handler);
        if (ctx == null) {
            // die
            throw new NoSuchElementException(handler.getClass().getName());
        } else {
            return ctx;
        }
    }

    private AbstractChannelHandlerContext getContextOrDie(Class<? extends ChannelHandler> handlerType) {
        AbstractChannelHandlerContext ctx = (AbstractChannelHandlerContext) context(handlerType);
        if (ctx == null) {
            throw new NoSuchElementException(handlerType.getName());
        } else {
            return ctx;
        }
    }

    /**
     * 执行在 PendingHandlerCallback 中的 ChannelHandler [添加完成( added )]事件
     */
    private void callHandlerAddedForAllHandlers() {
        final PendingHandlerCallback pendingHandlerCallbackHead;
        // 获得 pendingHandlerCallbackHead
        synchronized (this) {
            assert !registered;

            // This Channel itself was registered.
            // 标记已注册
            registered = true;

            pendingHandlerCallbackHead = this.pendingHandlerCallbackHead;
            // Null out so it can be GC'ed.
            // 置空，help gc
            this.pendingHandlerCallbackHead = null;
        }

        // This must happen outside of the synchronized(...) block as otherwise handlerAdded(...) may be called while
        // holding the lock and so produce a deadlock if handlerAdded(...) will try to add another handler from outside
        // the EventLoop.
        // 顺序向下，执行 PendingHandlerCallback 的回调, 从而将 ChannelHandler 添加到 pipeline 中。
        PendingHandlerCallback task = pendingHandlerCallbackHead;
        while (task != null) {
            task.execute();
            task = task.next;
        }
    }

    /**
     * 添加 PendingHandlerCallback 回调
     * @param ctx
     * @param added
     */
    private void callHandlerCallbackLater(AbstractChannelHandlerContext ctx, boolean added) {
        assert !registered;

        // 创建 PendingHandlerCallback 对象
        PendingHandlerCallback task = added ? new PendingHandlerAddedTask(ctx) : new PendingHandlerRemovedTask(ctx);
        PendingHandlerCallback pending = pendingHandlerCallbackHead;
        // 若原 pendingHandlerCallbackHead 不存在，则赋值给它
        if (pending == null) {
            pendingHandlerCallbackHead = task;
        } else {
            // 若原 pendingHandlerCallbackHead 已存在，则最后一个回调指向新创建的回调
            // Find the tail of the linked-list.
            while (pending.next != null) {
                pending = pending.next;
            }
            pending.next = task;
        }
    }

    /**
     * Called once a {@link Throwable} hit the end of the {@link ChannelPipeline} without been handled by the user
     * in {@link ChannelHandler#exceptionCaught(ChannelHandlerContext, Throwable)}.
     */
    protected void onUnhandledInboundException(Throwable cause) {
        try {
            // 打印告警日志
            logger.warn(
                    "An exceptionCaught() event was fired, and it reached at the tail of the pipeline. " +
                            "It usually means the last handler in the pipeline did not handle the exception.",
                    cause);
        } finally {
            // 释放需要释放的资源。
            ReferenceCountUtil.release(cause);
        }
    }

    /**
     * Called once the {@link ChannelInboundHandler#channelActive(ChannelHandlerContext)}event hit
     * the end of the {@link ChannelPipeline}.
     */
    protected void onUnhandledInboundChannelActive() {
    }

    /**
     * Called once the {@link ChannelInboundHandler#channelInactive(ChannelHandlerContext)} event hit
     * the end of the {@link ChannelPipeline}.
     */
    protected void onUnhandledInboundChannelInactive() {
    }

    /**
     * Called once a message hit the end of the {@link ChannelPipeline} without been handled by the user
     * in {@link ChannelInboundHandler#channelRead(ChannelHandlerContext, Object)}. This method is responsible
     * to call {@link ReferenceCountUtil#release(Object)} on the given msg at some point.
     */
    protected void onUnhandledInboundMessage(Object msg) {
        try {
            logger.debug(
                    "Discarded inbound message {} that reached at the tail of the pipeline. " +
                            "Please check your pipeline configuration.", msg);
        } finally {
            ReferenceCountUtil.release(msg);
        }
    }

    /**
     * Called once the {@link ChannelInboundHandler#channelReadComplete(ChannelHandlerContext)} event hit
     * the end of the {@link ChannelPipeline}.
     */
    protected void onUnhandledInboundChannelReadComplete() {
    }

    /**
     * Called once an user event hit the end of the {@link ChannelPipeline} without been handled by the user
     * in {@link ChannelInboundHandler#userEventTriggered(ChannelHandlerContext, Object)}. This method is responsible
     * to call {@link ReferenceCountUtil#release(Object)} on the given event at some point.
     */
    protected void onUnhandledInboundUserEventTriggered(Object evt) {
        // This may not be a configuration error and so don't log anything.
        // The event may be superfluous for the current pipeline configuration.
        ReferenceCountUtil.release(evt);
    }

    /**
     * Called once the {@link ChannelInboundHandler#channelWritabilityChanged(ChannelHandlerContext)} event hit
     * the end of the {@link ChannelPipeline}.
     */
    protected void onUnhandledChannelWritabilityChanged() {
    }

    @UnstableApi
    protected void incrementPendingOutboundBytes(long size) {
        ChannelOutboundBuffer buffer = channel.unsafe().outboundBuffer();
        if (buffer != null) {
            buffer.incrementPendingOutboundBytes(size);
        }
    }

    @UnstableApi
    protected void decrementPendingOutboundBytes(long size) {
        ChannelOutboundBuffer buffer = channel.unsafe().outboundBuffer();
        if (buffer != null) {
            buffer.decrementPendingOutboundBytes(size);
        }
    }

    /**
     * pipe 尾节点 Context 实现类
     */
    // A special catch-all handler that handles both bytes and messages.
    final class TailContext extends AbstractChannelHandlerContext implements ChannelInboundHandler {

        TailContext(DefaultChannelPipeline pipeline) {
            // 调用父 AbstractChannelHandlerContext 的构造方法，
            // 设置 inbound = true、outbound = false ，和 HeadContext 相反。
            super(pipeline, null, TAIL_NAME, true, false);
            // 设置 ChannelHandler 添加完成。此时，handlerStatus 会变成 ADD_COMPLETE 状态。
            setAddComplete();
        }

        /**
         * 返回自己作为 Context 的 ChannelHandler
         * 因为 TailContext ，实现 ChannelInboundHandler 接口，而它们本身就是 ChannelHandler 。
         * @return
         */
        @Override
        public ChannelHandler handler() {
            return this;
        }

        @Override
        public void channelRegistered(ChannelHandlerContext ctx) throws Exception { }

        @Override
        public void channelUnregistered(ChannelHandlerContext ctx) throws Exception { }

        @Override
        public void channelActive(ChannelHandlerContext ctx) throws Exception {
            onUnhandledInboundChannelActive();
        }

        @Override
        public void channelInactive(ChannelHandlerContext ctx) throws Exception {
            onUnhandledInboundChannelInactive();
        }

        @Override
        public void channelWritabilityChanged(ChannelHandlerContext ctx) throws Exception {
            onUnhandledChannelWritabilityChanged();
        }

        @Override
        public void handlerAdded(ChannelHandlerContext ctx) throws Exception { }

        @Override
        public void handlerRemoved(ChannelHandlerContext ctx) throws Exception { }

        @Override
        public void userEventTriggered(ChannelHandlerContext ctx, Object evt) throws Exception {
            onUnhandledInboundUserEventTriggered(evt);
        }

        @Override
        public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) throws Exception {
            onUnhandledInboundException(cause);
        }

        @Override
        public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {
            onUnhandledInboundMessage(msg);
        }

        @Override
        public void channelReadComplete(ChannelHandlerContext ctx) throws Exception {
            onUnhandledInboundChannelReadComplete();
        }
    }

    /**
     * pipe 头节点 Context 实现类。
     */
    final class HeadContext extends AbstractChannelHandlerContext
            implements ChannelOutboundHandler, ChannelInboundHandler {

        private final Unsafe unsafe;

        HeadContext(DefaultChannelPipeline pipeline) {
            // 调用父 AbstractChannelHandlerContext 的构造方法，设置 inbound = false、outbound = true 。
            super(pipeline, null, HEAD_NAME, false, true);
            // 使用 Channel 的 Unsafe 作为 unsafe 属性。
            // HeadContext 实现 ChannelOutboundHandler 接口的方法，都会调用 Unsafe 对应的方法。
            unsafe = pipeline.channel().unsafe();
            // 设置 ChannelHandler 添加完成。此时，handlerStatus 会变成 ADD_COMPLETE 状态
            setAddComplete();
        }

        /**
         * 返回自己作为 Context 的 ChannelHandler
         * 因为 HeadContext ，实现 ChannelOutboundHandler、ChannelInboundHandler 接口，而它们本身就是 ChannelHandler 。
         * @return
         */
        @Override
        public ChannelHandler handler() {
            return this;
        }

        @Override
        public void handlerAdded(ChannelHandlerContext ctx) throws Exception {
            // NOOP
        }

        @Override
        public void handlerRemoved(ChannelHandlerContext ctx) throws Exception {
            // NOOP
        }

        @Override
        public void bind(
                ChannelHandlerContext ctx, SocketAddress localAddress, ChannelPromise promise)
                throws Exception {
            unsafe.bind(localAddress, promise);
        }

        @Override
        public void connect(
                ChannelHandlerContext ctx,
                SocketAddress remoteAddress, SocketAddress localAddress,
                ChannelPromise promise) throws Exception {
            unsafe.connect(remoteAddress, localAddress, promise);
        }

        @Override
        public void disconnect(ChannelHandlerContext ctx, ChannelPromise promise) throws Exception {
            unsafe.disconnect(promise);
        }

        @Override
        public void close(ChannelHandlerContext ctx, ChannelPromise promise) throws Exception {
            unsafe.close(promise);
        }

        @Override
        public void deregister(ChannelHandlerContext ctx, ChannelPromise promise) throws Exception {
            unsafe.deregister(promise);
        }

        @Override
        public void read(ChannelHandlerContext ctx) {
            unsafe.beginRead();
        }

        @Override
        public void write(ChannelHandlerContext ctx, Object msg, ChannelPromise promise) throws Exception {
            unsafe.write(msg, promise);
        }

        @Override
        public void flush(ChannelHandlerContext ctx) throws Exception {
            unsafe.flush();
        }

        @Override
        public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) throws Exception {
            ctx.fireExceptionCaught(cause);
        }

        @Override
        public void channelRegistered(ChannelHandlerContext ctx) throws Exception {
            invokeHandlerAddedIfNeeded();
            ctx.fireChannelRegistered();
        }

        @Override
        public void channelUnregistered(ChannelHandlerContext ctx) throws Exception {
            ctx.fireChannelUnregistered();

            // Remove all handlers sequentially if channel is closed and unregistered.
            if (!channel.isOpen()) {
                destroy();
            }
        }

        @Override
        public void channelActive(ChannelHandlerContext ctx) throws Exception {
            // 传播 Channel active 事件给下一个 Inbound 节点
            ctx.fireChannelActive();

            // 执行 read 逻辑
            readIfIsAutoRead();
        }

        @Override
        public void channelInactive(ChannelHandlerContext ctx) throws Exception {
            ctx.fireChannelInactive();
        }

        @Override
        public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {
            ctx.fireChannelRead(msg);
        }

        @Override
        public void channelReadComplete(ChannelHandlerContext ctx) throws Exception {
            ctx.fireChannelReadComplete();

            readIfIsAutoRead();
        }

        private void readIfIsAutoRead() {
            if (channel.config().isAutoRead()) {
                channel.read();
            }
        }

        @Override
        public void userEventTriggered(ChannelHandlerContext ctx, Object evt) throws Exception {
            ctx.fireUserEventTriggered(evt);
        }

        @Override
        public void channelWritabilityChanged(ChannelHandlerContext ctx) throws Exception {
            ctx.fireChannelWritabilityChanged();
        }
    }

    /**
     * 实现 Runnable 接口，等待添加 ChannelHandler 回调抽象类。
     *
     * 为什么会有 PendingHandlerCallback 呢？
     * 因为 ChannelHandler 添加到 pipeline 中，会触发 ChannelHandler 的添加完成( added )事件，
     * 并且该事件需要在 Channel 所属的 EventLoop 中执行。但是 Channel 并未注册在 EventLoop 上时，
     * 需要暂时将“触发 ChannelHandler 的添加完成( added )事件”的逻辑，
     * 作为一个 PendingHandlerCallback 进行“缓存”。在 Channel 注册到 EventLoop 上时，进行回调执行。
     */
    private abstract static class PendingHandlerCallback implements Runnable {
        /**
         * AbstractChannelHandlerContext 节点
         */
        final AbstractChannelHandlerContext ctx;
        /**
         * 下一个回调 PendingHandlerCallback 对象
         * 通过 ctx 和 next 字段，形成回调链。
         */
        PendingHandlerCallback next;

        PendingHandlerCallback(AbstractChannelHandlerContext ctx) {
            this.ctx = ctx;
        }

        abstract void execute();
    }

    /**
     * 回调添加 ChannelHandler 节点
     */
    private final class PendingHandlerAddedTask extends PendingHandlerCallback {

        PendingHandlerAddedTask(AbstractChannelHandlerContext ctx) {
            super(ctx);
        }

        @Override
        public void run() {
            callHandlerAdded0(ctx);
        }

        @Override
        void execute() {
            EventExecutor executor = ctx.executor();
            // 在 EventLoop 的线程中，回调 ChannelHandler added 事件
            if (executor.inEventLoop()) {
                callHandlerAdded0(ctx);
            } else {
                // 提交 EventLoop 中，执行回调 ChannelHandler added 事件
                try {
                    //为什么 PendingHandlerAddedTask 可以直接提交到 EventLoop 中呢？
                    // 因为 PendingHandlerAddedTask 是个 Runnable ，
                    // 这也就是为什么 PendingHandlerCallback 实现 Runnable 接口的原因。
                    executor.execute(this);
                } catch (RejectedExecutionException e) {
                    if (logger.isWarnEnabled()) {
                        logger.warn(
                                "Can't invoke handlerAdded() as the EventExecutor {} rejected it, removing handler {}.",
                                executor, ctx.name(), e);
                    }
                    // 发生异常，进行移除
                    remove0(ctx);
                    // 标记 AbstractChannelHandlerContext 为已移除
                    ctx.setRemoved();
                }
            }
        }
    }

    /**
     * 用于回调移除 ChannelHandler 节点
     */
    private final class PendingHandlerRemovedTask extends PendingHandlerCallback {

        PendingHandlerRemovedTask(AbstractChannelHandlerContext ctx) {
            super(ctx);
        }

        @Override
        public void run() {
            callHandlerRemoved0(ctx);
        }

        @Override
        void execute() {
            EventExecutor executor = ctx.executor();
            // 在 EventLoop 的线程中，回调 ChannelHandler [移除完成( removed )]事件。
            if (executor.inEventLoop()) {
                callHandlerRemoved0(ctx);
            } else {
                // 提交 EventLoop 中，执行回调 ChannelHandler removed 事件
                try {
                    // 为什么 PendingHandlerRemovedTask 可以直接提交到 EventLoop 中呢？
                    // 因为 PendingHandlerRemovedTask 是个 Runnable ，
                    // 这也就是为什么 PendingHandlerCallback 实现 Runnable 接口的原因。
                    executor.execute(this);
                } catch (RejectedExecutionException e) {
                    if (logger.isWarnEnabled()) {
                        logger.warn(
                                "Can't invoke handlerRemoved() as the EventExecutor {} rejected it," +
                                        " removing handler {}.", executor, ctx.name(), e);
                    }
                    // remove0(...) was call before so just call AbstractChannelHandlerContext.setRemoved().
                    // 标记 AbstractChannelHandlerContext 为已移除
                    ctx.setRemoved();
                }
            }
        }
    }
}
