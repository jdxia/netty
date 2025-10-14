/*
 * Copyright 2013 The Netty Project
 *
 * The Netty Project licenses this file to you under the Apache License,
 * version 2.0 (the "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at:
 *
 *   https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations
 * under the License.
 */

package io.netty.buffer;

import java.util.concurrent.atomic.AtomicIntegerFieldUpdater;

import io.netty.util.internal.ReferenceCountUpdater;

/**
 * Abstract base class for {@link ByteBuf} implementations that count references.
 */
public abstract class AbstractReferenceCountedByteBuf extends AbstractByteBuf {
    /**
     * Netty 为 ByteBuf 引入了引用计数的机制，在 ByteBuf 的整个设计体系中，
     * 所有的 ByteBuf 都会继承一个抽象类 AbstractReferenceCountedByteBuf ， 它是对接口 ReferenceCounted 的实现。
     */

    /**
     * 获取 refCnt 字段在 ByteBuf 对象内存中的偏移
     * 后续通过 Unsafe 对 refCnt 进行操作
     */
    private static final long REFCNT_FIELD_OFFSET =
            ReferenceCountUpdater.getUnsafeOffset(AbstractReferenceCountedByteBuf.class, "refCnt");

    /**
     * 获取 refCnt 字段 的 AtomicFieldUpdater
     * 后续通过 AtomicFieldUpdater 来操作 refCnt 字段
     */
    private static final AtomicIntegerFieldUpdater<AbstractReferenceCountedByteBuf> AIF_UPDATER =
            AtomicIntegerFieldUpdater.newUpdater(AbstractReferenceCountedByteBuf.class, "refCnt");

    // 创建 ReferenceCountUpdater，对于引用计数的所有操作最终都会代理到这个类中
    private static final ReferenceCountUpdater<AbstractReferenceCountedByteBuf> updater =
            new ReferenceCountUpdater<AbstractReferenceCountedByteBuf>() {
        @Override
        protected AtomicIntegerFieldUpdater<AbstractReferenceCountedByteBuf> updater() {
            // 通过 AtomicIntegerFieldUpdater 操作 refCnt 字段
            return AIF_UPDATER;
        }
        @Override
        protected long unsafeOffset() {
            // 通过 Unsafe 操作 refCnt 字段
            return REFCNT_FIELD_OFFSET;
        }
    };

    // 引用计数
    // Value might not equal "real" reference count, all access should be via the updater
    @SuppressWarnings({"unused", "FieldMayBeFinal"})
    private volatile int refCnt;

    protected AbstractReferenceCountedByteBuf(int maxCapacity) {
        super(maxCapacity);
        updater.setInitialValue(this);
    }

    @Override
    boolean isAccessible() {
        // Try to do non-volatile read for performance as the ensureAccessible() is racy anyway and only provide
        // a best-effort guard.
        return updater.isLiveNonVolatile(this);
    }

    @Override
    public int refCnt() {
        return updater.refCnt(this);
    }

    /**
     * An unsafe operation intended for use by a subclass that sets the reference count of the buffer directly
     */
    protected final void setRefCnt(int refCnt) {
        updater.setRefCnt(this, refCnt);
    }

    /**
     * An unsafe operation intended for use by a subclass that resets the reference count of the buffer to 1
     */
    protected final void resetRefCnt() {
        updater.resetRefCnt(this);
    }

    @Override
    public ByteBuf retain() {
        return updater.retain(this);
    }

    @Override
    public ByteBuf retain(int increment) {
        return updater.retain(this, increment);
    }

    @Override
    public ByteBuf touch() {
        return this;
    }

    @Override
    public ByteBuf touch(Object hint) {
        return this;
    }

    @Override
    public boolean release() {
        // 回收
        return handleRelease(updater.release(this));
    }

    @Override
    public boolean release(int decrement) {
        return handleRelease(updater.release(this, decrement));
    }

    private boolean handleRelease(boolean result) {
        if (result) {
            deallocate();
        }
        return result;
    }

    /**
     * Called once {@link #refCnt()} is equals 0.
     */
    protected abstract void deallocate();
}
