/*
 * Copyright 2019 The Netty Project
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
package io.netty.util.internal;

import static io.netty.util.internal.ObjectUtil.checkPositive;

import java.util.concurrent.atomic.AtomicIntegerFieldUpdater;

import io.netty.util.IllegalReferenceCountException;
import io.netty.util.ReferenceCounted;

/**
 * Common logic for {@link ReferenceCounted} implementations
 */
public abstract class ReferenceCountUpdater<T extends ReferenceCounted> {
    /*
     * Implementation notes:
     *
     * For the updated int field:
     *   Even => "real" refcount is (refCnt >>> 1)
     *   Odd  => "real" refcount is 0
     *
     * (x & y) appears to be surprisingly expensive relative to (x == y). Thus this class uses
     * a fast-path in some places for most common low values when checking for live (even) refcounts,
     * for example: if (rawCnt == 2 || rawCnt == 4 || (rawCnt & 1) == 0) { ...
     */

    protected ReferenceCountUpdater() { }

    public static long getUnsafeOffset(Class<? extends ReferenceCounted> clz, String fieldName) {
        try {
            if (PlatformDependent.hasUnsafe()) {
                return PlatformDependent.objectFieldOffset(clz.getDeclaredField(fieldName));
            }
        } catch (Throwable ignore) {
            // fall-back
        }
        return -1;
    }

    protected abstract AtomicIntegerFieldUpdater<T> updater();

    protected abstract long unsafeOffset();

    public final int initialValue() {
        // ByteBuf 引用计数初始化为 2
        return 2;
    }

    public void setInitialValue(T instance) {
        final long offset = unsafeOffset();
        if (offset == -1) {
            updater().set(instance, initialValue());
        } else {
            PlatformDependent.safeConstructPutInt(instance, offset, initialValue());
        }
    }

    private static int realRefCnt(int rawCnt) {
        // 奇偶判断
        return rawCnt != 2 && rawCnt != 4 && (rawCnt & 1) != 0 ? 0 : rawCnt >>> 1;
    }

    /**
     * Like {@link #realRefCnt(int)} but throws if refCnt == 0
     */
    private static int toLiveRealRefCnt(int rawCnt, int decrement) {
        if (rawCnt == 2 || rawCnt == 4 || (rawCnt & 1) == 0) {
            return rawCnt >>> 1;
        }
        // odd rawCnt => already deallocated
        throw new IllegalReferenceCountException(0, -decrement);
    }

    private int nonVolatileRawCnt(T instance) {
        // TODO: Once we compile against later versions of Java we can replace the Unsafe usage here by varhandles.
        // 获取 REFCNT_FIELD_OFFSET
        final long offset = unsafeOffset();

        // 通过 UnSafe 的方式来访问 refCnt ， 避免内存屏障的开销
        return offset != -1 ? PlatformDependent.getInt(instance, offset) : updater().get(instance);
    }

    public final int refCnt(T instance) {
        return realRefCnt(updater().get(instance));
    }

    public final boolean isLiveNonVolatile(T instance) {
        final long offset = unsafeOffset();
        final int rawCnt = offset != -1 ? PlatformDependent.getInt(instance, offset) : updater().get(instance);

        // The "real" ref count is > 0 if the rawCnt is even.
        return rawCnt == 2 || rawCnt == 4 || rawCnt == 6 || rawCnt == 8 || (rawCnt & 1) == 0;
    }

    /**
     * An unsafe operation that sets the reference count directly
     */
    public final void setRefCnt(T instance, int refCnt) {
        updater().set(instance, refCnt > 0 ? refCnt << 1 : 1); // overflow OK here
    }

    /**
     * Resets the reference count to 1
     */
    public final void resetRefCnt(T instance) {
        // no need of a volatile set, it should happen in a quiescent state
        updater().lazySet(instance, initialValue());
    }

    public final T retain(T instance) {
        // 引用计数逻辑上是加 1 ，但实际上是加 2 （实现角度）
        return retain0(instance, 1, 2);
    }

    public final T retain(T instance, int increment) {
        // all changes to the raw count are 2x the "real" change - overflow is OK
        // rawIncrement 始终是逻辑计数 increment 的两倍
        int rawIncrement = checkPositive(increment, "increment") << 1;

        // 将 rawIncrement 设置到 ByteBuf 的 refCnt 字段中
        return retain0(instance, increment, rawIncrement);
    }

    // rawIncrement == increment << 1

    /**
     * rawIncrement = increment << 1
     * increment 表示引用计数的逻辑增长步长
     * rawIncrement 表示引用计数的实际增长步长
     */
    private T retain0(T instance, final int increment, final int rawIncrement) {
        // 先通过 XADD 指令将  refCnt 的值加起来
        int oldRef = updater().getAndAdd(instance, rawIncrement);

        // 如果 oldRef 是一个奇数，也就是 ByteBuf 已经没有引用了，抛出异常
        if (oldRef != 2 && oldRef != 4 && (oldRef & 1) != 0) {

            // 如果 oldRef 已经是一个奇数了，无论多线程在这里怎么并发 retain ，都是一个奇数，这里都会抛出异常
            throw new IllegalReferenceCountException(0, increment);
        }
        // don't pass 0!
        // refCnt 不可能为 0 ，只能是 1
        if ((oldRef <= 0 && oldRef + rawIncrement >= 0)
                || (oldRef >= 0 && oldRef + rawIncrement < oldRef)) {
            // overflow case
            // 如果 refCnt 字段已经溢出，则进行回退，并抛异常
            updater().getAndAdd(instance, -rawIncrement);
            throw new IllegalReferenceCountException(realRefCnt(oldRef), increment);
        }
        return instance;
    }

    public final boolean release(T instance) {
        // 第一次尝试采用 unSafe nonVolatile 的方式读取 refCnf 的值
        int rawCnt = nonVolatileRawCnt(instance);

        // 如果逻辑引用计数被减到 0 了，那么就通过 tryFinalRelease0 使用 CAS 将 refCnf 更新为 1
        // CAS 失败的话，则通过 retryRelease0 进行重试
        // 如果逻辑引用计数不为 0 ，则通过 nonFinalRelease0 将 refCnf 减 2
        return rawCnt == 2 ? tryFinalRelease0(instance, 2) || retryRelease0(instance, 1)
                : nonFinalRelease0(instance, 1, rawCnt, toLiveRealRefCnt(rawCnt, 1));
    }

    public final boolean release(T instance, int decrement) {
        int rawCnt = nonVolatileRawCnt(instance);
        int realCnt = toLiveRealRefCnt(rawCnt, checkPositive(decrement, "decrement"));
        return decrement == realCnt ? tryFinalRelease0(instance, rawCnt) || retryRelease0(instance, decrement)
                : nonFinalRelease0(instance, decrement, rawCnt, realCnt);
    }

    private boolean tryFinalRelease0(T instance, int expectRawCnt) {
        return updater().compareAndSet(instance, expectRawCnt, 1); // any odd number will work
    }

    private boolean nonFinalRelease0(T instance, int decrement, int rawCnt, int realCnt) {
        if (decrement < realCnt
                // all changes to the raw count are 2x the "real" change - overflow is OK
                && updater().compareAndSet(instance, rawCnt, rawCnt - (decrement << 1))) {
            // ByteBuf 的 rawCnt 减少 2 * decrement
            return false;
        }
        // CAS  失败则一直重试，如果引用计数已经为 0 ，那么抛出异常，不能再次 release
        return retryRelease0(instance, decrement);
    }

    private boolean retryRelease0(T instance, int decrement) {
        for (;;) {
            // 采用 Volatile 的方式读取 refCnt
            int rawCnt = updater().get(instance),
                    // 获取逻辑引用计数，如果 refCnt 已经变为奇数，则抛出异常
                    realCnt = toLiveRealRefCnt(rawCnt, decrement);

            // 如果执行完本次 release , 逻辑引用计数为 0
            if (decrement == realCnt) {
                // CAS 将 refCnt 更新为 1
                if (tryFinalRelease0(instance, rawCnt)) {
                    return true;
                }
            } else if (decrement < realCnt) {
                // all changes to the raw count are 2x the "real" change
                // 原来的逻辑引用计数 realCnt 大于 1（decrement）
                // 则通过 CAS 将 refCnt 减 2
                if (updater().compareAndSet(instance, rawCnt, rawCnt - (decrement << 1))) {
                    return false;
                }
            } else {
                // refCnt 字段如果发生溢出，则抛出异常
                throw new IllegalReferenceCountException(realCnt, -decrement);
            }

            // CAS 失败之后调用 yield
            // 减少无畏的竞争，否则所有线程在高并发情况下都在这里 CAS 失败
            Thread.yield(); // this benefits throughput under high contention
        }
    }
}
