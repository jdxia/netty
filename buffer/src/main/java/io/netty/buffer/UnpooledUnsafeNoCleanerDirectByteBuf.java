/*
 * Copyright 2016 The Netty Project
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

import io.netty.util.internal.PlatformDependent;

import java.nio.ByteBuffer;

class UnpooledUnsafeNoCleanerDirectByteBuf extends UnpooledUnsafeDirectByteBuf {
    /**
     * 由于 Cleaner 执行会依赖 GC , 而 GC 的发生往往不那么及时，会有一定的延时，
     * 所以 Netty 为了可以及时的释放  Direct Memory ，往往选择不依赖 JDK 的 Cleaner 机制，手动进行释放。
     * 所以就有了 NoCleaner 类型的 DirectByteBuf —— UnpooledUnsafeNoCleanerDirectByteBuf
     */

    UnpooledUnsafeNoCleanerDirectByteBuf(ByteBufAllocator alloc, int initialCapacity, int maxCapacity) {
        super(alloc, initialCapacity, maxCapacity);
    }

    /**
     * 无论 Netty 中的 DirectByteBuf 有没有 Cleaner， Netty 都会选择手动的进行释放，目的就是为了避免 GC 的延迟 ， 从而及时的释放 Direct Memory
     */
    @Override
    protected ByteBuffer allocateDirect(int initialCapacity) {
        // 创建没有 Cleaner 的 JDK DirectByteBuffer
        return PlatformDependent.allocateDirectNoCleaner(initialCapacity);
    }

    ByteBuffer reallocateDirect(ByteBuffer oldBuffer, int initialCapacity) {
        return PlatformDependent.reallocateDirectNoCleaner(oldBuffer, initialCapacity);
    }

    @Override
    protected void freeDirect(ByteBuffer buffer) {
        // 既然没有了 Cleaner ， 所以 Netty 要手动进行释放
        PlatformDependent.freeDirectNoCleaner(buffer);
    }

    @Override
    public ByteBuf capacity(int newCapacity) {
        checkNewCapacity(newCapacity);

        int oldCapacity = capacity();
        if (newCapacity == oldCapacity) {
            return this;
        }

        trimIndicesToCapacity(newCapacity);
        setByteBuffer(reallocateDirect(buffer, newCapacity), false);
        return this;
    }
}
