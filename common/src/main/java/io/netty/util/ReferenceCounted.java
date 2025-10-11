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
package io.netty.util;

/**
 * A reference-counted object that requires explicit deallocation.
 * <p>
 * When a new {@link ReferenceCounted} is instantiated, it starts with the reference count of {@code 1}.
 * {@link #retain()} increases the reference count, and {@link #release()} decreases the reference count.
 * If the reference count is decreased to {@code 0}, the object will be deallocated explicitly, and accessing
 * the deallocated object will usually result in an access violation.
 * </p>
 * <p>
 * If an object that implements {@link ReferenceCounted} is a container of other objects that implement
 * {@link ReferenceCounted}, the contained objects will also be released via {@link #release()} when the container's
 * reference count becomes 0.
 * </p>
 */
public interface ReferenceCounted {

    /**
     * 为了检测内存泄露的发生，这也是 Netty 为 ByteBuf 引入了引用计数的另一个原因，当 ByteBuf 不再被引用的时候，也就是没有任何强引用或者软引用的时候，
     * 如果此时发生 GC , 那么这个 ByteBuf 实例（位于 JVM 堆中）就需要被回收了，这时 Netty 就会检查这个 ByteBuf 的引用计数是否为 0 ，
     * 如果不为 0 ，说明我们忘记调用 release() 释放了，近而判断出这个 ByteBuf 发生了内存泄露
     */

    /**
     * 每个 ByteBuf 的内部都维护了一个叫做 refCnt 的引用计数，我们可以通过 refCnt() 方法来获取 ByteBuf 当前的引用计数 refCnt。
     * 当 ByteBuf 在其他上下文中被引用的时候，我们需要通过 retain() 方法将 ByteBuf 的引用计数加 1。
     * 另外我们也可以通过 retain(int increment) 方法来指定 refCnt 增加的大小（increment）。
     *
     * 有对 ByteBuf 的引用那么就有对 ByteBuf 的释放，每当我们使用完 ByteBuf 的时候就需要手动调用 release() 方法将 ByteBuf 的引用计数减 1 。
     * 当引用计数 refCnt 变成 0 的时候，Netty 就会通过 deallocate 方法来释放 ByteBuf 所引用的内存资源。
     * 这时 release() 方法会返回 true , 如果 refCnt 还不为 0 ，那么就返回 false 。同样我们也可以通过 release(int decrement) 方法来指定 refCnt 减少多少（decrement）
     */

    /**
     * Returns the reference count of this object.  If {@code 0}, it means this object has been deallocated.
     */
    int refCnt();

    /**
     * Increases the reference count by {@code 1}.
     */
    ReferenceCounted retain();

    /**
     * Increases the reference count by the specified {@code increment}.
     */
    ReferenceCounted retain(int increment);

    /**
     * Records the current access location of this object for debugging purposes.
     * If this object is determined to be leaked, the information recorded by this operation will be provided to you
     * via {@link ResourceLeakDetector}.  This method is a shortcut to {@link #touch(Object) touch(null)}.
     */
    ReferenceCounted touch();

    /**
     * Records the current access location of this object with an additional arbitrary information for debugging
     * purposes.  If this object is determined to be leaked, the information recorded by this operation will be
     * provided to you via {@link ResourceLeakDetector}.
     */
    ReferenceCounted touch(Object hint);

    /**
     * Decreases the reference count by {@code 1} and deallocates this object if the reference count reaches at
     * {@code 0}.
     *
     * @return {@code true} if and only if the reference count became {@code 0} and this object has been deallocated
     */
    boolean release();

    /**
     * Decreases the reference count by the specified {@code decrement} and deallocates this object if the reference
     * count reaches at {@code 0}.
     *
     * @return {@code true} if and only if the reference count became {@code 0} and this object has been deallocated
     */
    boolean release(int decrement);
}
