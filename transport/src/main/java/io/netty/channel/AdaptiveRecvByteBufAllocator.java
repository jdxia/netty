/*
 * Copyright 2012 The Netty Project
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
package io.netty.channel;

import java.util.ArrayList;
import java.util.List;

import static io.netty.util.internal.ObjectUtil.checkPositive;
import static java.lang.Math.max;
import static java.lang.Math.min;

/**
 * The {@link RecvByteBufAllocator} that automatically increases and
 * decreases the predicted buffer size on feed back.
 * <p>
 * It gradually increases the expected number of readable bytes if the previous
 * read fully filled the allocated buffer.  It gradually decreases the expected
 * number of readable bytes if the read operation was not able to fill a certain
 * amount of the allocated buffer two times consecutively.  Otherwise, it keeps
 * returning the same prediction.
 */
public class AdaptiveRecvByteBufAllocator extends DefaultMaxMessagesRecvByteBufAllocator {

    /**
     * 那么在什么情况下需要对 ByteBuf 扩容，每次扩容多少 ？ 什么情况下需要对 ByteBuf 进行缩容，每次缩容多少呢 ？
     *
     * 这就用到了一个重要的容量索引结构 ——  SIZE_TABLE，它里边定义索引了 ByteBuf 的每一种容量大小。相当于是扩缩容的容量索引表。
     * 每次扩容多少，缩容多少全部记录在这个容量索引表中。
     *
     * 当索引容量小于 512 时，SIZE_TABLE 中定义的容量是从 16 开始按照 16 递增
     * 当索引容量大于 512 时，SIZE_TABLE 中定义的容量是按前一个索引容量的 2 倍递增。
     *
     * 满足一次扩容条件就进行扩容，并且扩容步长为 4 (INDEX_INCREMENT)， 扩容比较奔放。
     *
     * 如果 totalBytesRead 小于等于 SIZE_TABLE[index - INDEX_DECREMENT]，也就是如果本轮 read loop 结束之后总共读取的字节数小于等于1024。
     * 表示本次读取到的字节数比当前 ByteBuf 容量的下一级容量还要小，说明当前 ByteBuf 的容量分配的有些大了，设置缩容标识decreaseNow = true。
     * 当下次 read loop 的时候如果继续满足缩容条件，那么就开始进行缩容。缩容后的容量为 SIZE_TABLE[index - INDEX_DECREMENT]，但不能小于SIZE_TABLE[minIndex]（16）。
     *
     * 注意，这里需要满足两次缩容条件才会进行缩容，且缩容步长为 1 (INDEX_DECREMENT)，缩容比较谨慎。
     */

    // 表示ByteBuffer最小的容量，默认为64，也就是无论ByteBuffer在怎么缩容，容量也不会低于64
    static final int DEFAULT_MINIMUM = 64;

    // Use an initial value that is bigger than the common MTU of 1500
    /**
     * 在接收网络数据的过程中，其实一开始是很难确定出该用多大容量的 ByteBuf 去接收的，所以 Netty 在一开始会首先预估一个初始容量 DEFAULT_INITIAL (2048)
     * 表示ByteBuffer的初始化容量。默认为2048
     *
     * 用初始容量为 2048 大小的 ByteBuf 去读取 socket 中的数据，在每一次读取完 socket 之后，Netty 都会评估 ByteBuf 的容量大小是否合适。
     * 如果每一次都能把 ByteBuf 装满，那说明我们预估的容量太小了，socket 中还有更多的数据，那么就需要对 ByteBuf 进行扩容，下一次读取 socket 的时候就换一个容量更大的 ByteBuf
     */
    static final int DEFAULT_INITIAL = 2048;

    // 表示ByteBuffer的最大容量，默认为65536，也就是无论ByteBuffer在怎么扩容，容量也不会超过65536
    static final int DEFAULT_MAXIMUM = 65536;

    //扩容步长
    private static final int INDEX_INCREMENT = 4;

    //缩容步长
    private static final int INDEX_DECREMENT = 1;

    //RecvBuf分配容量表（扩缩容索引表）按照表中记录的容量大小进行扩缩容
    private static final int[] SIZE_TABLE;

    static {
        //初始化RecvBuf容量分配表
        List<Integer> sizeTable = new ArrayList<Integer>();

        //当分配容量小于512时，扩容单位为16递增
        for (int i = 16; i < 512; i += 16) {
            sizeTable.add(i);
        }

        // Suppress a warning since i becomes negative when an integer overflow happens
        //当分配容量大于512时，扩容单位为一倍
        for (int i = 512; i > 0; i <<= 1) {
            sizeTable.add(i);
        }

        //初始化RecbBuf扩缩容索引表
        SIZE_TABLE = new int[sizeTable.size()];
        for (int i = 0; i < SIZE_TABLE.length; i ++) {
            SIZE_TABLE[i] = sizeTable.get(i);
        }
    }

    /**
     * @deprecated There is state for {@link #maxMessagesPerRead()} which is typically based upon channel type.
     */
    @Deprecated
    public static final AdaptiveRecvByteBufAllocator DEFAULT = new AdaptiveRecvByteBufAllocator();

    private static int getSizeTableIndex(final int size) {
        for (int low = 0, high = SIZE_TABLE.length - 1;;) {
            if (high < low) {
                return low;
            }
            if (high == low) {
                return high;
            }

            //无符号右移，高位始终补0
            int mid = low + high >>> 1;
            int a = SIZE_TABLE[mid];
            int b = SIZE_TABLE[mid + 1];
            if (size > b) {
                low = mid + 1;
            } else if (size < a) {
                high = mid - 1;
            } else if (size == a) {
                return mid;
            } else {
                return mid + 1;
            }
        }
    }

    private final class HandleImpl extends MaxMessageHandle {
        //最小容量在扩缩容索引表中的index
        private final int minIndex;

        //最大容量在扩缩容索引表中的index
        private final int maxIndex;
        private final int minCapacity;
        private final int maxCapacity;

        //当前容量在扩缩容索引表中的index 初始33 对应容量2048
        private int index;

        //预计下一次分配buffer的容量，初始：2048
        private int nextReceiveBufferSize;

        //是否缩容
        private boolean decreaseNow;

        HandleImpl(int minIndex, int maxIndex, int initialIndex, int minCapacity, int maxCapacity) {
            this.minIndex = minIndex;
            this.maxIndex = maxIndex;

            index = initialIndex;
            nextReceiveBufferSize = max(SIZE_TABLE[index], minCapacity);
            this.minCapacity = minCapacity;
            this.maxCapacity = maxCapacity;
        }

        @Override
        public void lastBytesRead(int bytes) {
            // If we read as much as we asked for we should check if we need to ramp up the size of our next guess.
            // This helps adjust more quickly when large amounts of data is pending and can avoid going back to
            // the selector to check for more data. Going back to the selector can add significant latency for large
            // data transfers.
            /**
             * bytes 为本次从 socket 中真实读取的数据大小
             * attemptedBytesRead 为 ByteBuf 可写的容量大小，初始为 2048
             */
            if (bytes == attemptedBytesRead()) {
                /**
                 * 如果本次读取 socket 中的数据将 ByteBuf 装满了
                 * 那么就对 ByteBuf 进行扩容，在下一次读取的时候用更大的 ByteBuf 去读
                 */
                record(bytes);
            }

            // 记录本次从 socket 中读取的数据大小
            super.lastBytesRead(bytes);
        }

        @Override
        public int guess() {
            return nextReceiveBufferSize;
        }

        private void record(int actualReadBytes) {
            if (actualReadBytes <= SIZE_TABLE[max(0, index - INDEX_DECREMENT)]) {
                if (decreaseNow) {
                    index = max(index - INDEX_DECREMENT, minIndex);
                    nextReceiveBufferSize = max(SIZE_TABLE[index], minCapacity);
                    decreaseNow = false;
                } else {
                    decreaseNow = true;
                }
            } else if (actualReadBytes >= nextReceiveBufferSize) {
                index = min(index + INDEX_INCREMENT, maxIndex);
                nextReceiveBufferSize = min(SIZE_TABLE[index], maxCapacity);
                decreaseNow = false;
            }
        }

        @Override
        public void readComplete() {
            //是否进行扩容缩容
            record(totalBytesRead());
        }
    }

    private final int minIndex;
    private final int maxIndex;
    private final int initialIndex;
    private final int minCapacity;
    private final int maxCapacity;

    /**
     * Creates a new predictor with the default parameters.  With the default
     * parameters, the expected buffer size starts from {@code 1024}, does not
     * go down below {@code 64}, and does not go up above {@code 65536}.
     */
    public AdaptiveRecvByteBufAllocator() {
        this(DEFAULT_MINIMUM, DEFAULT_INITIAL, DEFAULT_MAXIMUM);
    }

    /**
     * Creates a new predictor with the specified parameters.
     *
     * @param minimum  the inclusive lower bound of the expected buffer size
     * @param initial  the initial buffer size when no feed back was received
     * @param maximum  the inclusive upper bound of the expected buffer size
     */
    public AdaptiveRecvByteBufAllocator(int minimum, int initial, int maximum) {
        checkPositive(minimum, "minimum");
        if (initial < minimum) {
            throw new IllegalArgumentException("initial: " + initial);
        }
        if (maximum < initial) {
            throw new IllegalArgumentException("maximum: " + maximum);
        }

        /**
         * 计算minIndex maxIndex
         * 在SIZE_TABLE中二分查找最小 >= minimum的容量索引 ：3
         */
        int minIndex = getSizeTableIndex(minimum);
        if (SIZE_TABLE[minIndex] < minimum) {
            this.minIndex = minIndex + 1;
        } else {
            this.minIndex = minIndex;
        }

        //在SIZE_TABLE中二分查找最大 <= maximum的容量索引 ：38
        int maxIndex = getSizeTableIndex(maximum);
        if (SIZE_TABLE[maxIndex] > maximum) {
            this.maxIndex = maxIndex - 1;
        } else {
            this.maxIndex = maxIndex;
        }

        int initialIndex = getSizeTableIndex(initial);
        if (SIZE_TABLE[initialIndex] > initial) {
            this.initialIndex = initialIndex - 1;
        } else {
            this.initialIndex = initialIndex;
        }
        this.minCapacity = minimum;
        this.maxCapacity = maximum;
    }

    @SuppressWarnings("deprecation")
    @Override
    public Handle newHandle() {
        return new HandleImpl(minIndex, maxIndex, initialIndex, minCapacity, maxCapacity);
    }

    @Override
    public AdaptiveRecvByteBufAllocator respectMaybeMoreData(boolean respectMaybeMoreData) {
        super.respectMaybeMoreData(respectMaybeMoreData);
        return this;
    }
}
