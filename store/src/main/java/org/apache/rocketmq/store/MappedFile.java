/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.rocketmq.store;

import com.sun.jna.NativeLong;
import com.sun.jna.Pointer;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.lang.reflect.Method;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.FileChannel.MapMode;
import java.security.AccessController;
import java.security.PrivilegedAction;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.rocketmq.common.UtilAll;
import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.logging.InternalLogger;
import org.apache.rocketmq.logging.InternalLoggerFactory;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.common.message.MessageExtBatch;
import org.apache.rocketmq.store.config.FlushDiskType;
import org.apache.rocketmq.store.util.LibC;
import sun.nio.ch.DirectBuffer;

public class MappedFile extends ReferenceResource {
    // 操作系统每页大小，默认4KB
    public static final int OS_PAGE_SIZE = 1024 * 4;
    protected static final InternalLogger log = InternalLoggerFactory.getLogger(LoggerName.STORE_LOGGER_NAME);

    // 当前JVM实例中MappedFile的虚拟内存
    private static final AtomicLong TOTAL_MAPPED_VIRTUAL_MEMORY = new AtomicLong(0);

    // 当前JVM实例中
    // MappedFile对象个数
    private static final AtomicInteger TOTAL_MAPPED_FILES = new AtomicInteger(0);
    // 当前文件的写指针，从0开始（内存映射文件中的写指针）
    protected final AtomicInteger wrotePosition = new AtomicInteger(0);
    // 当前文件的提交指针，如果开启transientStore-PoolEnable，则数据会存储在
    //TransientStorePool中，然后提交到内存映射ByteBuffer中，再写入磁盘
    protected final AtomicInteger committedPosition = new AtomicInteger(0);
    // 将该指针之前的数据持久化存储到磁盘中
    private final AtomicInteger flushedPosition = new AtomicInteger(0);
    // 文件大小
    protected int fileSize;
    // 文件通道
    protected FileChannel fileChannel;
    /**
     * Message will put to here first, and then reput to FileChannel if writeBuffer is not null.
     */
    // 堆外内存ByteBuffer，如果不为空，数据首先将存储在该Buffer中，然后提交到MappedFile创建的
    //FileChannel中。transientStorePoolEnable为true时不为空
    protected ByteBuffer writeBuffer = null;
    // 堆外内存池，该内存池中的内存会提供内存锁机制。transientStorePoolEnable为true时启用
    protected TransientStorePool transientStorePool = null;
    // 文件名称
    private String fileName;
    // 该文件的初始偏移量
    private long fileFromOffset;
    // 物理文件
    private File file;
    // 物理文件对应的内存映射Buffer
    private MappedByteBuffer mappedByteBuffer;
    // 文件最后一次写入内容的时间
    private volatile long storeTimestamp = 0;
    // 是否是MappedFileQueue队列中第一个文件。
    private boolean firstCreateInQueue = false;

    public MappedFile() {
    }

    /**
     * transientStorePoolEnable为false
     */
    public MappedFile(final String fileName, final int fileSize) throws IOException {
        init(fileName, fileSize);
    }

    /**
     * transientStorePoolEnable为true表示内容先存储在堆外内存，
     * 然后通过Commit线程将数据提交到FileChannel中，再通过Flush线程将数据持久化到磁盘中
     */
    public MappedFile(final String fileName, final int fileSize,
        final TransientStorePool transientStorePool) throws IOException {
        init(fileName, fileSize, transientStorePool);
    }

    public static void ensureDirOK(final String dirName) {
        if (dirName != null) {
            File f = new File(dirName);
            if (!f.exists()) {
                boolean result = f.mkdirs();
                log.info(dirName + " mkdir " + (result ? "OK" : "Failed"));
            }
        }
    }

    public static void clean(final ByteBuffer buffer) {
        if (buffer == null || !buffer.isDirect() || buffer.capacity() == 0)
            return;
        /**
         * 嵌套递归获取directByteBuffer的最内部的attachment或者viewedBuffer方法
         * 获取directByteBuffer的Cleaner对象，然后调用cleaner.clean方法，进行释放资源
         *
         */
        invoke(invoke(viewed(buffer), "cleaner"), "clean");
    }

    private static Object invoke(final Object target, final String methodName, final Class<?>... args) {
        return AccessController.doPrivileged(new PrivilegedAction<Object>() {
            public Object run() {
                try {
                    Method method = method(target, methodName, args);
                    method.setAccessible(true);
                    return method.invoke(target);
                } catch (Exception e) {
                    throw new IllegalStateException(e);
                }
            }
        });
    }

    private static Method method(Object target, String methodName, Class<?>[] args)
        throws NoSuchMethodException {
        try {
            return target.getClass().getMethod(methodName, args);
        } catch (NoSuchMethodException e) {
            return target.getClass().getDeclaredMethod(methodName, args);
        }
    }

    private static ByteBuffer viewed(ByteBuffer buffer) {
        String methodName = "viewedBuffer";
        Method[] methods = buffer.getClass().getMethods();
        for (int i = 0; i < methods.length; i++) {
            if (methods[i].getName().equals("attachment")) {
                methodName = "attachment";
                break;
            }
        }

        ByteBuffer viewedBuffer = (ByteBuffer) invoke(buffer, methodName);
        if (viewedBuffer == null)
            return buffer;
        else
            return viewed(viewedBuffer);
    }

    public static int getTotalMappedFiles() {
        return TOTAL_MAPPED_FILES.get();
    }

    public static long getTotalMappedVirtualMemory() {
        return TOTAL_MAPPED_VIRTUAL_MEMORY.get();
    }

    public void init(final String fileName, final int fileSize,
        final TransientStorePool transientStorePool) throws IOException {
        // todo
        init(fileName, fileSize);
        // 初始化MappedFile的writeBuffer，该buffer从transientStorePool中获取
        this.writeBuffer = transientStorePool.borrowBuffer();
        this.transientStorePool = transientStorePool;
    }


    /**
     * 初始化fileFromOffset为文件名，也就是文件名代表该
     * 文件的起始偏移量，通过RandomAccessFile创建读写文件通道，并将
     * 文件内容使用NIO的内存映射Buffer将文件映射到内存中
     */
    private void init(final String fileName, final int fileSize) throws IOException {
        this.fileName = fileName;
        this.fileSize = fileSize;
        this.file = new File(fileName);
        //通过文件名获取起始偏移量
        this.fileFromOffset = Long.parseLong(this.file.getName());
        boolean ok = false;

        //确保父目录存在
        ensureDirOK(this.file.getParent());

        try {
            this.fileChannel = new RandomAccessFile(this.file, "rw").getChannel();
            this.mappedByteBuffer = this.fileChannel.map(MapMode.READ_WRITE, 0, fileSize);
            TOTAL_MAPPED_VIRTUAL_MEMORY.addAndGet(fileSize);
            TOTAL_MAPPED_FILES.incrementAndGet();
            ok = true;
        } catch (FileNotFoundException e) {
            log.error("Failed to create file " + this.fileName, e);
            throw e;
        } catch (IOException e) {
            log.error("Failed to map file " + this.fileName, e);
            throw e;
        } finally {
            if (!ok && this.fileChannel != null) {
                this.fileChannel.close();
            }
        }
    }

    public long getLastModifiedTimestamp() {
        return this.file.lastModified();
    }

    public int getFileSize() {
        return fileSize;
    }

    public FileChannel getFileChannel() {
        return fileChannel;
    }

    public AppendMessageResult appendMessage(final MessageExtBrokerInner msg, final AppendMessageCallback cb) {
        // todo
        return appendMessagesInner(msg, cb);
    }

    public AppendMessageResult appendMessages(final MessageExtBatch messageExtBatch, final AppendMessageCallback cb) {
        return appendMessagesInner(messageExtBatch, cb);
    }

    /**
     * 将消息追加到MappedFile文件中
     */
    public AppendMessageResult appendMessagesInner(final MessageExt messageExt, final AppendMessageCallback cb) {
        assert messageExt != null;
        assert cb != null;

        // 获取MappedFile当前文件写指针
        int currentPos = this.wrotePosition.get();

        // 如果currentPos小于文件大小
        if (currentPos < this.fileSize) {
            /**
             * RocketMQ提供两种数据落盘的方式:
             * 1. 直接将数据写到mappedByteBuffer, 然后flush;
             * 2. 先写到writeBuffer, 再从writeBuffer提交到fileChannel, 最后flush.
             */
            ByteBuffer byteBuffer = writeBuffer != null ? writeBuffer.slice() : this.mappedByteBuffer.slice();
            byteBuffer.position(currentPos);
            AppendMessageResult result;
            // 单个消息
            if (messageExt instanceof MessageExtBrokerInner) {
                // todo 追加消息
                result = cb.doAppend(this.getFileFromOffset(), byteBuffer, this.fileSize - currentPos, (MessageExtBrokerInner) messageExt);
            // 批量消息
            } else if (messageExt instanceof MessageExtBatch) {
                result = cb.doAppend(this.getFileFromOffset(), byteBuffer, this.fileSize - currentPos, (MessageExtBatch) messageExt);
            } else {
                return new AppendMessageResult(AppendMessageStatus.UNKNOWN_ERROR);
            }
            this.wrotePosition.addAndGet(result.getWroteBytes());
            this.storeTimestamp = result.getStoreTimestamp();
            return result;
        }
        // 如果currentPos大于或等于文件大小，表明文件已写满，抛出异常
        log.error("MappedFile.appendMessage return null, wrotePosition: {} fileSize: {}", currentPos, this.fileSize);
        return new AppendMessageResult(AppendMessageStatus.UNKNOWN_ERROR);
    }

    public long getFileFromOffset() {
        return this.fileFromOffset;
    }

    public boolean appendMessage(final byte[] data) {
        // 获取当前写的位置
        int currentPos = this.wrotePosition.get();

        // 判断在这个MappedFile中能不能 放下
        if ((currentPos + data.length) <= this.fileSize) {
            try {
                // 写入消息
                this.fileChannel.position(currentPos);
                this.fileChannel.write(ByteBuffer.wrap(data));
            } catch (Throwable e) {
                log.error("Error occurred when append message to mappedFile.", e);
            }
            // 重置 写入消息
            this.wrotePosition.addAndGet(data.length);
            return true;
        }

        return false;
    }

    /**
     * Content of data from offset to offset + length will be wrote to file.
     *
     * @param offset The offset of the subarray to be used.
     * @param length The length of the subarray to be used.
     */
    public boolean appendMessage(final byte[] data, final int offset, final int length) {
        int currentPos = this.wrotePosition.get();

        if ((currentPos + length) <= this.fileSize) {
            try {
                this.fileChannel.position(currentPos);
                this.fileChannel.write(ByteBuffer.wrap(data, offset, length));
            } catch (Throwable e) {
                log.error("Error occurred when append message to mappedFile.", e);
            }
            this.wrotePosition.addAndGet(length);
            return true;
        }

        return false;
    }

    /**
     * @return The current flushed position
     *
     * 刷盘指的是将内存中的数据写入磁盘，永久存储在磁盘中
     *
     */
    public int flush(final int flushLeastPages) {
        // todo
        if (this.isAbleToFlush(flushLeastPages)) {
            if (this.hold()) {
                // todo flushedPosition应该等于MappedByteBuffer中的写指针
                int value = getReadPosition();

                try {
                    //We only append data to fileChannel or mappedByteBuffer, never both.
                    if (writeBuffer != null || this.fileChannel.position() != 0) {
                        // todo 将内存中数据持久化到磁盘
                        this.fileChannel.force(false);
                    } else {
                        //
                        this.mappedByteBuffer.force();
                    }
                } catch (Throwable e) {
                    log.error("Error occurred when force data to disk.", e);
                }
                // 设置 记录flush位置
                this.flushedPosition.set(value);
                this.release();
            } else {
                log.warn("in flush, hold failed, flush offset = " + this.flushedPosition.get());
                // todo
                this.flushedPosition.set(getReadPosition());
            }
        }
        // todo 返回flush位置
        return this.getFlushedPosition();
    }

    /**
     * commitLeastPages 为本次提交最小的页面，默认4页(4*4KB),可参见
     * org.apache.rocketmq.store.CommitLog.CommitRealTimeService#run()
     */
    public int commit(final int commitLeastPages) {
        // writeBuffer如果为空，直接返回wrotePosition指针，无
        // 须执行commit操作，这表明commit操作的主体是writeBuffer
        if (writeBuffer == null) {
            //no need to commit data to file channel, so just regard wrotePosition as committedPosition.
            return this.wrotePosition.get();
        }
        // todo 如果可以提交数据
        if (this.isAbleToCommit(commitLeastPages)) {
            if (this.hold()) {
                // todo 提交操作
                commit0(commitLeastPages);
                this.release();
            } else {
                log.warn("in commit, hold failed, commit offset = " + this.committedPosition.get());
            }
        }

        // All dirty data has been committed to FileChannel.
        if (writeBuffer != null && this.transientStorePool != null && this.fileSize == this.committedPosition.get()) {
            //清理工作，归还到堆外内存池中，并且释放当前writeBuffer
            this.transientStorePool.returnBuffer(writeBuffer);
            this.writeBuffer = null;
        }
        // 返回提交位置
        return this.committedPosition.get();
    }

    /**
     * commit的作用是将MappedFile# writeBuffer中的数据提交到文件通道FileChannel中
     */
    protected void commit0(final int commitLeastPages) {
        // 获取写入位置
        int writePos = this.wrotePosition.get();
        // 获取上次提交的位置
        int lastCommittedPosition = this.committedPosition.get();

        if (writePos - lastCommittedPosition > commitLeastPages) {
            try {
                // 创建writeBuffer的共享缓存区
                ByteBuffer byteBuffer = writeBuffer.slice();
                // 然后将新创建的position回退到上一次提交的位置
                byteBuffer.position(lastCommittedPosition);
                // 设置limit为wrotePosition（当前最大有效数据指针）
                byteBuffer.limit(writePos);
                this.fileChannel.position(lastCommittedPosition);
                // todo 接着把committedPosition到wrotePosition的数据复制(写入）到FileChannel中
                this.fileChannel.write(byteBuffer);
                // todo 最后更新committedPosition指针为wrotePosition。
                this.committedPosition.set(writePos);
            } catch (Throwable e) {
                log.error("Error occurred when commit data to FileChannel.", e);
            }
        }
    }

    /**
     * 是否能够flush
     *  1. 文件已经写满
     *  2. flushLeastPages > 0 && 未flush部分超过flushLeastPages
     *  3. flushLeastPages==0&&有新写入的部分
     * @param flushLeastPages flush最小分页
     *      mmap映射后的内存一般是内存页大小的倍数，而内存页大小一般为4K，所以写入到映射内存的数据大小可以以4K进行分页，
     *      而flushLeastPages这个参数只是指示写了多少页后才可以强制将映射内存区域的数据强行写入到磁盘文件
     * @return
     */
    private boolean isAbleToFlush(final int flushLeastPages) {
        // 获取上次flush位置
        int flush = this.flushedPosition.get();
        // 写入位置偏移量
        int write = getReadPosition();

        if (this.isFull()) {
            return true;
        }

        // 如果flush的页数大于0，校验本次flush的页数是否满足条件
        if (flushLeastPages > 0) {
            // 本次flush的页数：写入位置偏移量/OS_PAGE_SIZE - 上次flush位置偏移量/OS_PAGE_SIZE，是否大于flushLeastPages
            return ((write / OS_PAGE_SIZE) - (flush / OS_PAGE_SIZE)) >= flushLeastPages;
        }
        // 写入位置偏移量是否大于flush位置偏移量
        return write > flush;
    }

    // 是否可以提交数据
    protected boolean isAbleToCommit(final int commitLeastPages) {
        // 获取提交数据的位置偏移量
        int flush = this.committedPosition.get();
        // 获取写入数据的位置偏移量
        int write = this.wrotePosition.get();

        if (this.isFull()) {
            return true;
        }

        // commitLeastPages为本次提交的最小页数，如果
        //待提交数据不满足commitLeastPages，则不执行本次提交操作，等待下次提交
        if (commitLeastPages > 0) {
            // 写入位置/页大小 - flush位置/页大小 是否大于至少提交的页数
            return ((write / OS_PAGE_SIZE) - (flush / OS_PAGE_SIZE)) >= commitLeastPages;
        }
        // commitLeastPages = 0情况
        // 判断是否需要flush数据
        return write > flush;
    }

    public int getFlushedPosition() {
        return flushedPosition.get();
    }

    public void setFlushedPosition(int pos) {
        this.flushedPosition.set(pos);
    }

    // 文件是否已写满
    public boolean isFull() {
        // 文件大小是否与写入数据位置相等
        return this.fileSize == this.wrotePosition.get();
    }

    public SelectMappedBufferResult selectMappedBuffer(int pos, int size) {
        int readPosition = getReadPosition();
        if ((pos + size) <= readPosition) {
            if (this.hold()) {
                ByteBuffer byteBuffer = this.mappedByteBuffer.slice();
                byteBuffer.position(pos);
                ByteBuffer byteBufferNew = byteBuffer.slice();
                byteBufferNew.limit(size);
                return new SelectMappedBufferResult(this.fileFromOffset + pos, byteBufferNew, size, this);
            } else {
                log.warn("matched, but hold failed, request pos: " + pos + ", fileFromOffset: "
                    + this.fileFromOffset);
            }
        } else {
            log.warn("selectMappedBuffer request pos invalid, request pos: " + pos + ", size: " + size
                + ", fileFromOffset: " + this.fileFromOffset);
        }

        return null;
    }

    // 根据位置读取
    public SelectMappedBufferResult selectMappedBuffer(int pos) {
        // 获取这个MappedFile 里面的一个read position，其实就是这个MappedFile 的一个可读位置
        int readPosition = getReadPosition();
        if (pos < readPosition && pos >= 0) {
            if (this.hold()) {
                ByteBuffer byteBuffer = this.mappedByteBuffer.slice();
                byteBuffer.position(pos);
                int size = readPosition - pos;
                ByteBuffer byteBufferNew = byteBuffer.slice();
                byteBufferNew.limit(size);
                return new SelectMappedBufferResult(this.fileFromOffset + pos, byteBufferNew, size, this);
            }
        }

        return null;
    }

    @Override
    public boolean cleanup(final long currentRef) {
        if (this.isAvailable()) {
            log.error("this file[REF:" + currentRef + "] " + this.fileName
                + " have not shutdown, stop unmapping.");
            return false;
        }

        // todo
        if (this.isCleanupOver()) {
            log.error("this file[REF:" + currentRef + "] " + this.fileName
                + " have cleanup, do not do it again.");
            return true;
        }

        // todo
        clean(this.mappedByteBuffer);
        //加一个fileSize大小的负数值
        TOTAL_MAPPED_VIRTUAL_MEMORY.addAndGet(this.fileSize * (-1));
        TOTAL_MAPPED_FILES.decrementAndGet();
        log.info("unmap file[REF:" + currentRef + "] " + this.fileName + " OK");
        return true;
    }

    /**
     * 销毁方法
     * @param intervalForcibly 表示拒绝被销毁的最大存活时间
     * @return
     */
    public boolean destroy(final long intervalForcibly) {
        // todo
        this.shutdown(intervalForcibly);
        // 清理结束
        if (this.isCleanupOver()) {
            try {
                // 关闭文件通道，
                this.fileChannel.close();
                log.info("close file channel " + this.fileName + " OK");

                long beginTime = System.currentTimeMillis();
                // 删除物理文件
                boolean result = this.file.delete();
                log.info("delete file[REF:" + this.getRefCount() + "] " + this.fileName
                    + (result ? " OK, " : " Failed, ") + "W:" + this.getWrotePosition() + " M:"
                    + this.getFlushedPosition() + ", "
                    + UtilAll.computeElapsedTimeMilliseconds(beginTime));
            } catch (Exception e) {
                log.warn("close file channel " + this.fileName + " Failed. ", e);
            }

            return true;
        } else {
            log.warn("destroy mapped file[REF:" + this.getRefCount() + "] " + this.fileName
                + " Failed. cleanupOver: " + this.cleanupOver);
        }

        return false;
    }

    public int getWrotePosition() {
        return wrotePosition.get();
    }

    public void setWrotePosition(int pos) {
        this.wrotePosition.set(pos);
    }

    /**
     * @return The max position which have valid data
     * 获取最大的读指针
     *
     * 如果writeBuffer不为空，则flushedPosition应等于上一次commit指针。
     * 因为上一次提交的数据就是进入MappedByteBuffer中的数据。
     *
     * 如果writeBuffer为空，表示数据是直接进入MappedByteBuffer的，
     * wrotePosition代表的是MappedByteBuffer中的指针，故设置flushedPosition为wrotePosition
     */
    public int getReadPosition() {
        // 如果writeBuffer为空使用写入位置，否则使用提交位置
        return this.writeBuffer == null ? this.wrotePosition.get() : this.committedPosition.get();
    }

    public void setCommittedPosition(int pos) {
        this.committedPosition.set(pos);
    }

    /**
     * 1. 对当前映射文件进行预热
     *   1.1. 先对当前映射文件的每个内存页写入一个字节0.当刷盘策略为同步刷盘时，执行强制刷盘，并且是每修改pages个分页刷一次盘
     *  再将当前MappedFile全部的地址空间锁定，防止被swap
     *   1.2. 然后将当前MappedFile全部的地址空间锁定在物理存储中，防止其被交换到swap空间。再调用madvise，传入 WILL_NEED 策略，将刚刚锁住的内存预热，其实就是告诉内核，我马上就要用（WILL_NEED）这块内存，先做虚拟内存到物理内存的映射，防止正式使用时产生缺页中断。
     *  2. 只要启用缓存预热，都会通过mappedByteBuffer来写入假值(字节0)，并且都会对mappedByteBuffer执行mlock和madvise。
     * @param type 刷盘策略
     * @param pages 预热时一次刷盘的分页数
     */
    public void warmMappedFile(FlushDiskType type, int pages) {
        long beginTime = System.currentTimeMillis();
        ByteBuffer byteBuffer = this.mappedByteBuffer.slice();
        //上一次刷盘的位置
        int flush = 0;
        long time = System.currentTimeMillis();
        for (int i = 0, j = 0; i < this.fileSize; i += MappedFile.OS_PAGE_SIZE, j++) {
            byteBuffer.put(i, (byte) 0);
            // force flush when flush disk type is sync
            if (type == FlushDiskType.SYNC_FLUSH) {
                /**
                 *  同步刷盘，每修改pages个分页强制刷一次盘，默认16MB
                 * 参见org.apache.rocketmq.store.config.MessageStoreConfig#flushLeastPagesWhenWarmMapedFile
                 */
                if ((i / OS_PAGE_SIZE) - (flush / OS_PAGE_SIZE) >= pages) {
                    flush = i;
                    //FIXME 刷入修改的内容，不会有性能问题？？
                    mappedByteBuffer.force();
                }
            }

            // prevent gc
            if (j % 1000 == 0) {
                log.info("j={}, costTime={}", j, System.currentTimeMillis() - time);
                time = System.currentTimeMillis();
                try {
                    Thread.sleep(0);
                } catch (InterruptedException e) {
                    log.error("Interrupted", e);
                }
            }
        }

        // force flush when prepare load finished
        if (type == FlushDiskType.SYNC_FLUSH) {
            log.info("mapped file warm-up done, force to disk, mappedFile={}, costTime={}",
                this.getFileName(), System.currentTimeMillis() - beginTime);
            mappedByteBuffer.force();
        }
        log.info("mapped file warm-up done. mappedFile={}, costTime={}", this.getFileName(),
            System.currentTimeMillis() - beginTime);

        this.mlock();
    }

    public String getFileName() {
        return fileName;
    }

    public MappedByteBuffer getMappedByteBuffer() {
        return mappedByteBuffer;
    }

    public ByteBuffer sliceByteBuffer() {
        return this.mappedByteBuffer.slice();
    }

    public long getStoreTimestamp() {
        return storeTimestamp;
    }

    public boolean isFirstCreateInQueue() {
        return firstCreateInQueue;
    }

    public void setFirstCreateInQueue(boolean firstCreateInQueue) {
        this.firstCreateInQueue = firstCreateInQueue;
    }

    public void mlock() {
        final long beginTime = System.currentTimeMillis();
        final long address = ((DirectBuffer) (this.mappedByteBuffer)).address();
        Pointer pointer = new Pointer(address);
        {
            // 内存锁定
            // 通过mlock可以将进程使用的部分或者全部的地址空间锁定在物理内存中，防止其被交换到swap空间。
            // 对时间敏感的应用会希望全部使用物理内存，提高数据访问和操作的效率。
            int ret = LibC.INSTANCE.mlock(pointer, new NativeLong(this.fileSize));
            log.info("mlock {} {} {} ret = {} time consuming = {}", address, this.fileName, this.fileSize, ret, System.currentTimeMillis() - beginTime);
        }

        {
            //文件预读
            //madvise 一次性先将一段数据读入到映射内存区域，这样就减少了缺页异常的产生。
            int ret = LibC.INSTANCE.madvise(pointer, new NativeLong(this.fileSize), LibC.MADV_WILLNEED);
            log.info("madvise {} {} {} ret = {} time consuming = {}", address, this.fileName, this.fileSize, ret, System.currentTimeMillis() - beginTime);
        }
    }

    public void munlock() {
        final long beginTime = System.currentTimeMillis();
        final long address = ((DirectBuffer) (this.mappedByteBuffer)).address();
        Pointer pointer = new Pointer(address);
        int ret = LibC.INSTANCE.munlock(pointer, new NativeLong(this.fileSize));
        log.info("munlock {} {} {} ret = {} time consuming = {}", address, this.fileName, this.fileSize, ret, System.currentTimeMillis() - beginTime);
    }

    //testable
    File getFile() {
        return this.file;
    }

    @Override
    public String toString() {
        return this.fileName;
    }
}
