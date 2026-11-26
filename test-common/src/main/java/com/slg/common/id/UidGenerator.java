package com.slg.common.id;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Instant;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.LongSupplier;

/**
 * 全局唯一 id 生成器：本地生成，不访问网络和数据库，多线程无锁调用，可以直接在业务线程上使用。
 * <pre>
 * | 1 位符号(0) | 31 位秒级时间（自 2026-01-01 起，约 68 年） | 12 位节点号 | 20 位秒内序号 |
 * </pre>
 * <ul>
 *     <li>同一进程生成的 id 严格递增；某一秒的序号用完时借用下一秒继续生成；</li>
 *     <li>运行中系统时钟回拨不会产生重复：逻辑时间只增不减，沿用上一次的时间继续累加序号；</li>
 *     <li>节点号必须在所有同时运行、将来可能合服的进程之间唯一，否则会产生重复 id；</li>
 *     <li>重启时如果系统时钟早于上次运行用到的时间（时钟回拨或借用了未来的秒），可能和上次运行的 id 重复。
 *     可以在停服时保存 {@link #lastSecond()}，启动时传给构造方法的 startAfterSecond 来避免。</li>
 * </ul>
 * 生成的 id 超过 53 位，不能直接用 JavaScript 的 number 表示，传给 JS/TS 客户端时要用字符串。
 */
public final class UidGenerator {

    private static final Logger log = LoggerFactory.getLogger(UidGenerator.class);

    /** 2026-01-01T00:00:00Z */
    public static final long EPOCH_SECOND = 1767225600L;

    static final int TIME_BITS = 31;
    static final int NODE_BITS = 12;
    static final int SEQ_BITS = 20;
    public static final int MAX_NODE_ID = (1 << NODE_BITS) - 1;
    static final long MAX_SEQ = (1L << SEQ_BITS) - 1;
    static final long MAX_TIME = (1L << TIME_BITS) - 1;
    /** 逻辑时间领先系统时间超过这么多秒时告警：说明发号过快或系统时钟回拨。 */
    private static final long AHEAD_WARN_SECONDS = 10;

    private final long nodeId;
    private final LongSupplier clockSeconds;
    /** 上一次发出的 (相对秒 << SEQ_BITS) | 序号。 */
    private final AtomicLong state;
    private volatile boolean aheadWarned;

    public UidGenerator(int nodeId) {
        this(nodeId, 0);
    }

    /**
     * @param startAfterSecond 只生成时间晚于这个 epoch 秒的 id，传上次运行的 {@link #lastSecond()}；没有则传 0
     */
    public UidGenerator(int nodeId, long startAfterSecond) {
        this(nodeId, startAfterSecond, () -> System.currentTimeMillis() / 1000);
    }

    UidGenerator(int nodeId, long startAfterSecond, LongSupplier clockSeconds) {
        if (nodeId < 0 || nodeId > MAX_NODE_ID) {
            throw new IllegalArgumentException("nodeId must be in [0, " + MAX_NODE_ID + "], got " + nodeId);
        }
        this.nodeId = nodeId;
        this.clockSeconds = clockSeconds;
        long floor = Math.max(0, startAfterSecond - EPOCH_SECOND);
        // 序号置满：系统时间不晚于 floor 时，下一个 id 会落到 floor + 1 秒
        this.state = new AtomicLong((floor << SEQ_BITS) | MAX_SEQ);
    }

    public long next() {
        while (true) {
            long prev = state.get();
            long prevSec = prev >>> SEQ_BITS;
            long prevSeq = prev & MAX_SEQ;
            long now = clockSeconds.getAsLong() - EPOCH_SECOND;
            long sec;
            long seq;
            if (now > prevSec) {
                sec = now;
                seq = 0;
            } else if (prevSeq < MAX_SEQ) {
                sec = prevSec;
                seq = prevSeq + 1;
            } else {
                sec = prevSec + 1;
                seq = 0;
            }
            if (sec > MAX_TIME) {
                throw new IllegalStateException("uid time overflow");
            }
            if (state.compareAndSet(prev, (sec << SEQ_BITS) | seq)) {
                checkAhead(sec - now);
                return (sec << (NODE_BITS + SEQ_BITS)) | (nodeId << SEQ_BITS) | seq;
            }
        }
    }

    /** 最近一次生成的 id 所用的 epoch 秒，停服时保存，下次启动传给构造方法。 */
    public long lastSecond() {
        return (state.get() >>> SEQ_BITS) + EPOCH_SECOND;
    }

    public int nodeId() {
        return (int) nodeId;
    }

    /** id 的生成时间。 */
    public static Instant timeOf(long uid) {
        return Instant.ofEpochSecond((uid >>> (NODE_BITS + SEQ_BITS)) + EPOCH_SECOND);
    }

    public static int nodeOf(long uid) {
        return (int) ((uid >>> SEQ_BITS) & MAX_NODE_ID);
    }

    public static long seqOf(long uid) {
        return uid & MAX_SEQ;
    }

    private void checkAhead(long aheadSeconds) {
        if (aheadSeconds > AHEAD_WARN_SECONDS) {
            if (!aheadWarned) {
                aheadWarned = true;
                log.warn("uid node {} logical clock is {}s ahead of system clock (clock moved back or ids issued too fast)",
                        nodeId, aheadSeconds);
            }
        } else if (aheadWarned) {
            aheadWarned = false;
        }
    }
}
