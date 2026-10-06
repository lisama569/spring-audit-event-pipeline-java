package com.github.lisama569.springauditeventpipelinejava.tests;

import com.github.lisama569.springauditeventpipelinejava.recovery.DurableLog;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.RandomAccessFile;
import java.nio.file.Path;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** 追加日志：正常读写、崩溃尾部残缺容忍、压缩重写。 */
class DurableLogTest {

    private static final Logger log = LoggerFactory.getLogger(DurableLogTest.class);

    @TempDir
    Path dir;

    @Test
    void appendsReadsAndIgnoresTruncatedTail() throws Exception {
        Path file = dir.resolve("data.log");
        DurableLog durableLog = new DurableLog(file);
        durableLog.append("alpha");
        durableLog.append("beta");
        durableLog.append("gamma");
        log.info("写入 3 条记录 alpha/beta/gamma，读回={}", durableLog.readAll());
        assertEquals(List.of("alpha", "beta", "gamma"), durableLog.readAll());

        // 模拟崩溃：在文件尾部截断若干字节（最后一条记录残缺）
        durableLog.rewriteAll(List.of("alpha", "beta", "gamma")); // 确保稳定文件
        try (RandomAccessFile raf = new RandomAccessFile(file.toFile(), "rw")) {
            long size = raf.length();
            raf.setLength(size - 3); // 砍掉最后一条载荷的尾部 3 字节
            log.info("模拟崩溃截断：文件大小 {} -> {}", size, size - 3);
        }

        List<String> after = new DurableLog(file).readAll();
        log.info("截断后读回={}（残缺尾部记录必须被忽略）", after);
        assertEquals(2, after.size(), "残缺的最后一条记录必须被忽略，不影响已刷盘记录");
        assertEquals(List.of("alpha", "beta"), after);

        // 在截断后的文件上继续追加仍然可用
        DurableLog reopened = new DurableLog(file);
        reopened.append("delta");
        List<String> continued = reopened.readAll();
        assertTrue(continued.contains("delta"));
        assertTrue(continued.containsAll(List.of("alpha", "beta")));
        log.info("截断后继续追加并读回={}", continued);

        // 压缩重写后只保留指定记录
        reopened.rewriteAll(List.of("one", "two"));
        assertEquals(List.of("one", "two"), new DurableLog(file).readAll());
        log.info("压缩重写后读回={}", new DurableLog(file).readAll());
    }
}
