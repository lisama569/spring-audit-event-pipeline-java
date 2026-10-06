package com.github.lisama569.springauditeventpipelinejava.recovery;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.List;

/**
 * 长度前缀的追加型记录日志：
 * - 每条记录 = 4 字节大端长度 + UTF-8 载荷；
 * - 追加后 force(true) 落盘，崩溃时最多丢失尚未写完的一条，尾部残缺记录读取时忽略；
 * - 面向本地嵌入式单写者场景。
 */
public final class DurableLog {

    private final Path file;
    private FileChannel channel;

    public DurableLog(Path file) {
        this.file = file;
        try {
            Files.createDirectories(file.toAbsolutePath().getParent());
            // 先归一化：若上次崩溃留下残缺尾部，截断到最后一条完整记录边界，
            // 保证后续追加紧接完整记录写入，而不是写到旧（已失效）文件长度处。
            long validLength = scanValidLength(file);
            try (FileChannel ch = FileChannel.open(file, StandardOpenOption.CREATE,
                    StandardOpenOption.WRITE, StandardOpenOption.READ)) {
                if (ch.size() != validLength) {
                    ch.truncate(validLength);
                    ch.force(true);
                }
            }
            this.channel = FileChannel.open(file,
                    StandardOpenOption.CREATE, StandardOpenOption.WRITE, StandardOpenOption.APPEND);
        } catch (IOException e) {
            throw new UncheckedIOException("无法打开持久化日志: " + file, e);
        }
    }

    /** 扫描文件，返回最后一条完整记录之后的字节偏移（残缺尾部之后的有效长度）。 */
    private static long scanValidLength(Path path) throws IOException {
        if (!Files.exists(path)) {
            return 0L;
        }
        long valid = 0;
        try (FileChannel reader = FileChannel.open(path, StandardOpenOption.READ)) {
            long size = reader.size();
            long position = 0;
            ByteBuffer sizeBuffer = ByteBuffer.allocate(Integer.BYTES);
            while (size - position >= Integer.BYTES) {
                sizeBuffer.clear();
                readFullyAbsolute(reader, position, sizeBuffer);
                int length = sizeBuffer.getInt(0);
                if (length < 0) {
                    break;
                }
                long payloadStart = position + Integer.BYTES;
                if (payloadStart + length > size) {
                    break; // 残缺载荷：有效长度停在该记录之前
                }
                position = payloadStart + length;
                valid = position;
            }
        }
        return valid;
    }

    /** 从绝对偏移开始读满 buffer。 */
    private static void readFullyAbsolute(FileChannel channel, long position, ByteBuffer buffer)
            throws IOException {
        long pos = position;
        while (buffer.hasRemaining()) {
            int n = channel.read(buffer, pos);
            if (n == -1) {
                throw new IOException("意外的日志尾");
            }
            pos += n;
        }
    }

    /** 追加一条 UTF-8 记录并 fsync。 */
    public synchronized void append(String record) {
        byte[] payload = record.getBytes(StandardCharsets.UTF_8);
        try {
            channel.write(ByteBuffer.allocate(Integer.BYTES).putInt(payload.length).flip());
            channel.write(ByteBuffer.wrap(payload));
            channel.force(true);
        } catch (IOException e) {
            throw new UncheckedIOException("追加持久化记录失败: " + file, e);
        }
    }

    /** 从头读取全部完整记录（尾部截断/残缺记录自动丢弃）。 */
    public synchronized List<String> readAll() {
        List<String> records = new ArrayList<>();
        try (FileChannel reader = FileChannel.open(file, StandardOpenOption.READ)) {
            ByteBuffer sizeBuffer = ByteBuffer.allocate(Integer.BYTES);
            while (true) {
                sizeBuffer.clear();
                int head = readFully(reader, sizeBuffer);
                if (head == -1) {
                    break; // 干净的文件尾
                }
                if (head < Integer.BYTES) {
                    break; // 残缺长度头：崩溃留下的尾部，忽略
                }
                int length = sizeBuffer.getInt(0);
                if (length < 0) {
                    break; // 损坏的长度，保守停止
                }
                ByteBuffer payload = ByteBuffer.allocate(length);
                int body = readFully(reader, payload);
                if (body < length) {
                    break; // 残缺载荷：忽略该条及其后所有内容
                }
                records.add(StandardCharsets.UTF_8.decode(payload.flip()).toString());
            }
        } catch (IOException e) {
            throw new UncheckedIOException("读取持久化日志失败: " + file, e);
        }
        return records;
    }

    /** 截断后重写全部记录并 fsync（用于状态压缩）。 */
    public synchronized void rewriteAll(List<String> records) {
        try {
            channel.close();
            Path tmp = file.resolveSibling(file.getFileName() + ".tmp");
            try (FileChannel writer = FileChannel.open(tmp,
                    StandardOpenOption.CREATE, StandardOpenOption.WRITE, StandardOpenOption.TRUNCATE_EXISTING)) {
                for (String record : records) {
                    byte[] payload = record.getBytes(StandardCharsets.UTF_8);
                    writer.write(ByteBuffer.allocate(Integer.BYTES).putInt(payload.length).flip());
                    writer.write(ByteBuffer.wrap(payload));
                }
                writer.force(true);
            }
            try {
                Files.move(tmp, file, java.nio.file.StandardCopyOption.REPLACE_EXISTING,
                        java.nio.file.StandardCopyOption.ATOMIC_MOVE);
            } catch (IOException atomicNotSupported) {
                Files.move(tmp, file, java.nio.file.StandardCopyOption.REPLACE_EXISTING);
            }
            this.channel = FileChannel.open(file,
                    StandardOpenOption.CREATE, StandardOpenOption.WRITE, StandardOpenOption.APPEND);
        } catch (IOException e) {
            throw new UncheckedIOException("压缩重写持久化日志失败: " + file, e);
        }
    }

    /** 读满 buffer，文件尾返回 -1；返回实际读到的字节数。 */
    private static int readFully(FileChannel channel, ByteBuffer buffer) throws IOException {
        int total = 0;
        while (buffer.hasRemaining()) {
            int n = channel.read(buffer);
            if (n == -1) {
                return total == 0 ? -1 : total;
            }
            total += n;
        }
        return total;
    }
}
