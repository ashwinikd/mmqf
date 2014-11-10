package com.ashwinikd.ds.mmqueue.file;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.FileLock;
import java.util.zip.CRC32;

/**
 * File format for memory mapped queue
 *
 * All values in little-endian
 *  +00 8B Magic Value [\NUL M M Q F EOT SUB \n]
 *  +08 1B Version (currently 0)
 *  +09 4B CRC32 checksum of header to follow
 *  +13 8B length of the file
 *  +21 8B serial version uid of class
 *  +29 4B capacity of the queue
 *  +33 4B size of serialized objects
 *  +37 2B offset to data
 *  +39 .. data
 *
 * @author Ashwini Dhekane<ashwini@ashwinidhekane.com>
 *
 * @see MemoryMappedFileException
 * @see MemoryMappedQueueFile
 *
 * @since 1.0
 */
public final class MemoryMappedQueueFileFormat {

    /**
     * File extension and mime type
     *
     * @since 1.0
     */
    public static final String FILE_EXTENSION = "MMQF";
    public static final String MIME_TYPE = "application/octet-stream";

    /**
     * File magic value, its position and size in header
     *
     * @since 1.0
     */
    public static final byte[] FILE_MAGIC_VALUE = {
            0x00, // NUL
            0x4d, // M
            0x4d, // M
            0x51, // Q
            0x46, // F
            0x03, // EOT
            0x1A, // SUB
            0x0A  // \n
    };

    /**
     * Position of magic value in the file header
     *
     * @since 1.0
     */
    public static final int HD_POS_MAGIC_VAL = 0;

    /**
     * Size of the magic value in the file header
     *
     * @since 1.0
     */
    public static final int HD_SIZ_MAGIC_VAL = 8;

    /**
     * Current version of the format
     *
     * @since 1.0
     */
    public static final byte VERSION = 0x00;

    /**
     * Minimum version supported
     *
     * @since 1.0
     */
    public static final byte MIN_SUPPORTED_VERSION = 0x00;

    /**
     * Position of version in the file header
     *
     * @since 1.0
     */
    public static final int HD_POS_VERSION = HD_POS_MAGIC_VAL + HD_SIZ_MAGIC_VAL;

    /**
     * Size of version in the file header
     *
     * @since 1.0
     */
    public static final int HD_SIZ_VERSION = 1;

    /**
     * Position of CRC32 checksum in file header
     *
     * @since 1.0
     */
    public static final int HD_POS_CHECKSUM = HD_POS_VERSION + HD_SIZ_VERSION;

    /**
     * Size of CRC32 checksum in file header
     *
     * @since 1.0
     */
    public static final int HD_SIZ_CHECKSUM = 4;

    /**
     * Position of Length of file in file header
     *
     * @since 1.0
     */
    public static final int HD_POS_LENGTH = HD_POS_CHECKSUM + HD_SIZ_CHECKSUM;

    /**
     * Size of file length in file header
     *
     * @since 1.0
     */
    public static final int HD_SIZ_LENGTH = 8;

    /**
     * Position of serial version uid of serialized class
     *
     * @since 1.0
     */
    public static final int HD_POS_SERIALVERUID = HD_POS_LENGTH + HD_SIZ_LENGTH;

    /**
     * Size of serial version uid
     *
     * @since 1.0
     */
    public static final int HD_SIZ_SERIALVERUID = 8;

    /**
     * Position of capacity of queue
     *
     * @since 1.0
     */
    public static final int HD_POS_CAPACITY = HD_POS_SERIALVERUID + HD_SIZ_SERIALVERUID;

    /**
     * Size of capacity of queue
     *
     * @since 1.0
     */
    public static final int HD_SIZ_CAPACITY = 4;

    /**
     * Position of size of serialized object
     *
     * @since 1.0
     */
    public static final int HD_POS_SIZE = HD_POS_CAPACITY + HD_SIZ_CAPACITY;

    /**
     * Size of serialized object size
     *
     * @since 1.0
     */
    public static final int HD_SIZ_SIZE = 4;

    /**
     * Position of offset to data in file header
     *
     * @since 1.0
     */
    public static final int HD_POS_DATA_OFFSET = HD_POS_SIZE + HD_SIZ_SIZE;

    /**
     * Size of offset to data in file header
     *
     * @since 1.0
     */
    public static final int HD_SIZ_DATA_OFFSET = 2;

    /**
     * Offset to data in current version of file format
     *
     * @since 1.0
     */
    public static final int DATA_OFFSET = HD_SIZ_MAGIC_VAL + HD_SIZ_VERSION +
            HD_SIZ_CHECKSUM + HD_SIZ_LENGTH + HD_SIZ_SERIALVERUID + HD_SIZ_CAPACITY +
            HD_SIZ_SIZE + HD_SIZ_DATA_OFFSET;

    /**
     * Validate that the given file is valid Memory mapped queue file and is
     * supported by current code. If file is not valid this will throw a
     * {@link MemoryMappedFileException}. Reason for error can be checked using
     * {@link MemoryMappedFileException#getCode()}.
     *
     * @param file the file to check.
     * @throws MemoryMappedFileException if file is not valid or IO error occurs
     *
     * @since 1.0
     */
    public static void validate(RandomAccessFile file) throws MemoryMappedFileException {
        checkMagicValue(file);
        checkVersion(file);
        verifyChecksum(file);
        checkFileLength(file);
    }

    /**
     * Get the offset to data in the current file. This does not check if the
     * given file is a valid/supported file. Developer should first check that
     * the file is supported using {@link #validate(java.io.RandomAccessFile)}
     * before reading the offset. If file is not valid this method will still
     * read the bytes representing the offset and return the result.
     *
     * @param file the file to check
     * @return offset to data.
     * @throws MemoryMappedFileException
     *
     * @since 1.0
     */
    static int getOffsetToData(RandomAccessFile file) throws MemoryMappedFileException {
        try {
            if (file.length() < HD_POS_DATA_OFFSET + HD_SIZ_DATA_OFFSET)
                throw new MemoryMappedFileException(MemoryMappedFileErrorCode.INVALID_FILE_FORMAT,
                        "Cannot read offset to data.");
            file.seek(HD_POS_DATA_OFFSET);
            ByteBuffer buffer = ByteBuffer.allocate(HD_SIZ_DATA_OFFSET);
            buffer.order(ByteOrder.LITTLE_ENDIAN);
            for (int i = 0; i < HD_SIZ_DATA_OFFSET; i++) {
                buffer.put(file.readByte());
            }
            buffer.rewind();
            return buffer.getShort();
        } catch (IOException e) {
            throw new MemoryMappedFileException("Error in reading file", e);
        }
    }

    /**
     * Get the serial version uid in the current file. This does not check if the
     * given file is a valid/supported file. Developer should first check that
     * the file is supported using {@link #validate(java.io.RandomAccessFile)}
     * before reading the offset. If file is not valid this method will still
     * read the bytes representing the offset and return the result.
     *
     * @param file the file to check
     * @return serial version uid.
     * @throws MemoryMappedFileException
     *
     * @since 1.0
     */
    static long getSerialVersionUid(RandomAccessFile file) throws MemoryMappedFileException {
        try {
            if (file.length() < HD_POS_SERIALVERUID + HD_SIZ_SERIALVERUID)
                throw new MemoryMappedFileException(MemoryMappedFileErrorCode.INVALID_FILE_FORMAT,
                        "Cannot read serial version uid.");
            file.seek(HD_POS_SERIALVERUID);
            ByteBuffer buffer = ByteBuffer.allocate(HD_SIZ_SERIALVERUID);
            buffer.order(ByteOrder.LITTLE_ENDIAN);
            for (int i = 0; i < HD_SIZ_SERIALVERUID; i++) {
                buffer.put(file.readByte());
            }
            buffer.rewind();
            return buffer.getLong();
        } catch (IOException e) {
            throw new MemoryMappedFileException("Error in reading file", e);
        }
    }

    /**
     * Get the capacity of queue in the current file. This does not check if the
     * given file is a valid/supported file. Developer should first check that
     * the file is supported using {@link #validate(java.io.RandomAccessFile)}
     * before reading the offset. If file is not valid this method will still
     * read the bytes representing the offset and return the result.
     *
     * @param file the file to check
     * @return serial version uid.
     * @throws MemoryMappedFileException
     *
     * @since 1.0
     */
    static int getCapacity(RandomAccessFile file) throws MemoryMappedFileException {
        try {
            if (file.length() < HD_POS_CAPACITY + HD_SIZ_CAPACITY)
                throw new MemoryMappedFileException(MemoryMappedFileErrorCode.INVALID_FILE_FORMAT,
                        "Cannot read serial version uid.");
            file.seek(HD_POS_CAPACITY);
            ByteBuffer buffer = ByteBuffer.allocate(HD_SIZ_CAPACITY);
            buffer.order(ByteOrder.LITTLE_ENDIAN);
            for (int i = 0; i < HD_SIZ_CAPACITY; i++) {
                buffer.put(file.readByte());
            }
            buffer.rewind();
            return buffer.getInt();
        } catch (IOException e) {
            throw new MemoryMappedFileException("Error in reading file", e);
        }
    }

    /**
     * Get the slot size of element in the current file. This does not check if the
     * given file is a valid/supported file. Developer should first check that
     * the file is supported using {@link #validate(java.io.RandomAccessFile)}
     * before reading the offset. If file is not valid this method will still
     * read the bytes representing the offset and return the result.
     *
     * @param file the file to check
     * @return slot size.
     * @throws MemoryMappedFileException
     *
     * @since 1.0
     */
    static int getSlotSize(RandomAccessFile file) throws MemoryMappedFileException {
        try {
            if (file.length() < HD_POS_SIZE + HD_SIZ_SIZE)
                throw new MemoryMappedFileException(MemoryMappedFileErrorCode.INVALID_FILE_FORMAT,
                        "Cannot read serial version uid.");
            file.seek(HD_POS_SIZE);
            ByteBuffer buffer = ByteBuffer.allocate(HD_SIZ_SIZE);
            buffer.order(ByteOrder.LITTLE_ENDIAN);
            for (int i = 0; i < HD_SIZ_SIZE; i++) {
                buffer.put(file.readByte());
            }
            buffer.rewind();
            return buffer.getInt();
        } catch (IOException e) {
            throw new MemoryMappedFileException("Error in reading file", e);
        }
    }

    /**
     * Get the version of the file format
     *
     * @param file file to check
     * @return version of format
     * @throws MemoryMappedFileException if IO error occurs
     *
     * @since 1.0
     */
    static byte getVersion(RandomAccessFile file) throws MemoryMappedFileException {
        try {
            file.seek(HD_POS_VERSION);
            return file.readByte();
        } catch (IOException e) {
            throw new MemoryMappedFileException("Error in reading file", e);
        }
    }

    /**
     * Create a new Memory Mapped file. This will overwrite an existing file.
     *
     * @param file file to be created
     * @param serial Serial version uid of class whose objects are being stored in queue
     * @param size size of serialized object
     * @param capacity capacity of the queue
     * @return created file
     * @throws IOException
     *
     * @since 1.0
     */
    static RandomAccessFile create(File file, long serial, int size, int capacity) throws IOException {
        RandomAccessFile raf = new RandomAccessFile(file, "rw");
        FileLock lock = raf.getChannel().tryLock();
        try {
            int dataSize = size * capacity + 32;
            raf.setLength(DATA_OFFSET + dataSize);

            /*  Write magic value  */
            raf.seek(HD_POS_MAGIC_VAL);
            raf.write(FILE_MAGIC_VALUE);

            /*  Write Version Number  */
            raf.seek(HD_POS_VERSION);
            raf.writeByte(VERSION);

            byte[] headerBytes = new byte[DATA_OFFSET - HD_POS_LENGTH];
            int byteCursor = 0;

            /*  Write length  */
            raf.seek(HD_POS_LENGTH);
            long length = raf.length();
            for (int i = 0; i < HD_SIZ_LENGTH; i++) {
                byte b = (byte) (length >> (i * 8));
                headerBytes[byteCursor] = b;
                byteCursor++;
                raf.writeByte(b);
            }
            System.out.println();

            /*  Write serial version uid  */
            raf.seek(HD_POS_SERIALVERUID);
            for (int i = 0; i < HD_SIZ_SERIALVERUID; i++) {
                byte b = (byte) ((serial >> (i * 8)) & 0xFF);
                headerBytes[byteCursor] = b;
                byteCursor++;
                raf.writeByte(b);
            }

            /*  Write capacity  */
            raf.seek(HD_POS_CAPACITY);
            for (int i = 0; i < HD_SIZ_CAPACITY; i++) {
                byte b = (byte) ((capacity >> (i * 8)) & 0xFF);
                headerBytes[byteCursor] = b;
                byteCursor++;
                raf.writeByte(b);
            }

            /*  Write class size  */
            raf.seek(HD_POS_SIZE);
            for (int i = 0; i < HD_SIZ_SIZE; i++) {
                byte b = (byte) ((size >> (i * 8)) & 0xFF);
                headerBytes[byteCursor] = b;
                byteCursor++;
                raf.writeByte(b);
            }

            /*  Write offset to data  */
            raf.seek(HD_POS_DATA_OFFSET);
            for (int i = 0; i < HD_SIZ_DATA_OFFSET; i++) {
                byte b = (byte) ((DATA_OFFSET >> (i * 8)) & 0xFF);
                headerBytes[byteCursor] = b;
                byteCursor++;
                raf.writeByte(b);
            }

            /*  Write checksum  */
            CRC32 crc32 = new CRC32();
            crc32.update(headerBytes);
            long checksum = crc32.getValue();
            raf.seek(HD_POS_CHECKSUM);
            for (int i = 0; i < HD_SIZ_CHECKSUM; i++) {
                byte b = (byte) ((checksum >> (i * 8)) & 0xFF);
                raf.writeByte(b);
            }

            return raf;
        } finally {
            lock.release();
        }
    }

    /**
     * Verifies that the magic value written in the file matches the
     * value for this file format. If values do not match
     * {@link MemoryMappedFileException} will be thrown
     *
     * @param file file to check
     * @throws MemoryMappedFileException if values do not match or IO error
     *   occurs.
     *
     * @since 1.0
     */
    private static void checkMagicValue(RandomAccessFile file) throws MemoryMappedFileException {
        try {
            if (file.length() - HD_POS_MAGIC_VAL < HD_SIZ_MAGIC_VAL) {
                throw new MemoryMappedFileException(MemoryMappedFileErrorCode.INVALID_FILE_FORMAT,
                        "Cannot read magic value");
            }
            file.seek(HD_POS_MAGIC_VAL);
            for (int i = 0; i < HD_SIZ_MAGIC_VAL; i++) {
                if(file.readByte() != FILE_MAGIC_VALUE[i])
                    throw new MemoryMappedFileException(MemoryMappedFileErrorCode.INVALID_FILE_FORMAT,
                            "Magic value does not match");
            }
        } catch (IOException e) {
            throw new MemoryMappedFileException("Error in reading file", e);
        }
    }

    /**
     * Check if the current version of file is supported.
     *
     * @param file file to check
     * @throws MemoryMappedFileException if file is not supported or IO error occurs
     *
     * @since 1.0
     */
    private static void checkVersion(RandomAccessFile file) throws MemoryMappedFileException {
        byte version = getVersion(file);
        if (version < MIN_SUPPORTED_VERSION || version > VERSION) {
            throw new MemoryMappedFileException(MemoryMappedFileErrorCode.VERSION_NOT_SUPPORTED);
        }
    }

    /**
     * Verify CRC32 checksum of the header.
     *
     * @param file file to check for
     * @throws MemoryMappedFileException if checksum does not match or IO error occurs
     *
     * @since 1.0
     */
    private static void verifyChecksum(RandomAccessFile file) throws MemoryMappedFileException {
        int offsetToData = getOffsetToData(file);
        try {
            long fileSize = file.length();
            if (offsetToData > fileSize) {
                throw new MemoryMappedFileException(MemoryMappedFileErrorCode.INVALID_FILE_FORMAT);
            }
            byte[] header = new byte[offsetToData - HD_POS_LENGTH];
            file.seek(HD_POS_LENGTH);
            for (int i = 0; i < header.length; i++) {
                header[i] = file.readByte();
            }
            CRC32 crc32 = new CRC32();
            crc32.update(header);
            long crc32Sum = crc32.getValue();

            file.seek(HD_POS_CHECKSUM);
            for (int i = 0; i < HD_SIZ_CHECKSUM; i++) {
                byte b = (byte) ((crc32Sum >> (i * 8)) & 0xFF);
                if (b != file.readByte()) {
                    throw new MemoryMappedFileException(MemoryMappedFileErrorCode.CHECKSUM_NOT_EQUAL);
                }
            }
        } catch (IOException e) {
            throw new MemoryMappedFileException("Could not read header", e);
        }
    }

    /**
     * Validates that the file length is equal to the length of file recorded in
     * header.
     *
     * @param file file to check
     * @throws MemoryMappedFileException if file length does not match or IO error occurs
     *
     * @since 1.0
     */
    private static void checkFileLength(RandomAccessFile file) throws MemoryMappedFileException {
        try {
            file.seek(HD_POS_LENGTH);
            ByteBuffer buffer = ByteBuffer.allocate(HD_SIZ_LENGTH);
            buffer.order(ByteOrder.LITTLE_ENDIAN);
            long len = 0;
            buffer.put(file.readByte());
            for (int i = 1; i < HD_SIZ_LENGTH; i++) {
                byte b = file.readByte();
                buffer.put(b);
            }
            buffer.rewind();
            len = buffer.getLong();
            if (len != file.length()) {
                throw new MemoryMappedFileException(MemoryMappedFileErrorCode.FILE_NOT_COMPLETE);
            }
            System.out.println();
        } catch (IOException e) {
            throw new MemoryMappedFileException("IOError", e);
        }
    }
}
