package com.ashwinikd.mmqueue.file;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;

import com.ashwinikd.mmqueue.MemoryMappedQueue;

/**
 * Memory Mapped Queue File(MMQF).
 *
 * <p>
 *     Every MMQF records
 *     the capacity and slot size of the queue it holds.
 *     Capacity is the maximum number of elements the queue
 *     can hold. Slot size is the maximum number of bytes a
 *     single element of the queue can take when converted
 *     to byte array.
 * </p>
 *
 * <p>
 *     MMQF also records the serial version UID of class whose
 *     objects are stored in the queue. The serial version
 *     UID has to be specified at the time of creation of file.
 *     Once the file has been created this cannot be changed.
 * </p>
 *
 * <p>
 *     File format of Memory Mapped Queue File is given by
 *     {@link MemoryMappedQueueFileFormat}. Every file contains
 *     some header information and data. The size of header is
 *     varies for different versions of file format.
 * </p>
 *
 * @author Ashwini Dhekane[ashwini@ashwinidhekane.com]
 *
 * @see MemoryMappedQueueFileFormat
 * @see MemoryMappedFileException
 * @see MemoryMappedQueue
 *
 * @since 1.0
 */
public class MemoryMappedQueueFile {
    private RandomAccessFile file;
    private String path;
    private byte version;
    private int dataOffset;
    private long serialVersion;
    private int capacity;
    private int slotSize;

    /**
     * Create a memory mapped file from a given absolute path. If
     * the file does not exist this will throw {@link FileNotFoundException}.
     * If the file exists the file will be checked if it is a valid
     * MMQF.
     *
     * <p>
     *     After creation queue implementations should check if
     *     the serial version UID of stored objects is same as the class
     *     of elements the queue is going to store. This can be done using
     *     {@link #getSerialVersion()}.
     * </p>
     *
     * @param path file path
     * @throws FileNotFoundException
     * @throws MemoryMappedFileException
     *
     * @since 1.0
     */
    public MemoryMappedQueueFile(String path)
            throws FileNotFoundException, MemoryMappedFileException {
        init(new File(path));
    }

    /**
     * Create a memory mapped file from a given file. If thefile does
     * not exist this will throw {@link FileNotFoundException}.
     * If the file exists the file will be checked if it is a valid
     * MMQ file.
     *
     * <p>
     *     After creation queue implementations should check if
     *     the serial version UID of stored objects is same as the class
     *     of elements the queue is going to store. This can be done using
     *     {@link #getSerialVersion()}.
     * </p>
     *
     * @param f file
     * @throws FileNotFoundException
     * @throws MemoryMappedFileException
     *
     * @since 1.0
     */
    public MemoryMappedQueueFile(File f)
            throws FileNotFoundException, MemoryMappedFileException {
        init(f);
    }

    /**
     * Create a memory mapped file at given path. This will allocate
     * sufficient size on disk for the given capacity and slot size.
     * Serial version UID of element of the queue has to be specified
     * at the time of creation along with capacity and slot size.
     *
     * <p>
     *     If the file specified by {@code path} does not exist it will
     *     be created. If the file already exists and {@code overwrite}
     *     is set to {@code false} The file will be validated if it is
     *     a valid MMQF. If the validation is successful this will
     *     verify that the desired capacity and slot size are equal to
     *     the one recorded in file. If the capacity or slot size do not
     *     match this will throw {@link IllegalArgumentException}.
     * </p>
     *
     * <p>
     *     If the {@code overwrite} flag is set to {@code true} this
     *     will behave similarly as if the file didn't exist.
     * </p>
     *
     * @param path absolute path to file
     * @param serialVersionUid serial version uid of the class whose objects
     *                         are stored in this queue
     * @param capacity capacity of the queue
     * @param size slot size of the queue
     * @param overwrite should the file be overwritten if exists
     * @throws IOException
     * @throws MemoryMappedFileException
     * @throws IllegalArgumentException
     *
     * @since 1.0
     */
    public MemoryMappedQueueFile(String path, long serialVersionUid, int capacity, int size, boolean overwrite)
            throws IOException, MemoryMappedFileException, IllegalArgumentException {
        init(new File(path), serialVersionUid, capacity, size, overwrite);
    }

    /**
     * Create a memory mapped file from a given file. This will allocate
     * sufficient size on disk for the given capacity and slot size.
     * Serial version UID of element of the queue has to be specified
     * at the time of creation along with capacity and slot size.
     *
     * <p>
     *     If the file specified by {@code f} does not exist it will
     *     be created. If the file already exists and {@code overwrite}
     *     is set to {@code false} The file will be validated if it is
     *     a valid MMQF. If the validation is successful this will
     *     verify that the desired capacity and slot size are equal to
     *     the one recorded in file. If the capacity or slot size do not
     *     match this will throw {@link IllegalArgumentException}.
     * </p>
     *
     * <p>
     *     If the {@code overwrite} flag is set to {@code true} this
     *     will behave similarly as if the file didn't exist.
     * </p>
     *
     * @param f file
     * @param serialVersionUid serial version uid of the class whose objects
     *                         are stored in this queue
     * @param capacity capacity of the queue
     * @param size slot size of the queue
     * @param overwrite should the file be overwritten if exists
     * @throws IOException
     * @throws MemoryMappedFileException
     *
     * @since 1.0
     */
    public MemoryMappedQueueFile(File f, long serialVersionUid, int capacity, int size, boolean overwrite)
            throws IOException, MemoryMappedFileException {
        init(f, serialVersionUid, capacity, size, overwrite);
    }

    /**
     * Get the file format version of this file
     *
     * @return file format version
     * @since 1.0
     */
    public int getVersion() { return version; }

    /**
     * Get the offset to data. This depends on the
     *
     * version of the file format.
     * @return offset to data
     * @since 1.0
     */
    public int getOffsetToData() { return dataOffset; }

    /**
     * Get the serial version UID of the class whose
     * elements are stored in the queue.
     *
     * @return serial version UID of class
     * @since 1.0
     */
    public long getSerialVersion() {
        return serialVersion;
    }

    /**
     * Get the capacity of the queue.
     *
     * @return queue capacity
     * @since 1.0
     */
    public int getCapacity() {
        return capacity;
    }

    /**
     * Get the slot size of the queue.
     *
     * @return slot size
     * @size 1.0
     */
    public int getSlotSize() {
        return slotSize;
    }

    /**
     *Create a memory mapped file from a given file. This will allocate
     * sufficient size on disk for the given capacity and slot size.
     * Serial version UID of element of the queue has to be specified
     * at the time of creation along with capacity and slot size.
     *
     * <p>
     *     If the file specified by {@code f} does not exist it will
     *     be created. If the file already exists and {@code overwrite}
     *     is set to {@code false} The file will be validated if it is
     *     a valid MMQF. If the validation is successful this will
     *     verify that the desired capacity and slot size are equal to
     *     the one recorded in file. If the capacity or slot size do not
     *     match this will throw {@link IllegalArgumentException}.
     * </p>
     *
     * <p>
     *     If the {@code overwrite} flag is set to {@code true} this
     *     will behave similarly as if the file didn't exist.
     * </p>
     *
     * @param f file
     * @param serialVersionUid serial version uid of the class whose objects
     *                         are stored in this queue
     * @param cap capacity of the queue
     * @param size slot size of the queue
     * @param overwrite should the file be overwritten if exists
     * @throws IOException
     * @throws MemoryMappedFileException
     *
     * @since 1.0
     */
    private void init(File f, long serialVersionUid, int cap, int size, boolean overwrite)
            throws IOException, MemoryMappedFileException {
        if (! overwrite && f.exists()) {
            throw new IllegalArgumentException("File " + f.getAbsolutePath() + " already exists");
        }
        file = MemoryMappedQueueFileFormat.create(f, serialVersionUid, size, cap);
        version = MemoryMappedQueueFileFormat.getVersion(file);
        dataOffset = MemoryMappedQueueFileFormat.getOffsetToData(file);
        serialVersion = MemoryMappedQueueFileFormat.getSerialVersionUid(file);
        capacity = MemoryMappedQueueFileFormat.getCapacity(file);
        slotSize = MemoryMappedQueueFileFormat.getSlotSize(file);
        path = f.getAbsolutePath();
    }

    /**
     * Create a memory mapped file from a given file. If thefile does
     * not exist this will throw {@link FileNotFoundException}.
     * If the file exists the file will be checked if it is a valid
     * MMQ file.
     *
     * <p>
     *     After creation queue implementations should check if
     *     the serial version UID of stored objects is same as the class
     *     of elements the queue is going to store. This can be done using
     *     {@link #getSerialVersion()}.
     * </p>
     *
     * @param f file
     * @throws FileNotFoundException
     * @throws MemoryMappedFileException
     *
     * @since 1.0
     */
    private void init(File f)
            throws FileNotFoundException, MemoryMappedFileException {
        if(! f.exists()) {
            throw new FileNotFoundException();
        }
        file = new RandomAccessFile(f, "rw");
        MemoryMappedQueueFileFormat.validate(file);
        version = MemoryMappedQueueFileFormat.getVersion(file);
        dataOffset = MemoryMappedQueueFileFormat.getOffsetToData(file);
        serialVersion = MemoryMappedQueueFileFormat.getSerialVersionUid(file);
        capacity = MemoryMappedQueueFileFormat.getCapacity(file);
        slotSize = MemoryMappedQueueFileFormat.getSlotSize(file);
        path = f.getAbsolutePath();
    }

    public MappedByteBuffer getDataBuffer() throws IOException, MemoryMappedFileException {
        int offset = MemoryMappedQueueFileFormat.getOffsetToData(file);
        return file.getChannel().map(FileChannel.MapMode.READ_WRITE, offset, file.length() - offset);
    }

    /**
     * Close the file.
     *
     * @throws IOException
     *
     * @since 1.0
     */
    public void close() throws IOException {
        file.close();
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder().
                append("[ Memory Mapped Queue File ]").append("\n\t").
                append("File path=").append(path).append("\n\t").
                append("Version=").append(version).append("\n\t").
                append("Class Serial Version UID=").append(serialVersion).append("\n\t").
                append("Capacity=").append(capacity).append("\n\t").
                append("Slot Size=").append(slotSize).append("\n\t").
                append("Offset to data=").append(dataOffset);
        return sb.toString();
    }
}
