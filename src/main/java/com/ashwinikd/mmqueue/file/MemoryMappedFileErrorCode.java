package com.ashwinikd.mmqueue.file;

/**
 * Error codes for {@link MemoryMappedFileException}
 *
 * @author Ashwini Dhekane [ashwini@ashwinidhekane.com]
 *
 * @see MemoryMappedFileException
 * @see MemoryMappedQueueFile
 *
 * @since 1.0
 */
public enum MemoryMappedFileErrorCode {
    /**
     * No error occured.
     *
     * @since 1.0
     */
    NO_ERROR(100, "Success"),

    /**
     * File error due to external error eg. IOException.
     *
     * @since 1.0
     */
    EXTERNAL_EXCEPTION(500, "Runtime Exception"),

    /**
     * File being read is not a Memory Mapped Queue File.
     *
     * @since 1.0
     */
    INVALID_FILE_FORMAT(401, "File is not a Memory mapped queue file."),

    /**
     * File version is not supported.
     *
     * @since 1.0
     */
    VERSION_NOT_SUPPORTED(405, "Current file version is not supported."),

    /**
     * Header checksum do not match.
     *
     * @since 1.0
     */
    CHECKSUM_NOT_EQUAL(501, "Calculated header checksum is not equal to written checksum"),

    /**
     * File length do not match. Generally a sign of incomplete download.
     *
     * @since 1.0
     */
    FILE_NOT_COMPLETE(502, "File is curropted. File download was not completed.");

    private int code;
    private String description;

    private MemoryMappedFileErrorCode(int errorCode, String desc) {
        this.code = errorCode;
        this.description = desc;
    }

    /**
     * Get the integer error code
     *
     * @since 1.0
     */
    public int getCode() {
        return code;
    }

    /**
     * Get the error description
     *
     * @since 1.0
     */
    public String getDescription() {
        return description;
    }

    @Override
    public String toString() {
        return "[Error Code=" + code + "] " + description + ".";
    }
}
