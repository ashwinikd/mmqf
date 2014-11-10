package com.ashwinikd.ds.mmqueue.file;

/**
 * Created by ashwini on 08/11/14.
 */
public class MemoryMappedFileException extends Exception {
    private MemoryMappedFileErrorCode errorCode;

    public MemoryMappedFileException(MemoryMappedFileErrorCode code) {
        super(code.toString());
        errorCode = code;
    }

    public MemoryMappedFileException(MemoryMappedFileErrorCode code, String message) {
        super(code.toString() + " " + message);
        errorCode = code;
    }

    public MemoryMappedFileException(String message, Throwable t) {
        super(MemoryMappedFileErrorCode.EXTERNAL_EXCEPTION.toString() + " " + message, t);
        errorCode = MemoryMappedFileErrorCode.EXTERNAL_EXCEPTION;
    }

    public MemoryMappedFileErrorCode getCode() {
        return errorCode;
    }
}
