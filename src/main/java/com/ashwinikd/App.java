package com.ashwinikd;

import com.ashwinikd.mmqueue.file.MemoryMappedFileException;
import com.ashwinikd.mmqueue.file.MemoryMappedQueueFile;

import java.io.FileNotFoundException;
import java.io.IOException;

/**
 * Hello world!
 *
 */
public class App 
{
    public static void main( String[] args )
    {
        try {
            //MemoryMappedQueueFile mmqf = new MemoryMappedQueueFile("/tmp/test.mmqf", 1000L, 100, 32, false);
            //mmqf.close();
            MemoryMappedQueueFile mmqf = new MemoryMappedQueueFile("/tmp/test.mmqf");
            System.out.println(mmqf);
            mmqf.close();
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (MemoryMappedFileException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
