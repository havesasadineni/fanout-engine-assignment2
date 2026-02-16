package com.fanout.engine;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.file.Path;

public class DlqWriter {
    private final Path path;

    public DlqWriter(Path path) {
        this.path = path;
    }

    public synchronized void append(String line) {
        try (BufferedWriter bw = new BufferedWriter(new FileWriter(path.toFile(), true))) {
            bw.write(line);
            bw.newLine();
        } catch (IOException e) {
            // Last resort: print; we still count it as DLQ attempt
            System.err.println("DLQ write failed to " + path + ": " + e.getMessage());
        }
    }
}
