package com.sysgears;

import java.io.File;

public class TestUtils {

    /**
     * Creates temporary subdirectory
     *
     * @param subdir subdirectory to create
     *
     * @return created subdirectory
     */
    static public File createTempSubdir(String subdir) {
        File subDir = new File(TestUtils.getTempDirectory(), subdir);
        if (!TestUtils.createEmptyDirectory(subDir)) {
            throw new RuntimeException("Error creating temporary subdirectory");
        }
        return subDir;
    }

    /**
     * Returns OS-specific temp directory
     *
     * @return temp directory
     */
    static public File getTempDirectory() {
        return new File(System.getProperty("java.io.tmpdir"));
    }

    /**
     * Creates empty directory.
     *
     * @param path path to create
     *
     * @return whether operation was successful
     */
    static public boolean createEmptyDirectory(File path) {
        return deleteDirectory(path) && path.mkdirs();
    }

    /**
     * Deletes directory recursively.
     *
     * @param path directory to delete
     *
     * @return whether operation was successful
     */
    static public boolean deleteDirectory(File path) {
        if (path.exists()) {
            File[] files = path.listFiles();
            if (files != null) {
                for (File file : files) {
                    deleteDirectory(file);
                }
            }
            return path.delete();
        }
        return true;
    }

}
