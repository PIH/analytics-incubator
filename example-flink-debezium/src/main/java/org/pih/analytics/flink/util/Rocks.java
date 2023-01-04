
package org.pih.analytics.flink.util;

import org.apache.commons.lang3.SerializationUtils;
import org.rocksdb.Options;
import org.rocksdb.RocksDB;

import java.io.File;
import java.io.Serializable;
import java.nio.file.Files;

/**
 * Uses a Rocks DB as a key/value store.
 */
public class Rocks {

    private final RocksDB db;

    public Rocks(String databaseName) {
        RocksDB.loadLibrary();
        final Options options = new Options();
        options.setCreateIfMissing(true);
        File dbDir = new File("/tmp/flinktesting/rocksdb", databaseName + ".db");
        try {
            Files.createDirectories(dbDir.getParentFile().toPath());
            Files.createDirectories(dbDir.getAbsoluteFile().toPath());
            db = RocksDB.open(options, dbDir.getAbsolutePath());
        }
        catch (Exception e) {
            throw new RuntimeException("Unable to instantiate rocksdb", e);
        }
    }

    public void put(Serializable key, Serializable value) {
        System.out.println("Saving: " + key + "->" + value);
        try {
            db.put(SerializationUtils.serialize(key), SerializationUtils.serialize(value));
        }
        catch (Exception e) {
            throw new RuntimeException("Unable to put into rocks db", e);
        }
    }

    public <T extends Serializable> T get(Serializable key) {
        System.out.println("Getting: " + key);
        try {
            byte[] bytes = db.get(SerializationUtils.serialize(key));
            if (bytes != null) {
                return SerializationUtils.deserialize(bytes);
            }
            return null;
        }
        catch (Exception e) {
            throw new RuntimeException("Unable to get from rocks db", e);
        }
    }

    public void delete(Serializable key) {
        try {
            db.delete(SerializationUtils.serialize(key));
        }
        catch (Exception e) {
            throw new RuntimeException("Unable to delete from rocks db", e);
        }
    }

    public void close() {
        db.close();
    }
}