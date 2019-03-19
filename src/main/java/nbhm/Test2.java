package nbhm;

import org.jctools.maps.NonBlockingHashMap;

import java.util.ArrayDeque;
import java.util.Deque;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;


public class Test2 {

    // thread local object pools
    private static final int THREADLOCAL_CHAR_ARRAY_POOL_SIZE = 24;
    private static final ThreadLocal<Deque<char[]>> THREADLOCAL_CHAR_ARRAY_POOL = ThreadLocal.withInitial(() -> new ArrayDeque<>(THREADLOCAL_CHAR_ARRAY_POOL_SIZE));
    static {
        for (int i = 0; i < THREADLOCAL_CHAR_ARRAY_POOL_SIZE; i++) {
            THREADLOCAL_CHAR_ARRAY_POOL.get().push(new char[42]);
        }
    }

    private static final int THREADLOCAL_ENTRY_KEY_POOL_SIZE = 24;
    private static final ThreadLocal<Deque<EntryKey>> THREADLOCAL_ENTRY_KEY_POOL = ThreadLocal.withInitial(() -> new ArrayDeque<>(THREADLOCAL_ENTRY_KEY_POOL_SIZE));
    static {
        for (int i = 0; i < THREADLOCAL_ENTRY_KEY_POOL_SIZE; i++) {
            THREADLOCAL_ENTRY_KEY_POOL.get().push(new EntryKey());
        }
    }

    private static final int THREADLOCAL_ENTRY_POOL_SIZE = 12;
    private static final ThreadLocal<Deque<Entry>> THREADLOCAL_ENTRY_POOL = ThreadLocal.withInitial(() -> new ArrayDeque<>());
    static {
        for (int i = 0; i < THREADLOCAL_ENTRY_POOL_SIZE; i++) {
            THREADLOCAL_ENTRY_POOL.get().push(new Entry());
        }
    }


    /**
     * Attempting to acquire a lock
     */
    private final Map<EntryKey, Entry> attempting;

    /**
     * Acquired locks
     */
    private final Map<EntryKey,Entry> acquired;

    private final boolean poolObjects;

    public Test2(final boolean useNBHM, final boolean poolObjects) {
        if (useNBHM) {
            attempting = new NonBlockingHashMap<>();
            acquired = new NonBlockingHashMap<>();
        } else {
            attempting = new ConcurrentHashMap<>();
            acquired = new ConcurrentHashMap<>();
        }
        this.poolObjects = poolObjects;
    }


    private void event(final LockEventType lockEventType, final String id, final LockType lockType, final LockMode lockMode) {

        final Thread currentThread = Thread.currentThread();
        final String threadName = currentThread.getName();
        final long threadId = currentThread.getId();

        switch (lockEventType) {
            case Attempt:
//                boolean took = false;
                Entry entry = poolObjects ? THREADLOCAL_ENTRY_POOL.get().poll() : null;
                if (entry == null) {
                    entry = new Entry();
                }// else {
                   // took = true;
//                }
                entry.id = id;
                entry.lockType = lockType;
                entry.lockMode = lockMode;
                entry.owner = threadName;
                // write count last to ensure reader-thread visibility of above fields
                entry.count = 1;

                // this key will be released in either `AttemptFailed`, `Acquired` (if merging), or `Released`
                final EntryKey entryKey = key(threadId, id, lockType, lockMode);
                entry.entryKey = entryKey;

//                if (took) {
//                    System.out.println("Took entry=" + entry.hashCode());
//                }

                attempting.put(entryKey, entry);
                break;

            case Acquired:
                final EntryKey attemptEntryKey = key(threadId, id, lockType, lockMode);

                final Entry attemptEntry = attempting.remove(attemptEntryKey);
                if (attemptEntry == null) {
                    System.err.println("No entry found when trying to remove `attempt` to promote to `acquired` for: id=" + id + ", EntryKey.hashCode=" + attemptEntryKey.hashCode());

                    // release the key that we used for the lookup
                    releaseEntryKey(attemptEntryKey);

                    break;
                }

                // release the key that we used for the lookup
                releaseEntryKey(attemptEntryKey);

                // we now either add or merge the `attemptEntry` with the `acquired` table
                Entry acquiredEntry = acquired.get(attemptEntry.entryKey);

                if (acquiredEntry == null) {
                    acquired.put(attemptEntry.entryKey, attemptEntry);
                    acquiredEntry = attemptEntry;
                } else {

                    acquiredEntry.count += attemptEntry.count;

                    // release the attempt entry (as we merged, rather than added)
                    releaseEntry(attemptEntry);
                }

                break;


            case Released:
                final EntryKey acquiredEntryKey = key(threadId, id, lockType, lockMode);

                final Entry releasedEntry = acquired.get(acquiredEntryKey);
                if (releasedEntry == null) {
                    System.err.println("No entry found when trying to `release` for: id=" + id + ", EntryKey.hashCode=" + acquiredEntryKey.hashCode());

                    // release the key that we used for the lookup
                    releaseEntryKey(acquiredEntryKey);

                    break;
                }

                // release the key that we used for the lookup
                releaseEntryKey(acquiredEntryKey);

                final int localCount = releasedEntry.count;

                // decrement
                releasedEntry.count = localCount - 1;

                if (releasedEntry.count == 0) {
                    // remove the entry
                    if (acquired.remove(releasedEntry.entryKey) == null) {
                        System.err.println("Unable to remove entry for `release`: id=" + id + ", EntryKey.hashCode=" + releasedEntry.entryKey.hashCode());
                    }

                    // release the entry
                    //TODO(AR) why can't  we have this line -- seems to mess with the entries in the attempt and acquired NonBlockingHashMaps?
                    releaseEntry(releasedEntry);
                }

                break;
        }
    }

    private void releaseEntryKey(final EntryKey entryKey) {
        if (poolObjects) {
            if (THREADLOCAL_CHAR_ARRAY_POOL.get().size() < THREADLOCAL_CHAR_ARRAY_POOL_SIZE) {
//                System.out.println("Releasing char[" + entryKey.bufLen + "]");
                THREADLOCAL_CHAR_ARRAY_POOL.get().push(entryKey.buf);
            }
            if (THREADLOCAL_ENTRY_KEY_POOL.get().size() < THREADLOCAL_ENTRY_KEY_POOL_SIZE) {
//                System.out.println("Releasing key: " + entryKey.hashCode());
                entryKey.setBuf(null, 0);
                THREADLOCAL_ENTRY_KEY_POOL.get().push(entryKey);
            } else {
                entryKey.setBuf(null, 0);
            }
        }
    }

    private void releaseEntry(final Entry entry) {
        if (poolObjects) {
//            System.out.println("Releasing entry: " + entry.hashCode());
            releaseEntryKey(entry.entryKey);
            entry.entryKey = null;
            if (THREADLOCAL_ENTRY_POOL.get().size() < THREADLOCAL_ENTRY_POOL_SIZE) {
                THREADLOCAL_ENTRY_POOL.get().push(entry);
            }
        }
    }

    public enum LockEventType {
        Attempt,
        Acquired,
        Released
    }

    private EntryKey key(final long threadId, final String id, final LockType lockType, final LockMode lockMode) {
        final boolean idIsUri = lockType == LockType.COLLECTION || lockType == LockType.DOCUMENT;

        final int requiredLen = 8 + 1 + (id.length() - (idIsUri ? (id.equals("/db") ? 3 : 4) : 0));

        char[] buf = poolObjects ? THREADLOCAL_CHAR_ARRAY_POOL.get().poll() : null;
        if (buf == null || buf.length < requiredLen) {
            buf = new char[requiredLen];
        }// else {
//            System.out.println("Took char[" + requiredLen + "]");
        //}

        longToChar(threadId, buf);
        buf[8] = (char) ((lockMode.getVal() << 4) | lockType.getVal());

        if (idIsUri) {
            appendUri(buf, 9, requiredLen, id);
        } else {
            id.getChars(0, id.length(), buf, 9);
        }

//        boolean took = false;
        EntryKey key = poolObjects ? THREADLOCAL_ENTRY_KEY_POOL.get().poll() : null;
        if (key == null) {
            key = new EntryKey();
        } // else {
        //    took = true;
        //}
        key.setBuf(buf, requiredLen);


//        if (took) {
//            System.out.println("Took key=" + key.hashCode());
//        }

        return key;
    }

    private static void longToChar(final long v, final char[] data) {
        data[0] = (char) ((v >>> 0) & 0xff);
        data[1] = (char) ((v >>> 8) & 0xff);
        data[2] = (char) ((v >>> 16) & 0xff);
        data[3] = (char) ((v >>> 24) & 0xff);
        data[4] = (char) ((v >>> 32) & 0xff);
        data[5] = (char) ((v >>> 40) & 0xff);
        data[6] = (char) ((v >>> 48) & 0xff);
        data[7] = (char) ((v >>> 56) & 0xff);
    }

    private static void appendUri(final char[] buf, int bufOffset, final int bufLen, final String id) {
        int partEnd = id.length() - 1;
        for (int i = partEnd; bufOffset < bufLen; i--) {
            final char c = id.charAt(i);
            if (c == '/') {
                id.getChars(i + 1, partEnd + 1, buf, bufOffset);
                bufOffset += partEnd - i;
                partEnd = i - 1;
                if (bufOffset < bufLen) {
                    buf[bufOffset++] = '/';
                }
            }
        }
    }

    private static class EntryKey {
        private char[] buf;
        private int bufLen;
        private int hashCode;

        public void setBuf(final char buf[], final int bufLen) {
            this.buf = buf;
            this.bufLen = bufLen;

            // calculate hashcode
            hashCode = 1;
            for (int i = 0; i < bufLen; i++)
                hashCode = 31 * hashCode + buf[i];
        }

        @Override
        public int hashCode() {
            return hashCode;
        }

        @Override
        public boolean equals(final Object o) {
            if (this == o) return true;
            if (o == null || EntryKey.class != o.getClass()) return false;

            final EntryKey other = (EntryKey)o;

            if (buf == other.buf)
                return true;

            if (other.bufLen != bufLen)
                return false;

            for (int i = 0; i < bufLen; i++)
                if (buf[i] != other.buf[i])
                    return false;

            return true;
        }
    }

    /**
     * Represents an entry in the {@link #attempting} or {@link #acquired} lock table.
     *
     * All class members are only written from a single
     * thread.
     *
     * However, they may be read from the same writer thread or a different read-only thread.
     * The member `count` is written last by the writer thread
     * and read first by the read-only reader thread to ensure correct visibility
     * of the member values.
     */
    public static class Entry {
        String id;
        LockType lockType;
        LockMode lockMode;
        String owner;

        /**
         * Intentionally marked volatile.
         * All variables visible before this point become available
         * to the reading thread.
         */
        volatile int count;

        /**
         * Used as a reference so that we can recycle the Map entry
         * key for reuse when we are done with this value.
         *
         * NOTE: Only ever read and written from the same thread
         */
        EntryKey entryKey;


        private Entry() {
        }

        private Entry(final String id, final LockType lockType, final LockMode lockMode, final String owner,
                      final StackTraceElement[] stackTrace) {
            this.id = id;
            this.lockType = lockType;
            this.lockMode = lockMode;
            this.owner = owner;
            // write last to ensure reader visibility of above fields!
            this.count = 1;
        }

        @Override
        public boolean equals(final Object o) {
            if (this == o) return true;
            if (o == null || Entry.class != o.getClass()) return false;
            Entry entry = (Entry) o;
            return id.equals(entry.id) &&
                    lockType == entry.lockType &&
                    lockMode == entry.lockMode &&
                    owner.equals(entry.owner);
        }

        @Override
        public int hashCode() {
            int result = id.hashCode();
            result = 31 * result + lockType.val;
            result = 31 * result + lockMode.val;
            result = 31 * result + owner.hashCode();
            return result;
        }
    }

    /**
     * The modes of a Lock
     */
    enum LockMode {
        NO_LOCK((byte)0x0),
        READ_LOCK((byte)0x1),
        WRITE_LOCK((byte)0x2),

        INTENTION_READ((byte)0x3),
        INTENTION_WRITE((byte)0x4);

        private final byte val;

        LockMode(final byte val) {
            this.val = val;
        }

        public byte getVal() {
            return val;
        }
    }

    /**
     * The type of a Lock
     */
    enum LockType {

        @Deprecated UNKNOWN((byte) 0x5),

        @Deprecated LEGACY_COLLECTION((byte) 0x4),
        @Deprecated LEGACY_DOCUMENT((byte) 0x3),

        COLLECTION((byte) 0x2),
        DOCUMENT((byte) 0x1),

        BTREE((byte) 0x0);

        private final byte val;

        LockType(final byte val) {
            this.val = val;
        }

        public byte getVal() {
            return val;
        }
    }

    public static void main(final String args[]) {

        // ConcurrentHashMap with NO object pooling
//        sendEvents(new Test2(false, false));

        // ConcurrentHashMap with object pooling
//        sendEvents(new Test2(false, true));

        // NonBlockingHashMap with NO object pooling
//        sendEvents(new Test2(true, false));

        // NonBlockingHashMap with object pooling
        // TODO(AR) causes a remove(K) after a put(K,object) to return null with NonBlockingHashMap
        sendEvents(new Test2(true, true));
    }

    private static void sendEvents(final Test2 lockTable) {
        lockTable.event(LockEventType.Attempt, "/db", LockType.COLLECTION, LockMode.INTENTION_READ);
        lockTable.event(LockEventType.Acquired, "/db", LockType.COLLECTION, LockMode.INTENTION_READ);

        lockTable.event(LockEventType.Attempt, "/db/apps", LockType.COLLECTION, LockMode.INTENTION_READ);
        lockTable.event(LockEventType.Acquired, "/db/apps", LockType.COLLECTION, LockMode.INTENTION_READ);
        lockTable.event(LockEventType.Released, "/db", LockType.COLLECTION, LockMode.INTENTION_READ);

        lockTable.event(LockEventType.Attempt, "BTREE", LockType.BTREE, LockMode.READ_LOCK);
        lockTable.event(LockEventType.Acquired, "BTREE", LockType.BTREE, LockMode.READ_LOCK);
        lockTable.event(LockEventType.Released, "BTREE", LockType.BTREE, LockMode.READ_LOCK);

        lockTable.event(LockEventType.Attempt, "/db/apps/docs", LockType.COLLECTION, LockMode.INTENTION_READ);
        lockTable.event(LockEventType.Acquired, "/db/apps/docs", LockType.COLLECTION, LockMode.INTENTION_READ);
        lockTable.event(LockEventType.Released, "/db/apps", LockType.COLLECTION, LockMode.INTENTION_READ);

        lockTable.event(LockEventType.Attempt, "/db/apps/docs/data", LockType.COLLECTION, LockMode.INTENTION_READ);
        lockTable.event(LockEventType.Acquired, "/db/apps/docs/data", LockType.COLLECTION, LockMode.INTENTION_READ);
        lockTable.event(LockEventType.Released, "/db/apps/docs", LockType.COLLECTION, LockMode.INTENTION_READ);

        lockTable.event(LockEventType.Attempt, "/db/apps/docs/data/0", LockType.COLLECTION, LockMode.READ_LOCK);
        lockTable.event(LockEventType.Acquired, "/db/apps/docs/data/0", LockType.COLLECTION, LockMode.READ_LOCK);
        lockTable.event(LockEventType.Released, "/db/apps/docs/data", LockType.COLLECTION, LockMode.INTENTION_READ);

        lockTable.event(LockEventType.Attempt, "/db/apps/docs/data/0/0", LockType.DOCUMENT, LockMode.READ_LOCK);
        lockTable.event(LockEventType.Acquired, "/db/apps/docs/data/0/0", LockType.DOCUMENT, LockMode.READ_LOCK);
        lockTable.event(LockEventType.Released, "/db/apps/docs/data/0/0", LockType.DOCUMENT, LockMode.READ_LOCK);

        lockTable.event(LockEventType.Released, "/db/apps/docs/data/0", LockType.COLLECTION, LockMode.READ_LOCK);
        lockTable.event(LockEventType.Attempt, "/db/apps/docs/data/1", LockType.COLLECTION, LockMode.READ_LOCK);
        lockTable.event(LockEventType.Acquired, "/db/apps/docs/data/1", LockType.COLLECTION, LockMode.READ_LOCK);

        // No entry found when trying to remove `attempt` to promote to `acquired` for: id=/db/apps/docs/data/1, EntryKey.hashCode=1786905457

        lockTable.event(LockEventType.Attempt, "/db/apps/docs/data/1/1", LockType.DOCUMENT, LockMode.READ_LOCK);
        lockTable.event(LockEventType.Acquired, "/db/apps/docs/data/1/1", LockType.DOCUMENT, LockMode.READ_LOCK);
        lockTable.event(LockEventType.Released, "/db/apps/docs/data/1/1", LockType.DOCUMENT, LockMode.READ_LOCK);
        lockTable.event(LockEventType.Released, "/db/apps/docs/data/1", LockType.COLLECTION, LockMode.READ_LOCK);

        // No entry found when trying to `release` for: id=/db/apps/docs/data/1, EntryKey.hashCode=1786905457

        lockTable.event(LockEventType.Attempt, "/db/apps/docs/data/2", LockType.COLLECTION, LockMode.READ_LOCK);
        lockTable.event(LockEventType.Acquired, "/db/apps/docs/data/2", LockType.COLLECTION, LockMode.READ_LOCK);
        lockTable.event(LockEventType.Attempt, "/db/apps/docs/data/2/2", LockType.DOCUMENT, LockMode.READ_LOCK);
        lockTable.event(LockEventType.Acquired, "/db/apps/docs/data/2/2", LockType.DOCUMENT, LockMode.READ_LOCK);

        lockTable.event(LockEventType.Released, "/db/apps/docs/data/2/2", LockType.DOCUMENT, LockMode.READ_LOCK);

        lockTable.event(LockEventType.Released, "/db/apps/docs/data/2", LockType.COLLECTION, LockMode.READ_LOCK);

    }
}
