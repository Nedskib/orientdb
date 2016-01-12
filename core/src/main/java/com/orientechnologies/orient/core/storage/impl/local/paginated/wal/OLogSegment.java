package com.orientechnologies.orient.core.storage.impl.local.paginated.wal;

import com.orientechnologies.common.directmemory.ODirectMemoryPointer;
import com.orientechnologies.common.directmemory.ODirectMemoryPointerFactory;
import com.orientechnologies.common.exception.OException;
import com.orientechnologies.common.io.OFileUtils;
import com.orientechnologies.common.log.OLogManager;
import com.orientechnologies.common.serialization.types.OIntegerSerializer;
import com.orientechnologies.common.serialization.types.OLongSerializer;
import com.orientechnologies.common.util.OPair;
import com.orientechnologies.orient.core.config.OGlobalConfiguration;
import com.orientechnologies.orient.core.exception.OStorageException;
import com.orientechnologies.orient.core.storage.OStorageAbstract;
import com.orientechnologies.orient.core.storage.impl.local.OLowDiskSpaceInformation;
import com.orientechnologies.orient.core.storage.impl.local.OLowDiskSpaceListener;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.lang.ref.WeakReference;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;
import java.util.concurrent.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.zip.CRC32;

/**
 * Created by tglman on 08/01/16.
 */
final class OLogSegment implements Comparable<OLogSegment> {
  private       ODiskWriteAheadLog oDiskWriteAheadLog;
  private final RandomAccessFile   rndFile;
  private final File               file;
  private final long               order;
  private final int                maxPagesCacheSize;
  private final ConcurrentLinkedQueue<OWALPage> pagesCache     = new ConcurrentLinkedQueue<OWALPage>();
  private final ScheduledExecutorService        commitExecutor = Executors.newSingleThreadScheduledExecutor(new ThreadFactory() {
    @Override
    public Thread newThread(Runnable r) {
      final Thread thread = new Thread(OStorageAbstract.storageThreadGroup, r);
      thread.setDaemon(true);
      thread.setName("OrientDB WAL Flush Task (" + oDiskWriteAheadLog.getStorage().getName() + ")");
      return thread;
    }
  });
  private long     filledUpTo;
  private boolean  closed;
  private OWALPage currentPage;
  private long     nextPositionToFlush;
  private OLogSequenceNumber last = null;
  private OLogSequenceNumber pendingLSNToFlush;
  private OLogSequenceNumber flushedLsn;

  private volatile boolean flushNewData = true;

  private WeakReference<OPair<OLogSequenceNumber, byte[]>> lastReadRecord = new WeakReference<OPair<OLogSequenceNumber, byte[]>>(null);

  private final Set<OOperationUnitId> activeOperations = new HashSet<OOperationUnitId>();
  private boolean closedSegment;

  public OLogSequenceNumber logStartOperationRecord(byte[] serializedForm, OOperationUnitId unitId) throws IOException {
    OLogSequenceNumber lsn = logRecord(serializedForm);
    activeOperations.add(unitId);
    return lsn;
  }

  public OLogSequenceNumber logEndOperationRecord(byte[] serializedForm, OOperationUnitId operationUnitId) throws IOException {
    OLogSequenceNumber lsn = logRecord(serializedForm);
    activeOperations.remove(operationUnitId);
    if (closedSegment && activeOperations.size() == 0) {
      stopFlush(true);
      oDiskWriteAheadLog.closeSegment(this);
    }
    return lsn;
  }

  private final class FlushTask implements Runnable {
    private FlushTask() {
    }

    @Override
    public void run() {
      try {
        OLogSegment.this.commit();
      } catch (Throwable e) {
        OLogManager.instance().error(this, "Error during WAL background flush", e);
      }
    }
  }

  OLogSegment(ODiskWriteAheadLog oDiskWriteAheadLog, File file, int maxPagesCacheSize) throws IOException {
    this.oDiskWriteAheadLog = oDiskWriteAheadLog;
    this.file = file;
    this.maxPagesCacheSize = maxPagesCacheSize;

    order = extractOrder(file.getName());
    closed = false;
    rndFile = new RandomAccessFile(file, "rw");
  }

  public void startFlush() {
    if (oDiskWriteAheadLog.getCommitDelay() > 0)
      commitExecutor.scheduleAtFixedRate(new FlushTask(), oDiskWriteAheadLog.getCommitDelay(), oDiskWriteAheadLog.getCommitDelay(), TimeUnit.MILLISECONDS);
  }

  public void stopFlush(boolean flush) {
    if (flush)
      flush();

    if (!commitExecutor.isShutdown()) {
      commitExecutor.shutdown();
      try {
        if (!commitExecutor.awaitTermination(OGlobalConfiguration.WAL_SHUTDOWN_TIMEOUT.getValueAsInteger(), TimeUnit.MILLISECONDS))
          throw new OStorageException("WAL flush task for '" + getPath() + "' segment cannot be stopped");

      } catch (InterruptedException e) {
        OLogManager.instance().error(this, "Cannot shutdown background WAL commit thread");
      }
    }
  }

  private void commit() throws IOException {
    if (pagesCache.isEmpty())
      return;

    if (!flushNewData)
      return;

    flushNewData = false;

    final int maxSize = pagesCache.size();

    ODirectMemoryPointer[] pagesToFlush = new ODirectMemoryPointer[maxSize];

    long filePointer = nextPositionToFlush;

    int flushedPages = 0;
    OLogSequenceNumber lastLSNToFlush = null;

    Iterator<OWALPage> pageIterator = pagesCache.iterator();
    while (flushedPages < maxSize) {
      final OWALPage page = pageIterator.next();
      synchronized (page) {
        final int filledUpTo = page.getFilledUpTo();
        int pos = OWALPage.RECORDS_OFFSET;

        while (pos < filledUpTo) {
          if (!page.mergeWithNextPage(pos)) {
            if (pos == OWALPage.RECORDS_OFFSET && pendingLSNToFlush != null) {
              lastLSNToFlush = pendingLSNToFlush;

              pendingLSNToFlush = null;
            } else
              lastLSNToFlush = new OLogSequenceNumber(order, filePointer + flushedPages * (long) OWALPage.PAGE_SIZE + pos);
          } else if (pendingLSNToFlush == null)
            pendingLSNToFlush = new OLogSequenceNumber(order, filePointer + flushedPages * (long) OWALPage.PAGE_SIZE + pos);

          pos += page.getSerializedRecordSize(pos);
        }

        ODirectMemoryPointer dataPointer;
        if (flushedPages == maxSize - 1) {
          dataPointer = ODirectMemoryPointerFactory.instance().createPointer(OWALPage.PAGE_SIZE);
          page.getPagePointer().moveData(0, dataPointer, 0, OWALPage.PAGE_SIZE);
        } else {
          dataPointer = page.getPagePointer();
        }

        pagesToFlush[flushedPages] = dataPointer;
      }

      flushedPages++;
    }

    synchronized (rndFile) {
      rndFile.seek(filePointer);
      for (int i = 0; i < pagesToFlush.length; i++) {
        ODirectMemoryPointer dataPointer = pagesToFlush[i];
        byte[] pageContent = dataPointer.get(0, OWALPage.PAGE_SIZE);
        if (i == pagesToFlush.length - 1)
          dataPointer.free();

        flushPage(pageContent);
        filePointer += OWALPage.PAGE_SIZE;
      }

      if (OGlobalConfiguration.WAL_SYNC_ON_PAGE_FLUSH.getValueAsBoolean())
        rndFile.getFD().sync();
    }

    nextPositionToFlush = filePointer - OWALPage.PAGE_SIZE;

    if (lastLSNToFlush != null)
      this.flushedLsn = lastLSNToFlush;

    for (int i = 0; i < flushedPages - 1; i++) {
      OWALPage page = pagesCache.poll();
      page.getPagePointer().free();
    }

    assert !pagesCache.isEmpty();

    final long freeSpace = oDiskWriteAheadLog.getWalLocation().getFreeSpace();
    if (freeSpace < oDiskWriteAheadLog.getFreeSpaceLimit()) {
      for (WeakReference<OLowDiskSpaceListener> listenerWeakReference : oDiskWriteAheadLog.getLowDiskSpaceListeners()) {
        final OLowDiskSpaceListener lowDiskSpaceListener = listenerWeakReference.get();

        if (lowDiskSpaceListener != null)
          lowDiskSpaceListener.lowDiskSpace(new OLowDiskSpaceInformation(freeSpace, oDiskWriteAheadLog.getFreeSpaceLimit()));
      }
    }
  }

  private void flushPage(byte[] content) throws IOException {
    CRC32 crc32 = new CRC32();
    crc32.update(content, OIntegerSerializer.INT_SIZE, OWALPage.PAGE_SIZE - OIntegerSerializer.INT_SIZE);
    OIntegerSerializer.INSTANCE.serializeNative((int) crc32.getValue(), content, 0);

    rndFile.write(content);
  }


  public long getOrder() {
    return order;
  }

  public void init() throws IOException {
    selfCheck();

    initPageCache();

    last = new OLogSequenceNumber(order, filledUpTo - 1);
  }

  @Override
  public int compareTo(OLogSegment other) {
    final long otherOrder = other.order;

    if (order > otherOrder)
      return 1;
    else if (order < otherOrder)
      return -1;

    return 0;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o)
      return true;
    if (o == null || getClass() != o.getClass())
      return false;

    OLogSegment that = (OLogSegment) o;

    return order == that.order;

  }

  @Override
  public int hashCode() {
    return (int) (order ^ (order >>> 32));
  }

  public long filledUpTo() throws IOException {
    return filledUpTo;
  }

  public OLogSequenceNumber begin() throws IOException {
    if (!pagesCache.isEmpty())
      return new OLogSequenceNumber(order, OWALPage.RECORDS_OFFSET);

    if (rndFile.length() > 0)
      return new OLogSequenceNumber(order, OWALPage.RECORDS_OFFSET);

    return null;
  }

  public OLogSequenceNumber end() {
    return last;
  }

  public void delete(boolean flush) throws IOException {
    close(flush);

    boolean deleted = OFileUtils.delete(file);
    int retryCount = 0;

    while (!deleted) {
      deleted = OFileUtils.delete(file);
      retryCount++;

      if (retryCount > 10)
        throw new IOException("Cannot delete file. Retry limit exceeded. (" + retryCount + ")");
    }
  }

  public String getPath() {
    return file.getAbsolutePath();
  }

  public OLogSequenceNumber logRecord(byte[] record) throws IOException {
    oDiskWriteAheadLog.lockStart();
    try {
      flushNewData = true;
      long preFilled = filledUpTo;
      int pageOffset = (int) (filledUpTo % OWALPage.PAGE_SIZE);
      long pageIndex = filledUpTo / OWALPage.PAGE_SIZE;

      if (pageOffset == 0 && pageIndex > 0)
        pageIndex--;

      int pos = 0;
      boolean firstChunk = true;

      OLogSequenceNumber lsn = null;

      while (pos < record.length) {
        if (currentPage == null) {
          ODirectMemoryPointer pointer = ODirectMemoryPointerFactory.instance().createPointer(OWALPage.PAGE_SIZE);
          currentPage = new OWALPage(pointer, true);
          pagesCache.add(currentPage);
          filledUpTo += OWALPage.RECORDS_OFFSET;
        }

        int freeSpace = currentPage.getFreeSpace();
        if (freeSpace < OWALPage.MIN_RECORD_SIZE) {
          filledUpTo += freeSpace + OWALPage.RECORDS_OFFSET;
          ODirectMemoryPointer pointer = ODirectMemoryPointerFactory.instance().createPointer(OWALPage.PAGE_SIZE);
          currentPage = new OWALPage(pointer, true);
          pagesCache.add(currentPage);
          pageIndex++;

          freeSpace = currentPage.getFreeSpace();
        }

        final OWALPage walPage = currentPage;
        synchronized (walPage) {
          final int entrySize = OWALPage.calculateSerializedSize(record.length - pos);
          int addedChunkOffset;
          if (entrySize <= freeSpace) {
            if (pos == 0)
              addedChunkOffset = walPage.appendRecord(record, false, !firstChunk);
            else
              addedChunkOffset = walPage.appendRecord(Arrays.copyOfRange(record, pos, record.length), false, !firstChunk);

            pos = record.length;
          } else {
            int chunkSize = OWALPage.calculateRecordSize(freeSpace);
            if (chunkSize > record.length - pos)
              chunkSize = record.length - pos;

            addedChunkOffset = walPage.appendRecord(Arrays.copyOfRange(record, pos, pos + chunkSize), true, !firstChunk);
            pos += chunkSize;
          }

          if (firstChunk)
            lsn = new OLogSequenceNumber(order, pageIndex * OWALPage.PAGE_SIZE + addedChunkOffset);

          int spaceDiff = freeSpace - walPage.getFreeSpace();
          filledUpTo += spaceDiff;

          firstChunk = false;
        }
      }

      if (pagesCache.size() > maxPagesCacheSize) {
        OLogManager.instance().info(this, "Max cache limit is reached (%d vs. %d), sync flush is performed", maxPagesCacheSize, pagesCache.size());
        flush();
      }
      if (filledUpTo() >= oDiskWriteAheadLog.getMaxSegmentSize()) {
        oDiskWriteAheadLog.swapToNewSegment();
        closedSegment = true;
      }

      oDiskWriteAheadLog.loggedSize(filledUpTo - preFilled);
      last = lsn;
    }finally {
      oDiskWriteAheadLog.lockEnd();
    }
    return last;
  }

  @SuppressFBWarnings(value = "PZLA_PREFER_ZERO_LENGTH_ARRAYS")
  public byte[] readRecord(OLogSequenceNumber lsn) throws IOException {
    final OPair<OLogSequenceNumber, byte[]> lastRecord = lastReadRecord.get();
    if (lastRecord != null && lastRecord.getKey().equals(lsn))
      return lastRecord.getValue();

    assert lsn.getSegment() == order;
    if (lsn.getPosition() >= filledUpTo)
      return null;

    if (!pagesCache.isEmpty())
      flush();

    long pageIndex = lsn.getPosition() / OWALPage.PAGE_SIZE;

    byte[] record = null;
    int pageOffset = (int) (lsn.getPosition() % OWALPage.PAGE_SIZE);

    long pageCount = (filledUpTo + OWALPage.PAGE_SIZE - 1) / OWALPage.PAGE_SIZE;

    while (pageIndex < pageCount) {
      byte[] pageContent = new byte[OWALPage.PAGE_SIZE];
      synchronized (rndFile) {
        rndFile.seek(pageIndex * OWALPage.PAGE_SIZE);
        rndFile.readFully(pageContent);
      }

      if (!checkPageIntegrity(pageContent))
        throw new OWALPageBrokenException("WAL page with index " + pageIndex + " is broken");

      ODirectMemoryPointer pointer = ODirectMemoryPointerFactory.instance().createPointer(pageContent);
      try {
        OWALPage page = new OWALPage(pointer, false);

        byte[] content = page.getRecord(pageOffset);
        if (record == null)
          record = content;
        else {
          byte[] oldRecord = record;

          record = new byte[record.length + content.length];
          System.arraycopy(oldRecord, 0, record, 0, oldRecord.length);
          System.arraycopy(content, 0, record, oldRecord.length, record.length - oldRecord.length);
        }

        if (page.mergeWithNextPage(pageOffset)) {
          pageOffset = OWALPage.RECORDS_OFFSET;
          pageIndex++;
          if (pageIndex >= pageCount)
            throw new OWALPageBrokenException("WAL page with index " + pageIndex + " is broken");
        } else {
          if (page.getFreeSpace() >= OWALPage.MIN_RECORD_SIZE && pageIndex < pageCount - 1)
            throw new OWALPageBrokenException("WAL page with index " + pageIndex + " is broken");

          break;
        }
      } finally {
        pointer.free();
      }
    }

    lastReadRecord = new WeakReference<OPair<OLogSequenceNumber, byte[]>>(new OPair<OLogSequenceNumber, byte[]>(lsn, record));
    return record;
  }

  public OLogSequenceNumber getNextLSN(OLogSequenceNumber lsn) throws IOException {
    final byte[] record = readRecord(lsn);
    if (record == null)
      return null;

    long pos = lsn.getPosition();
    long pageIndex = pos / OWALPage.PAGE_SIZE;
    int pageOffset = (int) (pos - pageIndex * OWALPage.PAGE_SIZE);

    int restOfRecord = record.length;
    while (restOfRecord > 0) {
      int entrySize = OWALPage.calculateSerializedSize(restOfRecord);
      if (entrySize + pageOffset < OWALPage.PAGE_SIZE) {
        if (entrySize + pageOffset <= OWALPage.PAGE_SIZE - OWALPage.MIN_RECORD_SIZE)
          pos += entrySize;
        else
          pos += OWALPage.PAGE_SIZE - pageOffset + OWALPage.RECORDS_OFFSET;
        break;
      } else if (entrySize + pageOffset == OWALPage.PAGE_SIZE) {
        pos += entrySize + OWALPage.RECORDS_OFFSET;
        break;
      } else {
        int chunkSize = OWALPage.calculateRecordSize(OWALPage.PAGE_SIZE - pageOffset);
        restOfRecord -= chunkSize;

        pos += OWALPage.PAGE_SIZE - pageOffset + OWALPage.RECORDS_OFFSET;
        pageOffset = OWALPage.RECORDS_OFFSET;
      }
    }

    if (pos >= filledUpTo)
      return null;

    return new OLogSequenceNumber(order, pos);
  }

  public void close(boolean flush) throws IOException {
    if (!closed) {
      lastReadRecord.clear();

      stopFlush(flush);

      rndFile.close();

      closed = true;

      if (!pagesCache.isEmpty()) {
        for (OWALPage page : pagesCache)
          page.getPagePointer().free();
      }

      currentPage = null;
    }
  }

  public OLogSequenceNumber readFlushedLSN() throws IOException {
    long pages = rndFile.length() / OWALPage.PAGE_SIZE;
    if (pages == 0)
      return null;

    return new OLogSequenceNumber(order, filledUpTo - 1);
  }

  public void loadFlushedLSN() throws IOException {
    flushedLsn = readFlushedLSN();
  }


  public void flush() {
    if (!commitExecutor.isShutdown()) {
      try {
        commitExecutor.submit(new FlushTask()).get();
      } catch (InterruptedException e) {
        Thread.interrupted();
        throw OException.wrapException(new OStorageException("Thread was interrupted during flush"), e);
      } catch (ExecutionException e) {
        throw OException.wrapException(new OStorageException("Error during WAL segment '" + getPath() + "' flush"), e);
      }
    } else {
      new FlushTask().run();
    }
  }

  private void initPageCache() throws IOException {
    synchronized (rndFile) {
      long pagesCount = rndFile.length() / OWALPage.PAGE_SIZE;
      if (pagesCount == 0)
        return;

      rndFile.seek((pagesCount - 1) * OWALPage.PAGE_SIZE);
      byte[] content = new byte[OWALPage.PAGE_SIZE];
      rndFile.readFully(content);

      if (checkPageIntegrity(content)) {
        ODirectMemoryPointer pointer = ODirectMemoryPointerFactory.instance().createPointer(content);
        currentPage = new OWALPage(pointer, false);
        filledUpTo = (pagesCount - 1) * OWALPage.PAGE_SIZE + currentPage.getFilledUpTo();
        nextPositionToFlush = (pagesCount - 1) * OWALPage.PAGE_SIZE;
      } else {
        ODirectMemoryPointer pointer = ODirectMemoryPointerFactory.instance().createPointer(OWALPage.PAGE_SIZE);
        currentPage = new OWALPage(pointer, true);
        filledUpTo = pagesCount * OWALPage.PAGE_SIZE + currentPage.getFilledUpTo();
        nextPositionToFlush = pagesCount * OWALPage.PAGE_SIZE;
      }

      pagesCache.add(currentPage);
    }
  }

  private long extractOrder(String name) {
    final Matcher matcher = Pattern.compile("^.*\\.(\\d+)\\.wal$").matcher(name);

    final boolean matches = matcher.find();
    assert matches;

    final String order = matcher.group(1);
    try {
      return Long.parseLong(order);
    } catch (NumberFormatException e) {
      // never happen
      throw new IllegalStateException(e);
    }
  }

  private boolean checkPageIntegrity(byte[] content) {
    final long magicNumber = OLongSerializer.INSTANCE.deserializeNative(content, OWALPage.MAGIC_NUMBER_OFFSET);
    if (magicNumber != OWALPage.MAGIC_NUMBER)
      return false;

    final CRC32 crc32 = new CRC32();
    crc32.update(content, OIntegerSerializer.INT_SIZE, OWALPage.PAGE_SIZE - OIntegerSerializer.INT_SIZE);

    return ((int) crc32.getValue()) == OIntegerSerializer.INSTANCE.deserializeNative(content, 0);
  }

  private void selfCheck() throws IOException {
    if (!pagesCache.isEmpty())
      throw new IllegalStateException("WAL cache is not empty, we cannot verify WAL after it was started to be used");

    synchronized (rndFile) {
      long pagesCount = rndFile.length() / OWALPage.PAGE_SIZE;

      if (rndFile.length() % OWALPage.PAGE_SIZE > 0) {
        OLogManager.instance().error(this, "Last WAL page was written partially, auto fix");

        rndFile.setLength(OWALPage.PAGE_SIZE * pagesCount);
      }
    }
  }

  public long getFilledUpTo() {
    return filledUpTo;
  }

  public OLogSequenceNumber getFlushedLsn() {
    return flushedLsn;
  }


}
