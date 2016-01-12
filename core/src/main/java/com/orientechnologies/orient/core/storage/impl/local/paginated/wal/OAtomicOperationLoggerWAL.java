package com.orientechnologies.orient.core.storage.impl.local.paginated.wal;

import com.orientechnologies.orient.core.storage.impl.local.paginated.atomicoperations.OAtomicOperationMetadata;

import java.io.IOException;
import java.util.Map;

/**
 * Created by tglman on 11/01/16.
 */
public class OAtomicOperationLoggerWAL implements OAtomicOperationLogger {
  private OLogSegment oLogSegment;

  public OAtomicOperationLoggerWAL(OLogSegment oLogSegment) {
    this.oLogSegment = oLogSegment;
  }

  @Override
  public OLogSequenceNumber log(OWALRecord record) throws IOException {
    if (this.oLogSegment == null)
      return new OLogSequenceNumber(Long.MAX_VALUE, Long.MAX_VALUE);
    final byte[] serializedForm = OWALRecordsFactory.INSTANCE.toStream(record);
    OLogSequenceNumber lsn = oLogSegment.logRecord(serializedForm);
    record.setLsn(lsn);
    return lsn;
  }

  @Override
  public OLogSequenceNumber logAtomicOperationStartRecord(boolean isRollbackSupported, OOperationUnitId unitId) throws IOException {
    if (this.oLogSegment == null)
      return new OLogSequenceNumber(Long.MAX_VALUE, Long.MAX_VALUE);
    OAtomicUnitStartRecord record = new OAtomicUnitStartRecord(isRollbackSupported, unitId);
    final byte[] serializedForm = OWALRecordsFactory.INSTANCE.toStream(record);
    OLogSequenceNumber lsn = oLogSegment.logStartOperationRecord(serializedForm, unitId);
    record.setLsn(lsn);
    return lsn;
  }

  @Override
  public void logAtomicOperationEndRecord(OOperationUnitId operationUnitId, boolean rollback, OLogSequenceNumber startLSN, Map<String, OAtomicOperationMetadata<?>> metadata) throws IOException {
    if (this.oLogSegment == null)
      return;
    OAtomicUnitEndRecord end = new OAtomicUnitEndRecord(operationUnitId, rollback, metadata);
    final byte[] serializedForm = OWALRecordsFactory.INSTANCE.toStream(end);
    OLogSequenceNumber lsn = oLogSegment.logEndOperationRecord(serializedForm, operationUnitId);
    end.setLsn(lsn);
  }


}
