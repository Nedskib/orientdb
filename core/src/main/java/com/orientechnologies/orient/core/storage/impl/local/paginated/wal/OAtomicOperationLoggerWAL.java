package com.orientechnologies.orient.core.storage.impl.local.paginated.wal;

import com.orientechnologies.orient.core.storage.impl.local.paginated.atomicoperations.OAtomicOperationMetadata;

import java.io.IOException;
import java.util.Map;

/**
 * Created by tglman on 11/01/16.
 */
public class OAtomicOperationLoggerWAL implements OAtomicOperationLogger {
  private OWriteAheadLog oDiskWriteAheadLog;

  public OAtomicOperationLoggerWAL(OWriteAheadLog oDiskWriteAheadLog) {
    this.oDiskWriteAheadLog = oDiskWriteAheadLog;
  }

  @Override
  public OLogSequenceNumber log(OWALRecord record) throws IOException {
    return oDiskWriteAheadLog.log(record);
  }

  @Override
  public OLogSequenceNumber logAtomicOperationStartRecord(boolean b, OOperationUnitId unitId) throws IOException {
    return oDiskWriteAheadLog.logAtomicOperationStartRecord(b, unitId);
  }

  @Override
  public void logAtomicOperationEndRecord(OOperationUnitId operationUnitId, boolean rollback, OLogSequenceNumber startLSN, Map<String, OAtomicOperationMetadata<?>> metadata) throws IOException {
    oDiskWriteAheadLog.logAtomicOperationEndRecord(operationUnitId, rollback, startLSN, metadata);
  }


}
