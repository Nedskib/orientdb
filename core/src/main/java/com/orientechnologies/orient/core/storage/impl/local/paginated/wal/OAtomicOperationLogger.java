package com.orientechnologies.orient.core.storage.impl.local.paginated.wal;

import com.orientechnologies.orient.core.storage.impl.local.paginated.atomicoperations.OAtomicOperationMetadata;

import java.io.IOException;
import java.util.Map;

/**
 * Created by tglman on 11/01/16.
 */
public interface OAtomicOperationLogger {
  OLogSequenceNumber log(OWALRecord record) throws IOException;

  OLogSequenceNumber logAtomicOperationStartRecord(boolean b, OOperationUnitId unitId) throws IOException;

  void logAtomicOperationEndRecord(OOperationUnitId operationUnitId, boolean rollback, OLogSequenceNumber startLSN, Map<String, OAtomicOperationMetadata<?>> metadata) throws IOException;
}
