package com.anilyanai.business.peer;

import java.util.Set;

/**
 * Functional abstraction for retrieving the current active cluster peer connection IDs.
 */
@FunctionalInterface
public interface PeerSupplier {

    /**
     * Returns an unmodifiable set of currently active peer connection IDs.
     */
    Set<String> getActivePeerConnectionIds();
}
