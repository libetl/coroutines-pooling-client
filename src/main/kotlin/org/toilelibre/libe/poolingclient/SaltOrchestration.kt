package org.toilelibre.libe.poolingclient

import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Deferred

typealias SaltOrchestration = suspend CoroutineScope.((String) -> Deferred<String?>) -> List<Deferred<String?>>