package com.slg.common.thread;

record SlotTask(long routeId,Long uniqueKey, Runnable task) {
}
