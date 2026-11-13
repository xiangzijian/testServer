package com.slg.common.thread;


import java.util.Collection;

public interface SlotQueue<E> {

    boolean offer(E item);

    E poll();

    int drainTo(Collection<? super E> c, int maxElements);
}
