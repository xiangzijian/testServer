package com.slg.common.thread;

/**
 * 邮箱中的一个任务。uniqueKey 为 null 表示普通任务。
 * 用类而不是 record：邮箱按引用删除被替换的任务，record 的 equals 会把内容相同的两个任务视为同一个。
 */
final class MailTask {

    final long id;
    final Object uniqueKey;
    final Runnable task;

    MailTask(long id, Object uniqueKey, Runnable task) {
        this.id = id;
        this.uniqueKey = uniqueKey;
        this.task = task;
    }
}
