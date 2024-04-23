package io.github.chaogeoop.base.business.threadlocals;

import java.util.Date;

public class PrimaryChooseHolder {
    private static final ThreadLocal<Boolean> primaryChooseHolder = new ThreadLocal<>();

    public static void init(long primaryChooseStamp) {
        if (primaryChooseHolder.get() != null) {
            return;
        }

        long i = new Date().getTime() - primaryChooseStamp;

        primaryChooseHolder.set(i < 0);
    }

    public static boolean get() {
        Boolean result = primaryChooseHolder.get();
        if (result == null) {
            return true;
        }

        return result;
    }

    public static void remove() {
        primaryChooseHolder.remove();
    }
}
