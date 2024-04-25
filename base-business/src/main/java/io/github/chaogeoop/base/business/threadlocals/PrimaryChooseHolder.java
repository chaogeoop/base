package io.github.chaogeoop.base.business.threadlocals;

import com.mysema.commons.lang.Pair;
import io.github.chaogeoop.base.business.common.errors.BizException;

import java.util.Date;

public class PrimaryChooseHolder {
    private static final ThreadLocal<Pair<Boolean, String>> primaryChooseHolder = new ThreadLocal<>();

    public static void init(long primaryChooseStamp, String funcName) {
        Pair<Boolean, String> pair = primaryChooseHolder.get();
        if (pair != null) {
            return;
        }

        if (funcName == null) {
            throw new BizException("funcName不能为空");
        }

        long i = new Date().getTime() - primaryChooseStamp;
        primaryChooseHolder.set(Pair.of(i < 0, funcName));
    }

    public static boolean get() {
        Pair<Boolean, String> pair = primaryChooseHolder.get();
        if (pair == null) {
            return true;
        }

        return pair.getFirst();
    }

    public static void remove(String funcName) {
        Pair<Boolean, String> pair = primaryChooseHolder.get();
        if (pair != null && !pair.getSecond().equals(funcName)) {
            return;
        }

        primaryChooseHolder.remove();
    }
}
