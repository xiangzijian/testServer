package com.slg.common.util;

public class ThreadUtil {

   public static int nextPowerOfTwo(int n) {
        if (n <= 1) {
            return 1;
        }
        if (n > (1 << 30)) {
            throw new ArithmeticException("overflow: no 32-bit power of two >= " + n);
        }
        return Integer.highestOneBit(n - 1) << 1;
    }
}
