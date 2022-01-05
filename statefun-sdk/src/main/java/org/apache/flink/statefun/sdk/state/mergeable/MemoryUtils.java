package org.apache.flink.statefun.sdk.state.mergeable;

import sun.misc.Unsafe;

import java.lang.reflect.Field;
import java.nio.ByteOrder;

public class MemoryUtils {
    public static final Unsafe UNSAFE = getUnsafe();
    public static final ByteOrder NATIVE_BYTE_ORDER = ByteOrder.nativeOrder();
    private static Unsafe getUnsafe() {
        try {
            Field unsafeField = Unsafe.class.getDeclaredField("theUnsafe");
            unsafeField.setAccessible(true);
            return (Unsafe)unsafeField.get((Object)null);
        } catch (SecurityException var1) {
            throw new Error("Could not access the sun.misc.Unsafe handle, permission denied by security manager.", var1);
        } catch (NoSuchFieldException var2) {
            throw new Error("The static handle field in sun.misc.Unsafe was not found.", var2);
        } catch (IllegalArgumentException var3) {
            throw new Error("Bug: Illegal argument reflection access for static field.", var3);
        } catch (IllegalAccessException var4) {
            throw new Error("Access to sun.misc.Unsafe is forbidden by the runtime.", var4);
        } catch (Throwable var5) {
            throw new Error("Unclassified error while trying to access the sun.misc.Unsafe handle.", var5);
        }
    }
}
