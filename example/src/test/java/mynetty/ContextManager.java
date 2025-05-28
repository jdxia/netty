package mynetty;

import lombok.Getter;
import lombok.ToString;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;


public class ContextManager implements AutoCloseable {

    // 静态变量，维护不同线程的上下文
    private static final ThreadLocal<ContextManager> CONTEXT_THREAD_LOCAL = new ThreadLocal<>();

    /**
     * 定义上下文中使用的键及其期望的值类型。
     * 这样可以提供类型安全，并集中管理所有可能的上下文键。
     * 尽量放和业务无关的, 和业务有关的, 自己通过参数传递, 不要放这里
     */
    @Getter
    @ToString
    public static enum ContextTypeEnum {
        USER_ID("userId", String.class, "用户ID", false),
        TRACE_ID("traceId", String.class, "链路追踪id", true),
        ;

        private final String keyName;
        private final Class<?> valueType;
        private final String desc;
        private final boolean builtIn;

        ContextTypeEnum(String keyName, Class<?> valueType, String desc, boolean builtIn) {
            this.keyName = keyName;
            this.valueType = valueType;
            this.desc = desc;
            this.builtIn = builtIn;
        }

    }

    /**
     * 实例变量，维护每个上下文中所有的状态数据
     * 里面的数据的kv用枚举维护起来, 枚举里面存放具体class类型
     */
    private final ConcurrentMap<String, Object> values = new ConcurrentHashMap<>();

    // 获取当前线程的上下文
    public static ContextManager getCurrentContext() {
        return CONTEXT_THREAD_LOCAL.get();
    }

    // 在当前上下文设置一个状态数据
    public void set(String key, Object value) {
        if (value != null) {
            values.put(key, value);
        } else {
            values.remove(key);
        }
    }

    /**
     * 使用ContextKey设置一个状态数据，提供类型检查。
     * @param contextTypeEnum 枚举定义的上下文键
     * @param value 要设置的值
     * @param <T> 值的类型
     * @throws IllegalArgumentException 如果值的类型与ContextKey中定义的期望类型不匹配
     */
    public <T> void set(ContextTypeEnum contextTypeEnum, T value) {
        if (value != null) {
            if (!contextTypeEnum.getValueType().isInstance(value)) {
                throw new IllegalArgumentException("Invalid value type for key '" + contextTypeEnum.getKeyName() +
                                                   "'. Expected " + contextTypeEnum.getValueType().getName() +
                                                   ", but got " + value.getClass().getName());
            }
            values.put(contextTypeEnum.getKeyName(), value);
        } else {
            values.remove(contextTypeEnum.getKeyName());
        }
    }

    // 在当前上下文读取一个状态数据
    public Object get(String key) {
        return values.get(key);
    }

    /**
     * 使用 ContextTypeEnum 读取一个数据，提供类型转换和检查。
     * @param contextTypeEnum 枚举定义的上下文键
     * @param <T> 期望返回的值的类型
     * @return 上下文中的值，如果键不存在或类型不匹配（理论上不应发生，若set被正确使用）则可能为null或抛出异常
     * @throws ClassCastException 如果存储的值不是ContextKey中定义的期望类型 (这主要是在不通过类型安全的set方法设值时可能发生)
     */
    @SuppressWarnings("unchecked")
    public <T> T get(ContextTypeEnum contextTypeEnum) {
        Object value = values.get(contextTypeEnum.getKeyName());
        if (value == null) {
            return null;
        }
        // 确保类型安全，如果类型不匹配则抛出 ClassCastException
        // 使用 cast 方法是更安全的方式，它会在类型不兼容时抛出 ClassCastException
        return (T) contextTypeEnum.getValueType().cast(value);
    }

    // 开启一个新的上下文
    public static ContextManager beginContext() {
        ContextManager context = CONTEXT_THREAD_LOCAL.get();
        if (context != null) {
            // 先清理当前上下文
            endCurrentContext();

            throw new IllegalStateException("ContextManager is already started in the current thread.");
        }
        context = new ContextManager();
        CONTEXT_THREAD_LOCAL.set(context);
        return context;
    }

    // 关闭当前上下文
    public static void endCurrentContext() {
        ContextManager context = CONTEXT_THREAD_LOCAL.get();
        if (context != null) {
            context.values.clear();
            CONTEXT_THREAD_LOCAL.remove();
        }
    }

    @Override
    public void close() {
        endCurrentContext();
    }
}
