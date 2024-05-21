package io.netty.example.demo.packet;

import com.alibaba.fastjson2.JSON;

public class JSONSerializer implements Serializer{
    @Override
    public byte getSerializerAlgorithm() {
        return DEFAULT.getSerializerAlgorithm();
    }

    @Override
    public <T> T deserialize(Class<T> clazz, byte[] bytes) {
        return DEFAULT.deserialize(clazz, bytes);
    }
}
