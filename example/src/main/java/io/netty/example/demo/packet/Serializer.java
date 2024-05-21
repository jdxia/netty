package io.netty.example.demo.packet;

public interface Serializer extends SerializerAlgorithm {

    //JSON序列化
    byte JSON_SERIALIZER = JSON;

    Serializer DEFAULT = new JSONSerializer();

    //序列化算法
    byte getSerializerAlgorithm();

    //反序列化算法
    <T> T deserialize(Class<T> clazz, byte[] bytes);
}
