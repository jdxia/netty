package io.netty.example.demo.packet;

import lombok.Data;

@Data
public class LoginRequestPacket extends Packet implements Command {

    private Integer userId;

    private String username;

    private String password;

    @Override
    public Byte getCommand() {
        return LOGIN_REQUEST;
    }
}
