package net.bondarik.model;

import java.io.Serializable;
import java.time.LocalDateTime;

public class MessageWithDate implements Serializable {
    private static final long serialVersionUID = 1L;

    private final LocalDateTime createTime;

    private final String message;

    public MessageWithDate(String message) {
        this.createTime = LocalDateTime.now();
        this.message = message;
    }

    public String getMessage() {
        return message;
    }

    @Override
    public String toString() {
        return "[" + createTime.toString() + "] " + message;
    }
}
