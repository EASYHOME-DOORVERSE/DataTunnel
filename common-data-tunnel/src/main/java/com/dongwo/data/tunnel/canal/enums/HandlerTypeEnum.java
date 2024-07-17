package com.dongwo.data.tunnel.canal.enums;

/**
 * @author : Leon on XXM Mac
 * @since : create in 2024/2/28 5:04 PM
 */
public enum HandlerTypeEnum {
    /**
     *
     */
    INSERT("INSERT", "新增"),
    UPDATE("UPDATE", "更新"),
    DELETE("DELETE", "删除"),
    ;

    private String type;
    private String desc;

    HandlerTypeEnum(String type, String desc) {
        this.type = type;
        this.desc = desc;
    }

    public String getType() {
        return type;
    }

    public void setType(String type) {
        this.type = type;
    }
}
