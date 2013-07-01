package com.xingcloud.server.exception;

/**
 * User: IvyTang
 * Date: 12-12-7
 * Time: 下午2:44
 */
public class ConfException extends Exception {

    public ConfException(String s) {
        super(s);
    }

    public ConfException(String s, Throwable throwable) {
        super(s, throwable);
    }
}
