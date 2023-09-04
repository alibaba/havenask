package com.taobao.search.iquan.boot.exception;

import com.taobao.search.iquan.boot.controller.BootController;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.ControllerAdvice;
import org.springframework.web.bind.annotation.ExceptionHandler;

import java.io.ByteArrayOutputStream;
import java.io.IOException;

@ControllerAdvice(basePackageClasses = BootController.class)
public class GlobalExceptionHandler {
    private static final Logger logger = LoggerFactory.getLogger(GlobalExceptionHandler.class);

    @ExceptionHandler(value = Exception.class)
    public ResponseEntity<?> handleGlobalException(Exception ex) {
        ByteArrayOutputStream buf = new ByteArrayOutputStream();
        ex.printStackTrace(new java.io.PrintWriter(buf, true));
        String expMessage = buf.toString();
        try {
            buf.close();
        } catch (IOException e) {
            e.printStackTrace();
        }

        String content = String.format("GlobalExceptionHandler: %s, %s.", ex.getMessage(), expMessage);
        logger.error(content);
        return new ResponseEntity<>(content, HttpStatus.INTERNAL_SERVER_ERROR);
    }
}

