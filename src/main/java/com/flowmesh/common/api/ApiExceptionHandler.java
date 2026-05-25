package com.flowmesh.common.api;

import com.flowmesh.dag.service.CycleDetectedException;
import com.flowmesh.dag.service.DagValidationException;
import jakarta.validation.ConstraintViolationException;
import org.springframework.http.HttpStatus;
import org.springframework.http.converter.HttpMessageNotReadableException;
import org.springframework.web.bind.MethodArgumentNotValidException;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.ResponseStatus;
import org.springframework.web.bind.annotation.RestControllerAdvice;

import java.util.LinkedHashMap;
import java.util.Map;

@RestControllerAdvice
public class ApiExceptionHandler {

    @ExceptionHandler(CycleDetectedException.class)
    @ResponseStatus(HttpStatus.BAD_REQUEST)
    public Map<String, Object> handleCycle(CycleDetectedException exception) {
        Map<String, Object> response = new LinkedHashMap<>();
        response.put("error", exception.error());
        response.put("path", exception.path());
        return response;
    }

    @ExceptionHandler(DagValidationException.class)
    @ResponseStatus(HttpStatus.BAD_REQUEST)
    public Map<String, Object> handleDagValidation(DagValidationException exception) {
        Map<String, Object> response = new LinkedHashMap<>();
        response.put("error", exception.error());
        response.put("message", exception.getMessage());
        return response;
    }

    @ExceptionHandler({
            ConstraintViolationException.class,
            MethodArgumentNotValidException.class,
            HttpMessageNotReadableException.class
    })
    @ResponseStatus(HttpStatus.BAD_REQUEST)
    public Map<String, Object> handleBadRequest(Exception exception) {
        Map<String, Object> response = new LinkedHashMap<>();
        response.put("error", "INVALID_DAG");
        response.put("message", exception.getMessage());
        return response;
    }
}
