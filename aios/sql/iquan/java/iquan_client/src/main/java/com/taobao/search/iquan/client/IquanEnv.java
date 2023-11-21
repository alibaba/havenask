package com.taobao.search.iquan.client;

import com.taobao.search.iquan.client.common.json.api.JsonKMonConfig;
import com.taobao.search.iquan.client.common.metrics.IquanKMonService;
import com.taobao.search.iquan.client.common.response.SqlResponse;
import com.taobao.search.iquan.client.common.utils.ClientUtils;
import com.taobao.search.iquan.client.common.utils.ErrorUtils;
import com.taobao.search.iquan.core.api.common.IquanErrorCode;
import com.taobao.search.iquan.core.api.exception.SqlQueryException;
import org.slf4j.LoggerFactory;

public class IquanEnv {
    /**
     * Init Env Resource
     */
    public static byte[] initEnvResource() {
        SqlResponse response = new SqlResponse();
        Thread current = Thread.currentThread();
        ClassLoader oldClassLoader = current.getContextClassLoader();
        try {
            // 1. set new class loader
            current.setContextClassLoader(IquanEnv.class.getClassLoader());

            // 2. init logger
            LoggerFactory.getLogger(IquanEnv.class);
        } catch (Exception ex) {
            ErrorUtils.setException(ex, response);
        } finally {
            current.setContextClassLoader(oldClassLoader);
        }
        return ClientUtils.toResponseByte(response);
    }

    /**
     * Start Kmonitor
     */
    public static byte[] startKmon(int format, byte[] params) {
        SqlResponse response;

        Thread current = Thread.currentThread();
        ClassLoader oldClassLoader = current.getContextClassLoader();
        try {
            // 1. set new class loader
            current.setContextClassLoader(IquanEnv.class.getClassLoader());

            // 2. parse config
            JsonKMonConfig kMonConfig = ClientUtils.parseRequest(format, params, JsonKMonConfig.class);
            if (!kMonConfig.isValid()) {
                throw new SqlQueryException(IquanErrorCode.IQUAN_EC_CLIENT_INIT_FAIL, "kmon config is not valid.");
            }

            // 3. start kmon
            return IquanKMonService.start(kMonConfig);
        } catch (SqlQueryException ex) {
            response = new SqlResponse();
            ErrorUtils.setSqlQueryException(ex, response);
        } catch (Exception ex) {
            response = new SqlResponse();
            ErrorUtils.setException(ex, response);
        } finally {
            current.setContextClassLoader(oldClassLoader);
        }
        return ClientUtils.toResponseByte(response);
    }

    /**
     * Stop Kmonitor
     */
    public static byte[] stopKmon() {
        SqlResponse response = new SqlResponse();
        Thread current = Thread.currentThread();
        ClassLoader oldClassLoader = current.getContextClassLoader();
        try {
            // 1. set new class loader
            current.setContextClassLoader(IquanEnv.class.getClassLoader());

            // 2. stop kmon
            IquanKMonService.stop();
        } catch (Exception ex) {
            ErrorUtils.setException(ex, response);
        } finally {
            current.setContextClassLoader(oldClassLoader);
        }
        return ClientUtils.toResponseByte(response);
    }
}
