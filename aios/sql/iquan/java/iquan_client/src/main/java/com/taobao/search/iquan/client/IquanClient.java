package com.taobao.search.iquan.client;

import com.taobao.kmonitor.KMonitor;
import com.taobao.search.iquan.client.common.common.FormatType;
import com.taobao.search.iquan.client.common.metrics.QueryMetricsReporter;
import com.taobao.search.iquan.client.common.response.SqlResponse;
import com.taobao.search.iquan.client.common.service.CatalogService;
import com.taobao.search.iquan.client.common.service.FunctionService;
import com.taobao.search.iquan.client.common.service.SqlQueryService;
import com.taobao.search.iquan.client.common.service.TableService;
import com.taobao.search.iquan.client.common.utils.ClientUtils;
import com.taobao.search.iquan.client.common.utils.ErrorUtils;
import com.taobao.search.iquan.client.common.json.api.JsonSqlConfig;
import com.taobao.search.iquan.core.api.SqlTranslator;
import com.taobao.search.iquan.core.api.common.IquanErrorCode;
import com.taobao.search.iquan.core.api.exception.SqlQueryException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

/**
 * interface design document:  https://yuque.antfin-inc.com/suez_turing/sql/dwtgnr
 */
public class IquanClient {
    private static final Logger logger = LoggerFactory.getLogger(IquanClient.class);
    private SqlTranslator translator;

    public IquanClient(int format, byte[] params) {
        JsonSqlConfig sqlConfig = ClientUtils.parseRequest(format, params, JsonSqlConfig.class);
        if (!sqlConfig.isValid()) {
            throw new SqlQueryException(IquanErrorCode.IQUAN_EC_CLIENT_INIT_FAIL, "params is not valid.");
        }

        translator = new SqlTranslator(sqlConfig.debugFlag);
    }

    public byte[] updateCatalog(int format, byte[] request) {
        SqlResponse response;

        try {
            Map<String, Object> reqMap = ClientUtils.parseMapRequest(format, request);
            response = CatalogService.updateCatalog(translator, reqMap);
        } catch (SqlQueryException e) {
            response = new SqlResponse();
            ErrorUtils.setSqlQueryException(e, response);
        } catch (Exception e) {
            response = new SqlResponse();
            ErrorUtils.setException(e, response);
        }
        return ClientUtils.toResponseByte(FormatType.JSON.getIndex(), response);
    }

    public byte[] updateFunctions(int format, byte[] request) {
        SqlResponse response;

        try {
            Map<String, Object> reqMap = ClientUtils.parseMapRequest(format, request);
            response = FunctionService.updateFunctions(translator, reqMap);
        } catch (SqlQueryException e) {
            response = new SqlResponse();
            ErrorUtils.setSqlQueryException(e, response);
        } catch (Exception e) {
            response = new SqlResponse();
            ErrorUtils.setException(e, response);
        }
        return ClientUtils.toResponseByte(FormatType.JSON.getIndex(), response);
    }

    public byte[] updateTables(int format, byte[] request) {
        SqlResponse response;

        try {
            Map<String, Object> reqMap = ClientUtils.parseMapRequest(format, request);
            response = TableService.updateTables(translator, reqMap);
        } catch (SqlQueryException e) {
            response = new SqlResponse();
            ErrorUtils.setSqlQueryException(e, response);
        } catch (Exception e) {
            response = new SqlResponse();
            ErrorUtils.setException(e, response);
        }
        return ClientUtils.toResponseByte(FormatType.JSON.getIndex(), response);
    }

    public byte[] updateLayerTables(int format, byte[] request) {
        SqlResponse response;

        try {
            Map<String, Object> reqMap = ClientUtils.parseMapRequest(format, request);
            response = TableService.updateLayerTables(translator, reqMap);
        } catch (SqlQueryException e) {
            response = new SqlResponse();
            ErrorUtils.setSqlQueryException(e, response);
        } catch (Exception e) {
            response = new SqlResponse();
            ErrorUtils.setException(e, response);
        }
        return ClientUtils.toResponseByte(FormatType.JSON.getIndex(), response);
    }

    public byte[] sqlQuery(int inputFormat, int outputFormat, byte[] request) {
        SqlResponse response;
        FormatType inputFormatType = FormatType.from(inputFormat);
        FormatType outputFormatType = FormatType.from(outputFormat);
        long startNs = System.nanoTime();

        try {
            Map<String, Object> reqMap = ClientUtils.parseMapRequest(inputFormatType.getIndex(), request);
            response = SqlQueryService.sqlQuery(translator, reqMap);
        } catch (SqlQueryException e) {
            response = new SqlResponse();
            ErrorUtils.setSqlQueryException(e, response);
        } catch (Exception e) {
            response = new SqlResponse();
            ErrorUtils.setException(e, response);
        }

        long endNs = System.nanoTime();
        KMonitor kMonitor = QueryMetricsReporter.getkMonitor();
        if (kMonitor != null) {
            kMonitor.report(QueryMetricsReporter.In_Qps, 1.0);
            kMonitor.report(QueryMetricsReporter.In_LatencyUs, (endNs - startNs) / 1000.0);
            if (response.isOk()) {
                kMonitor.report(QueryMetricsReporter.In_Success_Qps, 1.0);
                kMonitor.report(QueryMetricsReporter.In_Success_LatencyUs, (endNs - startNs) / 1000.0);
            } else {
                kMonitor.report(QueryMetricsReporter.In_Error_Qps, 1.0);
            }
        }

        return ClientUtils.toResponseByte(outputFormatType.getIndex(), response);
    }

    public byte[] listCatalog() {
        SqlResponse response;

        try {
            response = CatalogService.getCatalogNames(translator);
        } catch (SqlQueryException e) {
            response = new SqlResponse();
            ErrorUtils.setSqlQueryException(e, response);
        } catch (Exception e) {
            response = new SqlResponse();
            ErrorUtils.setException(e, response);
        }
        return ClientUtils.toResponseByte(FormatType.JSON.getIndex(), response);
    }

    public byte[] listDatabase(int format, byte[] request) {
        SqlResponse response;

        try {
            Map<String, Object> reqMap = ClientUtils.parseMapRequest(format, request);
            response = CatalogService.getDatabaseNames(translator, reqMap);
        } catch (SqlQueryException e) {
            response = new SqlResponse();
            ErrorUtils.setSqlQueryException(e, response);
        } catch (Exception e) {
            response = new SqlResponse();
            ErrorUtils.setException(e, response);
        }
        return ClientUtils.toResponseByte(FormatType.JSON.getIndex(), response);
    }

    public byte[] listTables(int format, byte[] request) {
        SqlResponse response;

        try {
            Map<String, Object> reqMap = ClientUtils.parseMapRequest(format, request);
            response = CatalogService.getTableNames(translator, reqMap);
        } catch (SqlQueryException e) {
            response = new SqlResponse();
            ErrorUtils.setSqlQueryException(e, response);
        } catch (Exception e) {
            response = new SqlResponse();
            ErrorUtils.setException(e, response);
        }
        return ClientUtils.toResponseByte(FormatType.JSON.getIndex(), response);
    }

    public byte[] listFunctions(int format, byte[] request) {
        SqlResponse response;

        try {
            Map<String, Object> reqMap = ClientUtils.parseMapRequest(format, request);
            response = CatalogService.getFunctionNames(translator, reqMap);
        } catch (SqlQueryException e) {
            response = new SqlResponse();
            ErrorUtils.setSqlQueryException(e, response);
        } catch (Exception e) {
            response = new SqlResponse();
            ErrorUtils.setException(e, response);
        }
        return ClientUtils.toResponseByte(FormatType.JSON.getIndex(), response);
    }

    public byte[] getTableDetails(int format, byte[] request) {
        SqlResponse response;

        try {
            Map<String, Object> reqMap = ClientUtils.parseMapRequest(format, request);
            response = CatalogService.getTableDetail(translator, reqMap);
        } catch (SqlQueryException e) {
            response = new SqlResponse();
            ErrorUtils.setSqlQueryException(e, response);
        } catch (Exception e) {
            response = new SqlResponse();
            ErrorUtils.setException(e, response);
        }
        return ClientUtils.toResponseByte(FormatType.JSON.getIndex(), response);
    }

    public byte[] getFunctionDetails(int format, byte[] request) {
        SqlResponse response;

        try {
            Map<String, Object> reqMap = ClientUtils.parseMapRequest(format, request);
            response = CatalogService.getFunctionDetail(translator, reqMap);
        } catch (SqlQueryException e) {
            response = new SqlResponse();
            ErrorUtils.setSqlQueryException(e, response);
        } catch (Exception e) {
            response = new SqlResponse();
            ErrorUtils.setException(e, response);
        }
        return ClientUtils.toResponseByte(FormatType.JSON.getIndex(), response);
    }

    public byte[] dumpCatalog() {
        SqlResponse response;

        try {
            response = CatalogService.dumpCatalog(translator);
        } catch (SqlQueryException e) {
            response = new SqlResponse();
            ErrorUtils.setSqlQueryException(e, response);
        } catch (Exception e) {
            response = new SqlResponse();
            ErrorUtils.setException(e, response);
        }
        return ClientUtils.toResponseByte(FormatType.JSON.getIndex(), response);
    }

    // only for debug
    public SqlTranslator getTranslator() {
        return translator;
    }
}
