package com.taobao.search.iquan.client.common.model;

import com.taobao.search.iquan.client.common.json.function.JsonUdxfFunction;
import com.taobao.search.iquan.core.api.exception.ExceptionUtils;
import com.taobao.search.iquan.core.api.exception.IquanNotValidateException;
import com.taobao.search.iquan.core.utils.IquanRelOptUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class IquanUdxfModel extends IquanFunctionModel {
    private static final Logger logger = LoggerFactory.getLogger(IquanUdxfModel.class);

    // for internal use
    private JsonUdxfFunction contentObj;

    private void parseContent() {
        if (contentObj != null) {
            return;
        }
        contentObj = IquanRelOptUtils.fromJson(function_content, JsonUdxfFunction.class);
    }

    public JsonUdxfFunction getContentObj() {
        parseContent();
        return contentObj;
    }

    @Override
    public boolean isValid() {
        try {
            ExceptionUtils.throwIfTrue(!isPathValid(), "udxf path is not valid");
            ExceptionUtils.throwIfTrue(function_version <= 0, "udxf function_version is smaller than 0");
            ExceptionUtils.throwIfTrue(!function_type.isValid(),
                                        "udxf function_type is not valid");
            ExceptionUtils.throwIfTrue(function_content_version.isEmpty(),
                                        "udxf function_content_version.isEmpty()");
            ExceptionUtils.throwIfTrue(function_content.isEmpty(),
                                        "udxf function_content is empty");
        } catch (IquanNotValidateException e) {
            logger.error("udxf function model is not valid: " + getDigest());
            logger.error(e.getMessage());
            return false;
        }

        parseContent();
        if (!contentObj.isValid(function_type)) {
            logger.error("function content is not valid: " + getDigest());
            return false;
        }
        return true;
    }
}
