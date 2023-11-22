package com.taobao.search.iquan.client.common.model;

import com.taobao.search.iquan.client.common.json.function.JsonTvfFunction;
import com.taobao.search.iquan.core.api.exception.ExceptionUtils;
import com.taobao.search.iquan.core.api.exception.IquanNotValidateException;
import com.taobao.search.iquan.core.utils.IquanRelOptUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class IquanTvfModel extends IquanFunctionModel {
    private static final Logger logger = LoggerFactory.getLogger(IquanUdxfModel.class);

    // for internal use
    private JsonTvfFunction contentObj;

    private void parseContent() {
        if (contentObj != null) {
            return;
        }
        contentObj = IquanRelOptUtils.fromJson(function_content, JsonTvfFunction.class);
    }

    public JsonTvfFunction getContentObj() {
        parseContent();
        return contentObj;
    }

    @Override
    public boolean isValid() {
        try {
            ExceptionUtils.throwIfTrue(!isPathValid(), "tvf path is not valid");
            ExceptionUtils.throwIfTrue(function_version <= 0, "tvf function_version is smaller than 0");
            ExceptionUtils.throwIfTrue(!function_type.isValid(),
                    "tvf function_type is not valid");
            ExceptionUtils.throwIfTrue(function_content_version.isEmpty(),
                    "tvf function_content_version.isEmpty()");
            ExceptionUtils.throwIfTrue(function_content.isEmpty(),
                    "tvf function_content is empty");
        } catch (IquanNotValidateException e) {
            logger.error("tvf function model is not valid: " + getDigest());
            logger.error(e.getMessage());
            return false;
        }

        parseContent();
        if (!contentObj.isValid()) {
            logger.error("function content object is not valid: " + getDigest());
            return false;
        }
        return true;
    }
}
