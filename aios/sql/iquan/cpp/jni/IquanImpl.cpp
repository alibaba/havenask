/*
 * Copyright 2014-present Alibaba Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
#include "iquan/jni/IquanImpl.h"

#include <memory>
#include <mutex>

#include "autil/HashAlgorithm.h"
#include "iquan/common/Common.h"
#include "iquan/common/Utils.h"
#include "iquan/common/catalog/InspectDef.h"
#include "iquan/jni/ConstantJvmDefine.h"
#include "iquan/jni/IquanDqlResponse.h"
#include "iquan/jni/WarmupService.h"
#include "iquan/jni/LayerTableNorm.h"

namespace iquan {

AUTIL_LOG_SETUP(iquan, IquanImpl);
alog::Logger* IquanImpl::_warmupLogger = alog::Logger::getLogger("sql.SqlWarmupLog");

Status IquanImpl::init(const JniConfig &jniConfig, const ClientConfig &sqlConfig) {
    try {
        if (unlikely(!jniConfig.isValid())) {
            std::string content;
            IQUAN_ENSURE_FUNC(Utils::toJson(jniConfig, content));
            return Status(IQUAN_INVALID_PARAMS, "jniConfig is invalid: " + content);
        }
        if (unlikely(!sqlConfig.isValid())) {
            std::string content;
            IQUAN_ENSURE_FUNC(Utils::toJson(sqlConfig, content));
            return Status(IQUAN_INVALID_PARAMS, "sqlConfig is invalid: " + content);
        }

        _jniConfig = jniConfig;

        std::map<std::string, CacheConfig>::const_iterator iter = sqlConfig.cacheConfigs.find(IQUAN_JNI_POST_OPTIMIZE);
        if (iter == sqlConfig.cacheConfigs.end()) {
            _planCache.reset();
            _planMetaCache.reset();
        } else {
            _planCache.reset(new IquanCache<std::shared_ptr<autil::legacy::RapidDocument>>(iter->second));
            _planCache->reset();
            _planMetaCache.reset(new IquanCache<PlanMetaPtr>(iter->second));
            _planMetaCache->reset();
            AUTIL_LOG(DEBUG, "init cpp LRUCache success.");
        }

        std::string sqlConfigStr;
        IQUAN_ENSURE_FUNC(Utils::toJson(sqlConfig, sqlConfigStr));
        LocalRef<JByteArray> sqlConfigByteArray = JByteArrayFromStdString(sqlConfigStr);
        IQUAN_RETURN_ERROR_IF_NULL(sqlConfigByteArray, IQUAN_NEW_INSTANCE_FAILED,
                "failed to create sqlConfig byte array");
        _sqlConfigByteArray = sqlConfigByteArray;

        _iquanClient = JIquanClient::newInstanceWithClassLoader(
                JIquanClient::getSelfClazz(), INPUT_JSON_FORMAT, _sqlConfigByteArray);
        IQUAN_RETURN_ERROR_IF_NULL(_iquanClient, IQUAN_NEW_INSTANCE_FAILED,
                "fail to create iquan client object");
    } catch (const JnippException &e) {
        return Status(IQUAN_JNI_EXCEPTION, e.what());
    } catch (const IquanException &e) {
        return Status(e.code(), e.what());
    } catch (const std::exception &e) {
        return Status(IQUAN_FAIL, e.what());
    }
    return Status::OK();
}

Status IquanImpl::updateTables(const TableModels &tables) {
    try {
        if (unlikely(!tables.isValid())) {
            return Status(IQUAN_INVALID_PARAMS, "tableModels is not valid");
        }
        std::string contentStr;
        IQUAN_ENSURE_FUNC(Utils::toJson(tables, contentStr));
        IQUAN_ENSURE_FUNC(updateTables(contentStr));
    } catch (const JnippException &e) {
        return Status(IQUAN_JNI_EXCEPTION, e.what());
    } catch (const IquanException &e) {
        return Status(e.code(), e.what());
    } catch (const std::exception &e) {
        return Status(IQUAN_FAIL, e.what());
    }
    return Status::OK();
}

Status IquanImpl::updateTables(const std::string &tableContent) {
    try {
        LocalRef<JByteArray> requestByteArray = JByteArrayFromStdString(tableContent);
        IQUAN_RETURN_ERROR_IF_NULL(requestByteArray,
                                   IQUAN_NEW_INSTANCE_FAILED,
                                   "fail to create request byte array in IquanImpl::updateTables()");
        AUTIL_LOG(INFO, "iquan updateTables content[%s]", tableContent.c_str());
        LocalRef<JByteArray> responseByteArray = _iquanClient->updateTables(INPUT_JSON_FORMAT, requestByteArray);
        std::string responseStr = JByteArrayToStdString(responseByteArray);
        IQUAN_ENSURE_FUNC(ResponseHeader::check(responseStr));
    } catch (const JnippException &e) {
        return Status(IQUAN_JNI_EXCEPTION, e.what());
    } catch (const IquanException &e) {
        return Status(e.code(), e.what());
    } catch (const std::exception &e) {
        return Status(IQUAN_FAIL, e.what());
    }
    return Status::OK();
}

Status IquanImpl::updateLayerTables(const LayerTableModels &tables) {
    try {
        if (unlikely(!tables.isValid())) {
            return Status(IQUAN_INVALID_PARAMS, "tableModels is not valid");
        }
        std::string contentStr;
        IQUAN_ENSURE_FUNC(Utils::toJson(tables, contentStr));
        IQUAN_ENSURE_FUNC(updateLayerTables(contentStr));
    } catch (const JnippException &e) {
        return Status(IQUAN_JNI_EXCEPTION, e.what());
    } catch (const IquanException &e) {
        return Status(e.code(), e.what());
    } catch (const std::exception &e) {
        return Status(IQUAN_FAIL, e.what());
    }
    return Status::OK();
}

bool IquanImpl::addLayerTableMeta(const LayerTableModels &models) {
    for (const auto &table : models.tables) {
        LayerTableMetaPtr ptr(new LayerTableMeta(table.tableContent));
        if(!_dynamicParamsManager.addLayerTableMeta(table.tableName, ptr)) {
            AUTIL_LOG(DEBUG, "add LayerTableMeta: [%s] failed", table.tableName.c_str());
            return false;
        }
        AUTIL_LOG(DEBUG, "add LayerTableMeta: [%s] succeed", table.tableName.c_str());
    }
    return true;
}

Status IquanImpl::updateLayerTables(const std::string &tableContent) {
    try {
        LocalRef<JByteArray> requestByteArray = JByteArrayFromStdString(tableContent);
        IQUAN_RETURN_ERROR_IF_NULL(requestByteArray,
                                   IQUAN_NEW_INSTANCE_FAILED,
                                   "fail to create request byte array in IquanImpl::updateLayerTables()");
        AUTIL_LOG(INFO, "iquan updateLayerTables content[%s]", tableContent.c_str());
        LocalRef<JByteArray> responseByteArray = _iquanClient->updateLayerTables(INPUT_JSON_FORMAT, requestByteArray);
        std::string responseStr = JByteArrayToStdString(responseByteArray);
        IQUAN_ENSURE_FUNC(ResponseHeader::check(responseStr));

        LayerTableModels models;
        std::shared_ptr<autil::legacy::RapidDocument> documentPtr(new autil::legacy::RapidDocument);
        documentPtr->Parse(tableContent.c_str());
        if (documentPtr->HasParseError()) {
            AUTIL_LOG(ERROR, "parse tablecontent failed");
            throw IquanException("failed to parse response json string using rapid json", IQUAN_FAIL);
        }
        IQUAN_ENSURE_FUNC(Utils::fromRapidValue(models, documentPtr));
        if(!addLayerTableMeta(models)) {
            return Status(IQUAN_LAYER_TABLE_ERROR, "add LayerTableMeta failed");
        }
    } catch (const JnippException &e) {
        return Status(IQUAN_JNI_EXCEPTION, e.what());
    } catch (const IquanException &e) {
        return Status(e.code(), e.what());
    } catch (const std::exception &e) {
        return Status(IQUAN_FAIL, e.what());
    }
    return Status::OK();
}

Status IquanImpl::updateFunctions(FunctionModels &functions) {
    try {
        if (unlikely(!functions.isValid())) {
            return Status(IQUAN_INVALID_PARAMS, "functionModels is not valid");
        }
        std::string contentStr;
        IQUAN_ENSURE_FUNC(Utils::toJson(functions, contentStr));
        IQUAN_ENSURE_FUNC(updateFunctions(contentStr));
    } catch (const JnippException &e) {
        return Status(IQUAN_JNI_EXCEPTION, e.what());
    } catch (const IquanException &e) {
        return Status(e.code(), e.what());
    } catch (const std::exception &e) {
        return Status(IQUAN_FAIL, e.what());
    }
    return Status::OK();
}

Status IquanImpl::updateFunctions(const TvfModels &functions) {
    std::string functionContent;
    IQUAN_ENSURE_FUNC(Utils::toJson(functions, functionContent));
    IQUAN_ENSURE_FUNC(updateFunctions(functionContent));
    return Status::OK();
}

Status IquanImpl::updateFunctions(const std::string &functionContent) {
    try {
        LocalRef<JByteArray> requestByteArray = JByteArrayFromStdString(functionContent);
        IQUAN_RETURN_ERROR_IF_NULL(requestByteArray,
                                   IQUAN_NEW_INSTANCE_FAILED,
                                   "fail to create request byte array in IquanImpl::updateFunctions()");
        AUTIL_LOG(DEBUG, "iquan updateFunctions content[%s]", functionContent.c_str());
        LocalRef<JByteArray> responseByteArray = _iquanClient->updateFunctions(INPUT_JSON_FORMAT, requestByteArray);
        std::string responseStr = JByteArrayToStdString(responseByteArray);
        IQUAN_ENSURE_FUNC(ResponseHeader::check(responseStr));
    } catch (const JnippException &e) {
        return Status(IQUAN_JNI_EXCEPTION, e.what());
    } catch (const IquanException &e) {
        return Status(e.code(), e.what());
    } catch (const std::exception &e) {
        return Status(IQUAN_FAIL, e.what());
    }
    return Status::OK();
}

Status IquanImpl::updateCatalog(const CatalogInfo &catalog) {
    try {
        if (unlikely(!catalog.isValid())) {
            AUTIL_LOG(WARN,
                      "catalog info is not valid [%s], tableModels[%d] layerTableModels[%d] functionsModels[%d] tvfModels[%d]",
                      autil::legacy::ToJsonString(catalog).c_str(),
                      catalog.tableModels.isValid(),
                      catalog.layerTableModels.isValid(),
                      catalog.functionModels.isValid(),
                      catalog.tvfFunctionModels.isValid());
            return Status(IQUAN_INVALID_PARAMS, "catalog info is not valid.");
        }
        std::string contentStr;
        IQUAN_ENSURE_FUNC(Utils::toJson(catalog, contentStr));
        AUTIL_LOG(DEBUG, "iquan updateCatalog content[%s]", contentStr.c_str());
        LocalRef<JByteArray> requestByteArray = JByteArrayFromStdString(contentStr);
        IQUAN_RETURN_ERROR_IF_NULL(requestByteArray,
                                   IQUAN_NEW_INSTANCE_FAILED,
                                   "fail to create request byte array in IquanImpl::updateCatalog()");

        LocalRef<JByteArray> responseByteArray = _iquanClient->updateCatalog(INPUT_JSON_FORMAT, requestByteArray);
        std::string responseStr = JByteArrayToStdString(responseByteArray);
        IQUAN_ENSURE_FUNC(ResponseHeader::check(responseStr));
    } catch (const JnippException &e) {
        return Status(IQUAN_JNI_EXCEPTION, e.what());
    } catch (const IquanException &e) { return Status(e.code(), e.what()); } catch (const std::exception &e) {
        return Status(IQUAN_FAIL, e.what());
    }
    return Status::OK();
}

std::string genFinalKeyStr(const std::string &firstHaskKey, const std::string &secHashKey) {
    return firstHaskKey + "@@@###$$$" + secHashKey;
}

bool IquanImpl::getSecHashKeyStr(PlanMetaPtr planMetaPtr,
                                 const DynamicParams &dynamicParams,
                                 std::string &hashStr)
{
    const auto &paramArray = dynamicParams._array;
    hashStr.clear();
    if (paramArray.empty()) {
        return true;
    }
    size_t cnt = paramArray[0].size();
    std::vector<autil::legacy::Any> params;
    for (int i = 0; i < cnt; ++i) {
        params.push_back(dynamicParams.at(i, 0));
    }
    return _dynamicParamsManager.normLayerTable(planMetaPtr->getLayerTableMetas(),
            params, hashStr);
}


Status IquanImpl::getFromCache(IquanDqlRequest &request,
                             IquanDqlResponse &response,
                             std::string &hashKeyStr,
                             std::string &secHashKeyStr,
                             std::string &finalHashKeyStr,
                             PlanCacheStatus &planCacheStatus)
{
    IQUAN_ENSURE_FUNC(getHashKeyStr(request, hashKeyStr));

    PlanMetaPtr planMetaPtr = nullptr;
    Status statusOne = _planMetaCache->get(hashKeyStr, planMetaPtr);
    if (statusOne.ok() && planMetaPtr != nullptr) {
        SqlPlan &sqlPlan = response.sqlPlan;
        planCacheStatus.planMetaGet = true;
        if (getSecHashKeyStr(planMetaPtr, request.dynamicParams, secHashKeyStr)) {
            finalHashKeyStr = genFinalKeyStr(hashKeyStr, secHashKeyStr);
            std::shared_ptr<autil::legacy::RapidDocument> documentPtr = nullptr;
            uint64_t hashKey = 0;
            Status status = _planCache->get(finalHashKeyStr, documentPtr, &hashKey);
            if (status.ok() && documentPtr != nullptr) {
                IQUAN_ENSURE_FUNC(Utils::fromRapidValue(response, documentPtr));
                sqlPlan.rawRapidDoc = documentPtr;
                planCacheStatus.planGet = true;
                request.queryHashKey = hashKey;
                AUTIL_LOG(DEBUG, "match plan cache!");
                return Status::OK();
            }
        }
    }
    AUTIL_LOG(DEBUG, "miss plan cache!");
    return Status(IQUAN_GET_FROM_CACHE_FAILED, "plan cache miss");
}

void IquanImpl::putCache(SqlPlan &sqlPlan,
                         IquanDqlRequest &request,
                         IquanDqlResponse &response,
                         std::string &hashKeyStr,
                         std::string &secHashKeyStr,
                         std::string &finalHashKeyStr,
                         size_t planStrSize,
                         std::shared_ptr<autil::legacy::RapidDocument> documentPtr,
                         PlanCacheStatus &planCacheStatus)
{
    if (!planCacheStatus.planMetaGet) {
        PlanMetaPtr planMetaPtr(new PlanMeta());
        for (const auto &planMetaDef : sqlPlan.sqlPlanMeta.layerTableMeta) {
            planMetaPtr->getLayerTableMetas().emplace_back(planMetaDef);
        }
        size_t fixSize = 1000;
        Status status = _planMetaCache->put(hashKeyStr, fixSize, planMetaPtr);
        if (unlikely(!status.ok())) {
            AUTIL_LOG(ERROR, "put first stage planMeta fail");
        }
        if (!getSecHashKeyStr(planMetaPtr, request.dynamicParams, secHashKeyStr)) {
            AUTIL_LOG(ERROR, "generate secHashKey fail!");
        }
        planCacheStatus.planMetaPut = true;
    }

    finalHashKeyStr = genFinalKeyStr(hashKeyStr, secHashKeyStr);
    uint64_t hashKey = 0;
    Status status = _planCache->put(finalHashKeyStr, planStrSize, documentPtr, &hashKey);
    if (unlikely(!status.ok())) {
        AUTIL_LOG(ERROR, "put document plan to cache fail");
        return;
    }
    AUTIL_LOG(DEBUG, "put to plan cache!");
    planCacheStatus.planPut = true;
    request.queryHashKey = hashKey;
    writeWarnupLog(request);
}

Status IquanImpl::query(IquanDqlRequest &request, IquanDqlResponse &response, PlanCacheStatus &planCacheStatus) {
    try {
        if (unlikely(!request.isValid())) {
            return Status(IQUAN_INVALID_PARAMS, "IquanDqlRequest is invalid");
        }

        std::string enabelCacheStr = readValue(request.sqlParams, IQUAN_PLAN_CACHE_ENALE, IQUAN_FALSE);
        std::string prepareLevel =
            readValue(request.sqlParams, IQUAN_PLAN_PREPARE_LEVEL, std::string(IQUAN_JNI_POST_OPTIMIZE));
        std::string planFormatType = readValue(request.sqlParams, IQUAN_PLAN_FORMAT_TYPE, IQUAN_JSON);
        autil::StringUtil::toLowerCase(enabelCacheStr);
        autil::StringUtil::toLowerCase(prepareLevel);
        autil::StringUtil::toLowerCase(planFormatType);

        bool enableCache = false;
        if ((_planCache != nullptr) && (enabelCacheStr == IQUAN_TRUE) && (planFormatType == IQUAN_JSON)) {
            enableCache = true;
        }

        SqlPlan &sqlPlan = response.sqlPlan;

        // get from cache
        planCacheStatus.reset();
        std::string hashKeyStr;
        std::string secHashKeyStr;
        std::string finalHashKeyStr;
        if (enableCache) {
            if (getFromCache(request, response, hashKeyStr, secHashKeyStr, finalHashKeyStr, planCacheStatus).ok()) {
                return Status::OK();
            }
        }

        // query sql
        std::string requestStr;
        IQUAN_ENSURE_FUNC(Utils::fastToJson(request, requestStr));
        LocalRef<JByteArray> requestByteArray = JByteArrayFromStdString(requestStr);

        LocalRef<JByteArray> responseByteArray =
            _iquanClient->query(INPUT_JSON_FORMAT, OUTPUT_JSON_FORMAT, requestByteArray);
        std::string responseStr = JByteArrayToStdString(responseByteArray);
        responseStr += IQUAN_DYNAMIC_PARAMS_SIMD_PADDING;

        std::shared_ptr<autil::legacy::RapidDocument> documentPtr(new autil::legacy::RapidDocument);
        documentPtr->Parse(responseStr.c_str());
        if (documentPtr->HasParseError()) {
            throw IquanException("failed to parse response json string using rapid json", IQUAN_FAIL);
        }

        IQUAN_ENSURE_FUNC(Utils::fromRapidValue(response, documentPtr));
        if (unlikely(response.errorCode != IQUAN_OK)) {
            return Status(response.errorCode, response.errorMsg);
        }

        sqlPlan.rawRapidDoc = documentPtr;
        // put to cache
        if (likely(enableCache)) {
            putCache(sqlPlan, request, response, hashKeyStr, secHashKeyStr, finalHashKeyStr, responseStr.size(), documentPtr, planCacheStatus);
        }
    } catch (const JnippException &e) {
        return Status(IQUAN_JNI_EXCEPTION, e.what());
    } catch (const IquanException &e) {
        return Status(e.code(), e.what());
    } catch (const std::exception &e) {
        return Status(IQUAN_FAIL, e.what());
    }
    return Status::OK();
}

Status IquanImpl::listCatalogs(std::string &result) {
    Status status = Status::OK();
    try {
        LocalRef<jbyteArray> resultByteArray = _iquanClient->listCatalogs();
        result = JByteArrayToStdString(resultByteArray);
        IQUAN_ENSURE_FUNC(ResponseHeader::check(result));
    } catch (const JnippException &e) {
        return Status(IQUAN_JNI_EXCEPTION, e.what());
    } catch (const IquanException &e) {
        return Status(e.code(), e.what());
    } catch (const std::exception &e) {
        return Status(IQUAN_FAIL, e.what());
    }
    return status;
}

Status IquanImpl::listDatabases(const std::string &catalogName, std::string &result) {
    Status status = Status::OK();
    try {
        InspectCatalogDef catalogDef;
        catalogDef.catalogName = catalogName;
        if (unlikely(!catalogDef.isValid())) {
            return Status(IQUAN_INVALID_PARAMS, "InspectCatalogDef is not valid");
        }

        std::string requestStr;
        IQUAN_ENSURE_FUNC(Utils::toJson(catalogDef, requestStr));
        LocalRef<JByteArray> requestByteArray = JByteArrayFromStdString(requestStr);

        LocalRef<JByteArray> responseByteArray =
            _iquanClient->listDatabases(INPUT_JSON_FORMAT, requestByteArray);
        result = JByteArrayToStdString(responseByteArray);
        IQUAN_ENSURE_FUNC(ResponseHeader::check(result));
    } catch (const JnippException &e) {
        return Status(IQUAN_JNI_EXCEPTION, e.what());
    } catch (const IquanException &e) {
        return Status(e.code(), e.what());
    } catch (const std::exception &e) {
        return Status(IQUAN_FAIL, e.what());
    }
    return status;
}

Status IquanImpl::listTables(const std::string &catalogName,
                             const std::string &dbName, std::string &result)
{
    Status status = Status::OK();
    try {
        InspectDataBaseDef inspectDatabaseDef;
        inspectDatabaseDef.catalogName = catalogName;
        inspectDatabaseDef.databaseName = dbName;
        if (unlikely(!inspectDatabaseDef.isValid())) {
            return Status(IQUAN_INVALID_PARAMS, "InspectDataBaseDef is not valid");
        }

        std::string requestStr;
        IQUAN_ENSURE_FUNC(Utils::toJson(inspectDatabaseDef, requestStr));
        LocalRef<JByteArray> requestByteArray = JByteArrayFromStdString(requestStr);

        LocalRef<JByteArray> responseByteArray =
            _iquanClient->listTables(INPUT_JSON_FORMAT, requestByteArray);
        result = JByteArrayToStdString(responseByteArray);
        IQUAN_ENSURE_FUNC(ResponseHeader::check(result));
    } catch (const JnippException &e) {
        return Status(IQUAN_JNI_EXCEPTION, e.what());
    } catch (const IquanException &e) {
        return Status(e.code(), e.what());
    } catch (const std::exception &e) {
        return Status(IQUAN_FAIL, e.what());
    }
    return status;
}

Status IquanImpl::listFunctions(const std::string &catalogName,
                                const std::string &dbName,
                                std::string &result)
{
    Status status = Status::OK();
    try {
        InspectDataBaseDef inspectDatabaseDef;
        inspectDatabaseDef.catalogName = catalogName;
        inspectDatabaseDef.databaseName = dbName;
        if (unlikely(!inspectDatabaseDef.isValid())) {
            return Status(IQUAN_INVALID_PARAMS, "InspectDataBaseDef is not valid");
        }

        std::string requestStr;
        IQUAN_ENSURE_FUNC(Utils::toJson(inspectDatabaseDef, requestStr));
        LocalRef<JByteArray> requestByteArray = JByteArrayFromStdString(requestStr);

        LocalRef<JByteArray> responseByteArray =
            _iquanClient->listFunctions(INPUT_JSON_FORMAT, requestByteArray);
        result = JByteArrayToStdString(responseByteArray);
        IQUAN_ENSURE_FUNC(ResponseHeader::check(result));
    } catch (const JnippException &e) {
        return Status(IQUAN_JNI_EXCEPTION, e.what());
    } catch (const IquanException &e) {
        return Status(e.code(), e.what());
    } catch (const std::exception &e) {
        return Status(IQUAN_FAIL, e.what());
    }
    return status;
}

Status IquanImpl::getTableDetails(const std::string &catalogName,
                                  const std::string &dbName,
                                  const std::string &tableName,
                                  std::string &result)
{
    Status status = Status::OK();
    try {
        InspectTableDef inspectTableDef;
        inspectTableDef.catalogName = catalogName;
        inspectTableDef.databaseName = dbName;
        inspectTableDef.tableName = tableName;
        if (unlikely(!inspectTableDef.isValid())) {
            return Status(IQUAN_INVALID_PARAMS, "InspectTableDef is not valid");
        }

        std::string requestStr;
        IQUAN_ENSURE_FUNC(Utils::toJson(inspectTableDef, requestStr));
        LocalRef<JByteArray> requestByteArray = JByteArrayFromStdString(requestStr);

        LocalRef<JByteArray> responseByteArray =
            _iquanClient->getTableDetails(INPUT_JSON_FORMAT, requestByteArray);
        result = JByteArrayToStdString(responseByteArray);
        IQUAN_ENSURE_FUNC(ResponseHeader::check(result));
    } catch (const JnippException &e) {
        return Status(IQUAN_JNI_EXCEPTION, e.what());
    } catch (const IquanException &e) {
        return Status(e.code(), e.what());
    } catch (const std::exception &e) {
        return Status(IQUAN_FAIL, e.what());
    }
    return status;
}

Status IquanImpl::getFunctionDetails(const std::string &catalogName,
                                     const std::string &dbName,
                                     const std::string &functionName,
                                     std::string &result)
{
    Status status = Status::OK();
    try {
        InspectFunctionDef inspectFunctionDef;
        inspectFunctionDef.catalogName = catalogName;
        inspectFunctionDef.databaseName = dbName;
        inspectFunctionDef.functionName = functionName;
        if (unlikely(!inspectFunctionDef.isValid())) {
            return Status(IQUAN_INVALID_PARAMS, "InspectFunctionDef is not valid");
        }

        std::string requestStr;
        IQUAN_ENSURE_FUNC(Utils::toJson(inspectFunctionDef, requestStr));
        LocalRef<JByteArray> requestByteArray =
            JByteArrayFromStdString(requestStr);

        LocalRef<JByteArray> responseByteArray =
            _iquanClient->getFunctionDetails(INPUT_JSON_FORMAT, requestByteArray);
        result = JByteArrayToStdString(responseByteArray);
        IQUAN_ENSURE_FUNC(ResponseHeader::check(result));
    } catch (const JnippException &e) {
        return Status(IQUAN_JNI_EXCEPTION, e.what());
    } catch (const IquanException &e) {
        return Status(e.code(), e.what());
    } catch (const std::exception &e) {
        return Status(IQUAN_FAIL, e.what());
    }
    return status;
}

Status IquanImpl::dumpCatalog(std::string &result) {
    Status status = Status::OK();
    try {
        LocalRef<JByteArray> responseByteArray = _iquanClient->dumpCatalog();
        result = JByteArrayToStdString(responseByteArray);
        IQUAN_ENSURE_FUNC(ResponseHeader::check(result));
    } catch (const JnippException &e) {
        return Status(IQUAN_JNI_EXCEPTION, e.what());
    } catch (const IquanException &e) {
        return Status(e.code(), e.what());
    } catch (const std::exception &e) {
        return Status(IQUAN_FAIL, e.what());
    }
    return status;
}

Status IquanImpl::warmup(const WarmupConfig &warmupConfig) {
    if (unlikely(!warmupConfig.isValid())) {
        return Status(IQUAN_INVALID_PARAMS, "warmupConfig is not valid");
    }
    if (unlikely(warmupConfig.warmupFilePathList.empty())) {
        return Status(IQUAN_FILE_DOES_NOT_EXISTED, "warmup file path list is empty");
    }

    try {
        Status status = WarmupService::warmup(this, warmupConfig);
        if (unlikely(!status.ok())) {
            AUTIL_LOG(ERROR, "warmup failed: error code [%d], error message [%s]",
                      status.code(), status.errorMessage().c_str());
        }
    } catch (const JnippException &e) {
        AUTIL_LOG(ERROR, "JnippException [%s]", e.what());
    } catch (const IquanException &e) {
        AUTIL_LOG(ERROR, "IquanException [%s]", e.what());
    } catch (const std::exception &e) {
        AUTIL_LOG(ERROR, "exception [%s]", e.what());
    }
    return Status::OK();
}

void IquanImpl::writeWarnupLog(IquanDqlRequest &request) {
    static constexpr auto logLevel = alog::LOG_LEVEL_INFO;
    if (!_warmupLogger->isLevelEnabled(logLevel)) {
        return;
    }
    std::string jsonStr;
    auto status = request.toWarmupJson(jsonStr);
    if (status.ok()) {
        _warmupLogger->log(logLevel, "%s\n", jsonStr.c_str());
    }
}

std::string IquanImpl::readValue(const autil::legacy::json::JsonMap &params, const std::string &key, const std::string &defValue)
{
    std::string value;
    Status status = Utils::readValue(params, key, value);
    if (status.ok()) {
        return value;
    }
    return defValue;
}

uint64_t IquanImpl::getPlanCacheKeyCount() {
    return (_planCache != nullptr) ? _planCache->keyCount() : 0;
}

uint64_t IquanImpl::getPlanMetaCacheKeyCount() {
    return (_planMetaCache != nullptr) ? _planMetaCache->keyCount() : 0;
}

void IquanImpl::resetPlanCache() {
    _planCache->reset();
}

void IquanImpl::resetPlanMetaCache() {
     _planMetaCache->reset();
}

Status IquanImpl::getHashKeyStr(IquanDqlRequest &request, std::string &hashKeyStr) {
    IQUAN_ENSURE_FUNC(request.toCacheKey(hashKeyStr));
    return Status::OK();
}

} // namespace iquan
