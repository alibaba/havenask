#include "iquan/jni/test/testlib/TestUtils.h"

#include "iquan/common/Common.h"
#include "iquan/common/Utils.h"
#include "iquan/common/catalog/FunctionModel.h"
#include "iquan/common/catalog/TableModel.h"
namespace iquan {

AUTIL_LOG_SETUP(iquan, TestUtils);

std::shared_ptr<autil::legacy::RapidDocument> TestUtils::parseDocument(const std::string &content) {
    std::shared_ptr<autil::legacy::RapidDocument> documentPtr(new autil::legacy::RapidDocument);
    std::string newContent = content + IQUAN_DYNAMIC_PARAMS_SIMD_PADDING;
    documentPtr->Parse(newContent.c_str());
    if (documentPtr->HasParseError()) {
        return std::shared_ptr<autil::legacy::RapidDocument>();
    }
    return documentPtr;
}

Status TestUtils::formatJsonStr(const std::string &content, std::string &newContent) {
    autil::legacy::json::JsonMap jsonMap;
    IQUAN_ENSURE_FUNC(Utils::fromJson(jsonMap, content));
    IQUAN_ENSURE_FUNC(Utils::toJson(jsonMap, newContent));
    return Status::OK();
}

IquanPtr TestUtils::createIquan() {
    JniConfig jniConfig;
    ClientConfig sqlConfig;
    return createIquan(jniConfig, sqlConfig);
}

IquanPtr TestUtils::createIquan(const JniConfig &jniConfig, const ClientConfig &sqlConfig) {
    IquanPtr pIquan(new Iquan());
    Status status = pIquan->init(jniConfig, sqlConfig);
    if (status.ok()) {
        return pIquan;
    }
    AUTIL_LOG(ERROR,
              "TestUtils::createIquan() fail, error code[%d], error message[%s]",
              status.code(),
              status.errorMessage().c_str());
    return IquanPtr();
}

Status TestUtils::registerTable(IquanPtr &pIquan,
                                const std::string &catalogRootPath,
                                const std::string &tableName) {
    std::string content;
    std::string filePath = catalogRootPath + "/" + tableName;
    Status status = Utils::readFile(filePath, content);
    if (!status.ok()) {
        AUTIL_LOG(ERROR, "readFile from %s fail", filePath.c_str());
        return Status(IQUAN_FAIL, "fail");
    }
    status = pIquan->updateTables(content);
    if (!status.ok()) {
        AUTIL_LOG(ERROR, "updateTables for %s fail", filePath.c_str());
        return Status(IQUAN_FAIL, "fail");
    }
    return Status::OK();
}

Status TestUtils::registerTable(IquanPtr &pIquan,
                                const std::string &catalogRootPath,
                                const std::vector<std::string> &tableNames) {
    for (const std::string &name : tableNames) {
        IQUAN_ENSURE_FUNC(registerTable(pIquan, catalogRootPath, name));
    }
    return Status::OK();
}

Status TestUtils::registerLayerTable(IquanPtr &pIquan,
                                     const std::string &catalogRootPath,
                                     const std::string &tableName) {
    std::string content;
    std::string filePath = catalogRootPath + "/" + tableName;
    Status status = Utils::readFile(filePath, content);
    if (!status.ok()) {
        AUTIL_LOG(ERROR, "readFile from %s fail", filePath.c_str());
        return Status(IQUAN_FAIL, "fail");
    }
    status = pIquan->updateLayerTables(content);
    if (!status.ok()) {
        AUTIL_LOG(ERROR, "updateLayerTables for %s fail", filePath.c_str());
        return Status(IQUAN_FAIL, "fail");
    }
    return Status::OK();
}

Status TestUtils::registerLayerTables(IquanPtr &pIquan,
                                      const std::string &catalogRootPath,
                                      const std::vector<std::string> &tableNames) {
    for (const std::string &name : tableNames) {
        IQUAN_ENSURE_FUNC(registerLayerTable(pIquan, catalogRootPath, name));
    }
    return Status::OK();
}

Status TestUtils::registerFunction(IquanPtr &pIquan,
                                   const std::string &catalogRootPath,
                                   const std::string &functionName) {
    std::string content;
    std::string filePath = catalogRootPath + "/" + functionName;
    Status status = Utils::readFile(filePath, content);
    if (!status.ok()) {
        AUTIL_LOG(ERROR, "readFile from %s fail", filePath.c_str());
        return Status(IQUAN_FAIL, "fail");
    }
    status = pIquan->updateFunctions(content);
    if (!status.ok()) {
        AUTIL_LOG(ERROR, "updateFunctions for %s fail", filePath.c_str());
        return Status(IQUAN_FAIL, "fail");
    }
    return Status::OK();
}

Status TestUtils::registerFunction(IquanPtr &pIquan,
                                   const std::string &catalogRootPath,
                                   const std::vector<std::string> &functionNames) {
    for (const std::string &name : functionNames) {
        IQUAN_ENSURE_FUNC(registerFunction(pIquan, catalogRootPath, name));
    }
    return Status::OK();
}

Status
TestUtils::getTableModels(const std::string &filePath, int64_t version, TableModels &tableModels) {
    std::string fileContent;
    IQUAN_ENSURE_FUNC(Utils::readFile(filePath, fileContent));

    TableModels tmpTableModels;
    Status status = Utils::fromJson(tmpTableModels, fileContent);
    if (!status.ok()) {
        AUTIL_LOG(ERROR,
                  "fromJson for %s fail, error msg [%s]",
                  filePath.c_str(),
                  status.errorMessage().c_str());
        return Status(IQUAN_FAIL, "fail");
    }
    for (auto &model : tmpTableModels.tables) {
        model.tableVersion = version;
    }
    if (!tmpTableModels.isValid()) {
        AUTIL_LOG(ERROR, "tableModel for %s is not valid", filePath.c_str());
        return Status(IQUAN_FAIL, "fail");
    }

    tableModels.merge(tmpTableModels);
    return Status::OK();
}

Status TestUtils::getFunctionModels(const std::string &filePath,
                                    int64_t version,
                                    FunctionModels &functionModels) {
    std::string fileContent;
    IQUAN_ENSURE_FUNC(Utils::readFile(filePath, fileContent));

    FunctionModels tmpFunctionModels;
    Status status = Utils::fromJson(tmpFunctionModels, fileContent);
    if (!status.ok()) {
        AUTIL_LOG(ERROR, "fromJson for %s fail", filePath.c_str());
        return Status(IQUAN_FAIL, "fail");
    }
    for (auto &model : tmpFunctionModels.functions) {
        model.functionVersion = version;
    }
    if (!tmpFunctionModels.isValid()) {
        AUTIL_LOG(ERROR, "functionModels for %s is not valid", filePath.c_str());
        return Status(IQUAN_FAIL, "fail");
    }

    functionModels.merge(tmpFunctionModels);
    return Status::OK();
}

Status TestUtils::simpleQuery(const IquanPtr &pIquan,
                              const std::string &defaultCatalogName,
                              const std::string &defaultDatabaseName,
                              const std::vector<std::string> &sqls,
                              const autil::legacy::json::JsonMap &sqlParams,
                              const std::vector<std::vector<autil::legacy::Any>> &dynamicParams,
                              IquanDqlResponse &response,
                              PlanCacheStatus &planCacheStatus) {
    IquanDqlRequest request;
    {
        autil::legacy::json::JsonMap newSqlParams = Utils::shallowClone(sqlParams);
        request.defaultCatalogName = defaultCatalogName;
        request.defaultDatabaseName = defaultDatabaseName;
        request.sqls = sqls;
        request.sqlParams = newSqlParams;
        if (!dynamicParams.empty()) {
            request.dynamicParams._array = Utils::shallowClone(dynamicParams);
        }
    }

    IQUAN_ENSURE_FUNC(pIquan->query(request, response, planCacheStatus));
    if (response.errorCode != IQUAN_OK) {
        return Status(response.errorCode, response.errorMsg);
    }
    return Status::OK();
}

} // namespace iquan
