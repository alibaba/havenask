#include "iquan/jni/test/testlib/TestUtils.h"

#include "iquan/common/Common.h"
#include "iquan/common/Utils.h"
#include "iquan/common/catalog/CatalogDef.h"
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

Status TestUtils::registerCatalogs(const std::string &filePath, IquanPtr &pIquan) {
    std::string catalogsContent;
    IQUAN_ENSURE_FUNC(Utils::readFile(filePath, catalogsContent));
    CatalogDefs catalogDefs;
    Status status = Utils::fromJson(catalogDefs, catalogsContent);
    if (!status.ok()) {
        return status;
    }
    return registerCatalogs(catalogDefs, pIquan);
}

Status TestUtils::registerCatalogs(const CatalogDefs &catalogDefs, IquanPtr &pIquan) {
    if (!catalogDefs.isValid()) {
        return Status(-1, "catalog is not valid");
    }
    return pIquan->registerCatalogs(catalogDefs);
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
