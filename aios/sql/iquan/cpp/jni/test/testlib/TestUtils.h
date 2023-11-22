#pragma once

#include <map>
#include <string>
#include <vector>

#include "autil/Log.h"
#include "autil/legacy/jsonizable.h"
#include "iquan/common/Common.h"
#include "iquan/common/Status.h"
#include "iquan/config/ClientConfig.h"
#include "iquan/config/JniConfig.h"
#include "iquan/jni/Iquan.h"
#include "iquan/jni/IquanDqlResponse.h"
#include "iquan/jni/test/testlib/SqlSuiteInfo.h"

namespace iquan {
class CatalogDefs;

class TestUtils {
public:
    static std::shared_ptr<autil::legacy::RapidDocument> parseDocument(const std::string &content);
    static Status formatJsonStr(const std::string &content, std::string &newContent);

    static IquanPtr createIquan();
    static IquanPtr createIquan(const JniConfig &jniConfig, const ClientConfig &sqlConfig);
    static Status registerCatalogs(const CatalogDefs &catalogDefs, IquanPtr &iquan);
    static Status registerCatalogs(const std::string &filaPath, IquanPtr &iquan);
    static Status simpleQuery(const IquanPtr &pIquan,
                              const std::string &defaultCatalogName,
                              const std::string &defaultDatabaseName,
                              const std::vector<std::string> &sqls,
                              const autil::legacy::json::JsonMap &sqlParams,
                              const std::vector<std::vector<autil::legacy::Any>> &dynamicParams,
                              IquanDqlResponse &response,
                              PlanCacheStatus &planCacheStatus);

private:
    AUTIL_LOG_DECLARE();
};

} // namespace iquan
