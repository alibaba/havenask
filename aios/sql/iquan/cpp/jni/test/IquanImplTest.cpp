#include "iquan/jni/IquanImpl.h"

#include <thread>

#include "autil/TimeUtility.h"
#include "iquan/common/Utils.h"
#include "iquan/jni/test/testlib/IquanTestBase.h"

using namespace autil::legacy;
using namespace autil::legacy::json;

namespace iquan {

class IquanImplTest : public IquanTestBase {
public:
    static std::shared_ptr<RapidDocument> parseDocument(const std::string &str) {
        std::shared_ptr<autil::legacy::RapidDocument> documentPtr(new autil::legacy::RapidDocument);
        std::string newStr = str + IQUAN_DYNAMIC_PARAMS_SIMD_PADDING;
        documentPtr->Parse(newStr.c_str());
        return documentPtr;
    }
    static std::string formatJsonStr(const std::string &str) {
        autil::legacy::json::JsonMap jsonMap;
        Utils::fromJson(jsonMap, str);
        std::string newStr;
        Utils::toJson(jsonMap, newStr, false);
        return newStr;
    }
};

TEST_F(IquanImplTest, testGetHashKeyStr) {
    IquanDqlRequest request;
    autil::legacy::FastFromJsonString(request.dynamicParams._array, "[[\"a\"]]");
    std::string originStr;
    Utils::toJson(request, originStr, false);
    std::string hashKeyStr;
    ASSERT_TRUE(IquanImpl::getHashKeyStr(request, hashKeyStr).ok());
    std::string expectStr = "{\"sqls\":[],\"sql_params\":{},\"default_catalog_name\":\"\","
                            "\"default_database_name\":\"\"}";
    ASSERT_EQ(expectStr, hashKeyStr);
    ASSERT_NE(originStr, hashKeyStr);
}

} // namespace iquan
