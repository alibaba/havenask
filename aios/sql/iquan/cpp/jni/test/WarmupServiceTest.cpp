#include "iquan/jni/WarmupService.h"

#include <thread>

#include "autil/TimeUtility.h"
#include "iquan/common/Utils.h"
#include "iquan/jni/test/testlib/IquanTestBase.h"
#include "iquan/jni/test/testlib/TestUtils.h"

using namespace autil::legacy;
using namespace autil::legacy::json;

namespace iquan {

class WarmupServiceTest : public IquanTestBase {};

TEST_F(WarmupServiceTest, testReadJsonQuerysError_empty) {
    std::vector<IquanDqlRequest> sqlQueryRequestList;
    WarmupConfig config;
    auto status = WarmupService::readJsonQuerys(config, sqlQueryRequestList);
    ASSERT_FALSE(status.ok());
    ASSERT_TRUE(status.errorMessage().find("warmup query is empty:") != std::string::npos);
}

TEST_F(WarmupServiceTest, testReadJsonQuerys_parseError) {
    std::vector<IquanDqlRequest> sqlQueryRequestList;
    WarmupConfig config;
    config.warmupFilePathList = {GET_PRIVATE_TEST_DATA_PATH_WITHIN_TEST()
                                 + std::string("/warmup/warmup_sqls_error.json")};
    auto status = WarmupService::readJsonQuerys(config, sqlQueryRequestList);
    ASSERT_FALSE(status.ok());
    ASSERT_TRUE(status.errorMessage().find("warmup query is empty:") != std::string::npos);
}

TEST_F(WarmupServiceTest, testReadJsonQuerys) {
    std::vector<IquanDqlRequest> sqlQueryRequestList;
    WarmupConfig config;
    config.warmupFilePathList
        = {GET_PRIVATE_TEST_DATA_PATH_WITHIN_TEST() + std::string("/warmup/warmup_sqls.json"),
           GET_PRIVATE_TEST_DATA_PATH_WITHIN_TEST() + std::string("/warmup/warmup_sqls.json")};
    auto status = WarmupService::readJsonQuerys(config, sqlQueryRequestList);
    ASSERT_TRUE(status.ok()) << status.errorMessage();
    ASSERT_EQ(1, sqlQueryRequestList.size());
}

TEST_F(WarmupServiceTest, testWarmupFailed_readJsonQueryError) {
    WarmupConfig config;
    config.warmupFilePathList = {GET_PRIVATE_TEST_DATA_PATH_WITHIN_TEST()
                                 + std::string("/warmup/warmup_sqls_error.json")};
    auto status = WarmupService::warmup(nullptr, config);
    ASSERT_FALSE(status.ok());
    ASSERT_TRUE(status.errorMessage().find("warmup query is empty:") != std::string::npos);
}

TEST_F(WarmupServiceTest, testWarmupFailed_warmupThreadFailed) {
    WarmupConfig config;
    config.warmupQueryNum = 10;
    config.warmupFilePathList = {GET_PRIVATE_TEST_DATA_PATH_WITHIN_TEST()
                                 + std::string("/warmup/warmup_sqls_query_failed.json")};
    auto iquan = TestUtils::createIquan();
    auto status = WarmupService::warmup(iquan->_impl.get(), config);
    ASSERT_FALSE(status.ok()) << status.errorMessage();
    ASSERT_TRUE(status.errorMessage().find("Warmup fail, query not 100% succeed")
                != std::string::npos);
}

TEST_F(WarmupServiceTest, testWarmup) {
    WarmupConfig config;
    config.warmupQueryNum = 10;
    config.warmupFilePathList
        = {GET_PRIVATE_TEST_DATA_PATH_WITHIN_TEST() + std::string("/warmup/warmup_sqls.json")};
    auto iquan = TestUtils::createIquan();
    std::string catalogPath = GET_PRIVATE_TEST_DATA_PATH_WITHIN_TEST() + std::string("/iquan_catalog/catalogs.json");
    ASSERT_TRUE(TestUtils::registerCatalogs(catalogPath,iquan).ok());
    auto status = WarmupService::warmup(iquan->_impl.get(), config);
    ASSERT_TRUE(status.ok());
}

} // namespace iquan
