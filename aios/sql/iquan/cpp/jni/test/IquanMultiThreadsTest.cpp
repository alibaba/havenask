#include "autil/Thread.h"
#include "autil/TimeUtility.h"
#include "iquan/common/Status.h"
#include "iquan/jni/test/testlib/IquanTestBase.h"
#include "iquan/jni/test/testlib/TestUtils.h"

namespace iquan {

class IquanMultiThreadsTest : public IquanTestBase {
public:
    static void registerCatalogs(IquanPtr pIquan, const std::string &catalogPath);
    static void query(IquanPtr pIquan);
    static void createAndQueryTest(const std::string &catalogRootPath, size_t loopNum);
};

void IquanMultiThreadsTest::registerCatalogs(IquanPtr pIquan, const std::string &catalogPath) {
    Status status = TestUtils::registerCatalogs(catalogPath, pIquan);
    if (!status.ok()) {
        AUTIL_LOG(ERROR,
                  "register catalogs fail, error code[%d], error message[%s]",
                  status.code(),
                  status.errorMessage().c_str());
    }
    ASSERT_TRUE(status.ok()) << status.errorMessage();
}

void IquanMultiThreadsTest::query(IquanPtr pIquan) {
    std::string catalogName = "default";
    std::string databaseName = "db1";
    std::string sql = "select * from t1";

    autil::legacy::json::JsonMap sqlParams = Utils::defaultSqlParams();
    IquanDqlResponse response;
    PlanCacheStatus planCacheStatus;
    Status status = TestUtils::simpleQuery(
        pIquan, catalogName, databaseName, {sql}, sqlParams, {}, response, planCacheStatus);
    if (!status.ok()) {
        AUTIL_LOG(
            ERROR,
            "query fail, catalog[%s], database[%s], sql[%s], error code[%d], error message[%s]",
            catalogName.c_str(),
            databaseName.c_str(),
            sql.c_str(),
            status.code(),
            status.errorMessage().c_str());
    }
    ASSERT_TRUE(status.ok()) << status.errorMessage();
}

void IquanMultiThreadsTest::createAndQueryTest(const std::string &catalogPath, size_t loopNum) {
    for (size_t i = 0; i < loopNum; ++i) {
        IquanPtr pIquan = TestUtils::createIquan();
        ASSERT_TRUE(pIquan != nullptr);
        registerCatalogs(pIquan, catalogPath);
        query(pIquan);
    }
}

TEST_F(IquanMultiThreadsTest, testCreateAndQuery) {
    std::string catalogPath = GET_PRIVATE_TEST_DATA_PATH_WITHIN_TEST() + std::string("/iquan_catalog/catalogs.json");

    size_t loopNum = 20;

    autil::ThreadPtr t1 =
        autil::Thread::createThread([&]() { IquanMultiThreadsTest::createAndQueryTest(catalogPath, loopNum); });
    autil::ThreadPtr t2 =
        autil::Thread::createThread([&]() { IquanMultiThreadsTest::createAndQueryTest(catalogPath, loopNum); });
    autil::ThreadPtr t3 =
        autil::Thread::createThread([&]() { IquanMultiThreadsTest::createAndQueryTest(catalogPath, loopNum); });
    autil::ThreadPtr t4 =
        autil::Thread::createThread([&]() { IquanMultiThreadsTest::createAndQueryTest(catalogPath, loopNum); });

    t1->join();
    t2->join();
    t3->join();
    t4->join();
}

TEST_F(IquanMultiThreadsTest, testMultiThreadQueryAndUpdate) {
    std::string catalogPath = GET_PRIVATE_TEST_DATA_PATH_WITHIN_TEST() + std::string("/iquan_catalog/catalogs.json");
    IquanPtr pIquan = TestUtils::createIquan();
    ASSERT_TRUE(pIquan != nullptr);

    registerCatalogs(pIquan, catalogPath);

    size_t loopNum = 1000;
    autil::ThreadPtr queryThread1 = autil::Thread::createThread([&]() {
        for (size_t i = 0; i < loopNum; ++i) {
            IquanMultiThreadsTest::query(pIquan);
        }
    });
    autil::ThreadPtr queryThread2 = autil::Thread::createThread([&]() {
        for (size_t i = 0; i < loopNum; ++i) {
            IquanMultiThreadsTest::query(pIquan);
        }
    });
    autil::ThreadPtr queryThread3 = autil::Thread::createThread([&]() {
        for (size_t i = 0; i < loopNum; ++i) {
            IquanMultiThreadsTest::query(pIquan);
        }
    });

    queryThread1->join();
    queryThread2->join();
    queryThread3->join();
}

} // namespace iquan
