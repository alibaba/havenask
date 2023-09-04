#include "autil/Thread.h"
#include "autil/TimeUtility.h"
#include "iquan/common/Status.h"
#include "iquan/jni/test/testlib/IquanTestBase.h"
#include "iquan/jni/test/testlib/TestUtils.h"

namespace iquan {

class IquanMultiThreadsTest : public IquanTestBase {
public:
    static void
    updateCatalog(IquanPtr pIquan, TableModels &tableModels, FunctionModels &functionModels);
    static void query(IquanPtr pIquan);
    static void createAndQueryTest(const std::string &catalogRootPath, size_t loopNum);

private:
    static void getCatalog(const std::string &catalogRootPath,
                           TableModels &tableModels,
                           FunctionModels &functionModels);
};

void IquanMultiThreadsTest::updateCatalog(IquanPtr pIquan,
                                          TableModels &tableModels,
                                          FunctionModels &functionModels) {
    for (auto &model : tableModels.tables) {
        model.tableVersion += 1;
    }
    Status status = pIquan->updateTables(tableModels);
    if (!status.ok()) {
        AUTIL_LOG(ERROR,
                  "updateTables fail, error code[%d], error message[%s]",
                  status.code(),
                  status.errorMessage().c_str());
    }
    ASSERT_TRUE(status.ok()) << status.errorMessage();

    for (auto &model : functionModels.functions) {
        model.functionVersion += 1;
    }
    status = pIquan->updateFunctions(functionModels);
    if (!status.ok()) {
        AUTIL_LOG(ERROR,
                  "updateFunctions fail, error code[%d], error message[%s]",
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

void IquanMultiThreadsTest::createAndQueryTest(const std::string &catalogRootPath, size_t loopNum) {
    TableModels tableModels;
    FunctionModels functionModels;
    getCatalog(catalogRootPath, tableModels, functionModels);

    for (size_t i = 0; i < loopNum; ++i) {
        IquanPtr pIquan = TestUtils::createIquan();
        ASSERT_TRUE(pIquan != nullptr);
        updateCatalog(pIquan, tableModels, functionModels);
        query(pIquan);
    }
}

void IquanMultiThreadsTest::getCatalog(const std::string &catalogRootPath,
                                       TableModels &tableModels,
                                       FunctionModels &functionModels) {
    {
        Status status = TestUtils::getTableModels(
            catalogRootPath + "/t1.json", autil::TimeUtility::currentTime(), tableModels);
        ASSERT_TRUE(status.ok()) << status.errorMessage();
        status = TestUtils::getTableModels(
            catalogRootPath + "/t2.json", autil::TimeUtility::currentTime(), tableModels);
        ASSERT_TRUE(status.ok()) << status.errorMessage();
    }
    {
        Status status = TestUtils::getFunctionModels(
            catalogRootPath + "/udf.json", autil::TimeUtility::currentTime(), functionModels);
        ASSERT_TRUE(status.ok()) << status.errorMessage();
        status = TestUtils::getFunctionModels(
            catalogRootPath + "/udaf.json", autil::TimeUtility::currentTime(), functionModels);
        ASSERT_TRUE(status.ok()) << status.errorMessage();
        status = TestUtils::getFunctionModels(
            catalogRootPath + "/udtf.json", autil::TimeUtility::currentTime(), functionModels);
        ASSERT_TRUE(status.ok()) << status.errorMessage();
    }
}

TEST_F(IquanMultiThreadsTest, testCreateAndQuery) {
    std::string catalogRootPath
        = GET_PRIVATE_TEST_DATA_PATH_WITHIN_TEST() + std::string("/iquan_catalog");

    size_t loopNum = 20;

    autil::ThreadPtr t1 = autil::Thread::createThread(
        [&]() { IquanMultiThreadsTest::createAndQueryTest(catalogRootPath, loopNum); });
    autil::ThreadPtr t2 = autil::Thread::createThread(
        [&]() { IquanMultiThreadsTest::createAndQueryTest(catalogRootPath, loopNum); });
    autil::ThreadPtr t3 = autil::Thread::createThread(
        [&]() { IquanMultiThreadsTest::createAndQueryTest(catalogRootPath, loopNum); });
    autil::ThreadPtr t4 = autil::Thread::createThread(
        [&]() { IquanMultiThreadsTest::createAndQueryTest(catalogRootPath, loopNum); });

    t1->join();
    t2->join();
    t3->join();
    t4->join();
}

TEST_F(IquanMultiThreadsTest, testMultiThreadQueryAndUpdate) {
    std::string catalogRootPath
        = GET_PRIVATE_TEST_DATA_PATH_WITHIN_TEST() + std::string("/iquan_catalog");
    IquanPtr pIquan = TestUtils::createIquan();
    ASSERT_TRUE(pIquan != nullptr);

    TableModels tableModels;
    FunctionModels functionModels;
    getCatalog(catalogRootPath, tableModels, functionModels);
    updateCatalog(pIquan, tableModels, functionModels);

    size_t loopNum = 1000;

    autil::ThreadPtr updateThread = autil::Thread::createThread([&]() {
        for (size_t i = 0; i < loopNum; ++i) {
            IquanMultiThreadsTest::updateCatalog(pIquan, tableModels, functionModels);
        }
    });
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

    updateThread->join();
    queryThread1->join();
    queryThread2->join();
    queryThread3->join();
}

} // namespace iquan
