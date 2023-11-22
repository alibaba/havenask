#include <string>
#include <thread>
#include <vector>

#include "autil/Thread.h"
#include "autil/TimeUtility.h"
#include "iquan/common/Status.h"
#include "iquan/common/Utils.h"
#include "iquan/common/catalog/CatalogDef.h"
#include "iquan/jni/IquanDqlResponse.h"
#include "iquan/jni/test/testlib/Counter.h"
#include "iquan/jni/test/testlib/IquanTestBase.h"
#include "iquan/jni/test/testlib/SqlSuiteInfo.h"
#include "iquan/jni/test/testlib/TestUtils.h"

namespace iquan {

struct CaseInfo {
    std::string path;
    std::string name;
    SqlQueryInfo query;
    SqlPlanInfo plan;
};

struct SuiteInfo {
    std::string name;
    CatalogDefs catalogDefs;
    DefaultCatalogPath defaultCatalogPath;
    std::vector<CaseInfo> caseInfos;
};

class ItTest : public IquanTestBase {
public:
    static void loadSuiteInfos(const std::string &sqlSuitesRootPath,
                               std::vector<SuiteInfo> &suiteInfos);
    static void runSuites(const std::vector<SuiteInfo> &suiteInfos);

private:
    static void loadDefaultCatalogPath(const std::string &rootPath,
                                       DefaultCatalogPath &defaultCatalogPath);
    static void loadCatalogDefs(const std::string &catalogDefPath, CatalogDefs &catalogDefs);
    static void loadCaseInfos(const std::string &rootPath, std::vector<CaseInfo> &caseInfos);

    static void runCheck(const std::vector<SuiteInfo> &suiteInfos);
    static void runSingleThread(const std::vector<SuiteInfo> &suiteInfos,
                                size_t outLoopNum,
                                size_t innerLoopNum);
    static void runMultipleThread(const std::vector<SuiteInfo> &suiteInfos,
                                  size_t threadNum,
                                  size_t outLoopNum,
                                  size_t innerLoopNum);
    static void runSuite(const SuiteInfo &suiteInfo, size_t loopNum, Counter &counter);
    static void runQuery(const IquanPtr &pIquan,
                         const DefaultCatalogPath &defaultCatalogPath,
                         const autil::legacy::json::JsonMap &sqlParams,
                         const CaseInfo &caseInfo);
    static void runQueryWithCheck(const IquanPtr &pIquan,
                                  const DefaultCatalogPath &defaultCatalogPath,
                                  const CaseInfo &caseInfo);
    static IquanPtr createIquan(const SuiteInfo &suiteInfo);
};

TEST_F(ItTest, testSqlSuiteCases) {
    std::string sqlSuitesRootPath = GET_WORKSPACE_PATH() + "/integration_test/sql/sql-suites";
    if (!Utils::isExist(sqlSuitesRootPath)) {
        sqlSuitesRootPath = TEST_ROOT_PATH() + "../../integration_test/sql/sql-suites";
        ASSERT_TRUE(Utils::isExist(sqlSuitesRootPath)) << sqlSuitesRootPath + " is not exist";
    }
    std::vector<SuiteInfo> suiteInfos;
    ASSERT_NO_FATAL_FAILURE(loadSuiteInfos(sqlSuitesRootPath, suiteInfos));
    ASSERT_NO_FATAL_FAILURE(runSuites(suiteInfos));
}

void ItTest::loadSuiteInfos(const std::string &sqlSuitesRootPath,
                            std::vector<SuiteInfo> &suiteInfos) {
    std::string suitesRootPath = sqlSuitesRootPath + "/suites";
    std::string plansRootPath = sqlSuitesRootPath + "/plans";
    AUTIL_LOG(INFO, "suitesRootPath: %s", suitesRootPath.c_str());
    AUTIL_LOG(INFO, "plansRootPath: %s", plansRootPath.c_str());

    std::vector<std::string> suiteNames;
    Status status = Utils::listDir(suitesRootPath, suiteNames, false);
    ASSERT_TRUE(status.ok()) << status.errorMessage();

    for (const auto &suiteName : suiteNames) {
        if (!Utils::isDir(suitesRootPath + "/" + suiteName)) {
            AUTIL_LOG(ERROR,
                      "suite %s in %s is not dir, skip",
                      suiteName.c_str(),
                      suitesRootPath.c_str());
            continue;
        }
        if (!Utils::isDir(plansRootPath + "/" + suiteName)) {
            AUTIL_LOG(
                ERROR, "suite %s in %s is not dir, skip", suiteName.c_str(), plansRootPath.c_str());
            continue;
        }

        SuiteInfo suiteInfo;
        suiteInfo.name = suiteName;
        AUTIL_LOG(INFO, "==== start to load suite %s", suiteName.c_str());

        loadCatalogDefs(suitesRootPath + "/" + suiteName + "/catalogs/catalog_def.json",
                        suiteInfo.catalogDefs);
        ASSERT_TRUE(suiteInfo.catalogDefs.isValid());

        loadDefaultCatalogPath(suitesRootPath + "/" + suiteName
                                   + "/catalogs/default_catalog_path.json",
                               suiteInfo.defaultCatalogPath);
        ASSERT_TRUE(suiteInfo.defaultCatalogPath.isValid());
        AUTIL_LOG(INFO, "load suite %s default catalog path success", suiteName.c_str());

        loadCaseInfos(plansRootPath + "/" + suiteName, suiteInfo.caseInfos);
        ASSERT_TRUE(!suiteInfo.caseInfos.empty());
        AUTIL_LOG(INFO,
                  "load suite %s case infos success, total %lu cases",
                  suiteName.c_str(),
                  suiteInfo.caseInfos.size());

        suiteInfos.emplace_back(suiteInfo);
    }
    AUTIL_LOG(INFO, "load suite infos finish");
}

void ItTest::loadDefaultCatalogPath(const std::string &rootPath,
                                    DefaultCatalogPath &defaultCatalogPath) {
    std::string content;
    Status status = Utils::readFile(rootPath, content);
    ASSERT_TRUE(status.ok()) << status.errorMessage();
    ASSERT_TRUE(!content.empty());
    status = Utils::fromJson(defaultCatalogPath, content);
    ASSERT_TRUE(status.ok()) << status.errorMessage();
}

void ItTest::loadCatalogDefs(const std::string &catalogDefPath, CatalogDefs &catalogDefs) {
    ASSERT_TRUE(Utils::isExist(catalogDefPath));
    std::string catalogDefsContent;
    Status status = Utils::readFile(catalogDefPath, catalogDefsContent);
    ASSERT_TRUE(status.ok()) << status.errorMessage();
    status = Utils::fromJson(catalogDefs, catalogDefsContent);
    ASSERT_TRUE(status.ok()) << status.errorMessage();
}

void ItTest::loadCaseInfos(const std::string &rootPath, std::vector<CaseInfo> &caseInfos) {
    std::vector<std::string> caseNames;
    Status status = Utils::listDir(rootPath, caseNames, false);
    ASSERT_TRUE(status.ok()) << status.errorMessage();

    for (const auto &caseName : caseNames) {
        if (!autil::StringUtil::endsWith(caseName, ".json")) {
            AUTIL_LOG(INFO, "load case infos, skip %s", caseName.c_str());
            continue;
        }
        std::string content;
        std::string path = rootPath + "/" + caseName;
        status = Utils::readFile(path, content);
        ASSERT_TRUE(status.ok()) << status.errorMessage();
        ASSERT_TRUE(!content.empty());

        CaseInfo caseInfo;
        caseInfo.path = path;
        caseInfo.name = caseName.substr(0, caseName.size() - 4);

        status = Utils::fromJson(caseInfo.query, content);
        ASSERT_TRUE(status.ok()) << status.errorMessage();
        ASSERT_TRUE(caseInfo.query.isValid());

        std::shared_ptr<autil::legacy::RapidDocument> documentPtr
            = TestUtils::parseDocument(content);
        ASSERT_TRUE(documentPtr != nullptr);
        status = Utils::fromRapidValue(caseInfo.plan, documentPtr);
        ASSERT_TRUE(status.ok()) << status.errorMessage();
        caseInfo.plan.documentPtr = documentPtr;

        AUTIL_LOG(INFO, "load case info %s success", caseName.c_str());
        caseInfos.emplace_back(caseInfo);
    }
}

void ItTest::runSuites(const std::vector<SuiteInfo> &suiteInfos) {
    for (int i = 0; i < 2; ++i) {
        ASSERT_NO_FATAL_FAILURE(runCheck(suiteInfos));
    }

#ifdef RUN_ITEST_FOREVER
    while (true) {
#endif
        ASSERT_NO_FATAL_FAILURE(
            runSingleThread(suiteInfos, 2 /* out loops */, 3 /* inner loops */));
        ASSERT_NO_FATAL_FAILURE(runMultipleThread(
            suiteInfos, 4 /* thread num */, 2 /* out loops */, 3 /* inner loops */));
#ifdef RUN_ITEST_FOREVER
    }
#endif
}

void ItTest::runCheck(const std::vector<SuiteInfo> &suiteInfos) {
    AUTIL_LOG(INFO, "==== start to check suite infos");
    for (const SuiteInfo &suiteInfo : suiteInfos) {
        AUTIL_LOG(INFO, "==== start to check suite: %s", suiteInfo.name.c_str());
        IquanPtr pIquan = createIquan(suiteInfo);
        ASSERT_TRUE(pIquan != nullptr);

        for (const CaseInfo &caseInfo : suiteInfo.caseInfos) {
            AUTIL_LOG(INFO, "check case: %s", caseInfo.name.c_str());
            ASSERT_NO_FATAL_FAILURE(
                runQueryWithCheck(pIquan, suiteInfo.defaultCatalogPath, caseInfo))
                << "CASE PATH: " << caseInfo.path;
        }
    }
    AUTIL_LOG(INFO, "==== finish to check suite infos");
}

void ItTest::runSingleThread(const std::vector<SuiteInfo> &suiteInfos,
                             size_t outLoopNum,
                             size_t innerLoopNum) {
    std::hash<std::thread::id> hasher;
    for (size_t i = 0; i < outLoopNum; ++i) {
        Counter counter;
        for (const auto &suiteInfo : suiteInfos) {
            ASSERT_NO_FATAL_FAILURE(runSuite(suiteInfo, innerLoopNum, counter));
        }
        if (counter.count() == 0) {
            AUTIL_LOG(INFO,
                      "thread id %ld, outter round %ld, sqls num %ld",
                      hasher(std::this_thread::get_id()),
                      i,
                      counter.count());
        } else {
            AUTIL_LOG(INFO,
                      "thread id %ld, outter round %ld, sqls num %ld, avg rt %.2lfus",
                      hasher(std::this_thread::get_id()),
                      i,
                      counter.count(),
                      counter.elapsed() * 1.0f / counter.count());
        }
    }
}

void ItTest::runMultipleThread(const std::vector<SuiteInfo> &suiteInfos,
                               size_t threadNum,
                               size_t outLoopNum,
                               size_t innerLoopNum) {
    std::vector<autil::ThreadPtr> threads;
    for (size_t i = 0; i < threadNum; ++i) {
        autil::ThreadPtr thread = autil::Thread::createThread([&]() {
            ASSERT_NO_FATAL_FAILURE(ItTest::runSingleThread(suiteInfos, outLoopNum, innerLoopNum));
        });
        threads.push_back(thread);
    }
    for (const auto &thread : threads) {
        thread->join();
    }
}

void ItTest::runSuite(const SuiteInfo &suiteInfo, size_t loopNum, Counter &counter) {
    IquanPtr pIquan = createIquan(suiteInfo);
    ASSERT_TRUE(pIquan != nullptr);

    int64_t totalSqlNum = 0;
    int64_t totalUsedTime = 0;
    int64_t sqlNum = loopNum * suiteInfo.caseInfos.size();
    ASSERT_TRUE(sqlNum > 0);

    // original query and sql params
    {
        int64_t startTime1 = autil::TimeUtility::currentTime();
        for (size_t i = 0; i < loopNum; ++i) {
            for (const auto &caseInfo : suiteInfo.caseInfos) {
                autil::legacy::json::JsonMap newSqlParams
                    = Utils::shallowClone(caseInfo.query.sqlParams);
                runQuery(pIquan, suiteInfo.defaultCatalogPath, newSqlParams, caseInfo);
            }
        }
        int64_t endTime1 = autil::TimeUtility::currentTime();
        totalSqlNum += sqlNum;
        totalUsedTime += endTime1 - startTime1;
        AUTIL_LOG(INFO,
                  "original query and sql params: suite %s, sqls num %ld, avg rt %.2lf us",
                  suiteInfo.name.c_str(),
                  sqlNum,
                  (endTime1 - startTime1) * 1.0f / sqlNum);
    }

    // enable cache and set prepareLevel = jni
    {
        int64_t startTime1 = autil::TimeUtility::currentTime();
        for (size_t i = 0; i < loopNum; ++i) {
            for (const auto &caseInfo : suiteInfo.caseInfos) {
                autil::legacy::json::JsonMap newSqlParams
                    = Utils::shallowClone(caseInfo.query.sqlParams);
                newSqlParams[IQUAN_PLAN_CACHE_ENALE] = std::string("true");
                newSqlParams[IQUAN_PLAN_PREPARE_LEVEL] = std::string(IQUAN_JNI_POST_OPTIMIZE);
                runQuery(pIquan, suiteInfo.defaultCatalogPath, newSqlParams, caseInfo);
            }
        }
        int64_t endTime1 = autil::TimeUtility::currentTime();
        totalSqlNum += sqlNum;
        totalUsedTime += endTime1 - startTime1;
        AUTIL_LOG(INFO,
                  "cache=enable|prepareLevel=jni: suite %s, sqls num %ld, avg rt %.2lf us",
                  suiteInfo.name.c_str(),
                  sqlNum,
                  (endTime1 - startTime1) * 1.0f / sqlNum);
    }

    // diable cache and set prepareLevel = jni
    {
        int64_t startTime1 = autil::TimeUtility::currentTime();
        for (size_t i = 0; i < loopNum; ++i) {
            for (const auto &caseInfo : suiteInfo.caseInfos) {
                autil::legacy::json::JsonMap newSqlParams
                    = Utils::shallowClone(caseInfo.query.sqlParams);
                newSqlParams[IQUAN_PLAN_CACHE_ENALE] = std::string("false");
                newSqlParams[IQUAN_PLAN_PREPARE_LEVEL] = std::string(IQUAN_JNI_POST_OPTIMIZE);
                runQuery(pIquan, suiteInfo.defaultCatalogPath, newSqlParams, caseInfo);
            }
        }
        int64_t endTime1 = autil::TimeUtility::currentTime();
        totalSqlNum += sqlNum;
        totalUsedTime += endTime1 - startTime1;
        AUTIL_LOG(INFO,
                  "cache=false|prepareLevel=jni: suite %s, sqls num %ld, avg rt %.2lf us",
                  suiteInfo.name.c_str(),
                  sqlNum,
                  (endTime1 - startTime1) * 1.0f / sqlNum);
    }

    counter.addCount(totalSqlNum);
    counter.addElapsed(totalUsedTime);
}

void ItTest::runQuery(const IquanPtr &pIquan,
                      const DefaultCatalogPath &defaultCatalogPath,
                      const autil::legacy::json::JsonMap &sqlParams,
                      const CaseInfo &caseInfo) {
    IquanDqlResponse response;
    PlanCacheStatus planCacheStatus;
    Status status = TestUtils::simpleQuery(pIquan,
                                           defaultCatalogPath.catalogName,
                                           defaultCatalogPath.databaseName,
                                           caseInfo.query.sqls,
                                           sqlParams,
                                           caseInfo.query.dynamicParams,
                                           response,
                                           planCacheStatus);
    if (!status.ok()) {
        AUTIL_LOG(ERROR, "query fail, sql is %s", caseInfo.query.sqls[0].c_str());
    }
    ASSERT_TRUE(status.ok()) << status.errorMessage();
}

void ItTest::runQueryWithCheck(const IquanPtr &pIquan,
                               const DefaultCatalogPath &defaultCatalogPath,
                               const CaseInfo &caseInfo) {
    // check query
    IquanDqlResponse response;
    PlanCacheStatus planCacheStatus;
    Status status = TestUtils::simpleQuery(pIquan,
                                           defaultCatalogPath.catalogName,
                                           defaultCatalogPath.databaseName,
                                           caseInfo.query.sqls,
                                           caseInfo.query.sqlParams,
                                           caseInfo.query.dynamicParams,
                                           response,
                                           planCacheStatus);
    if (!status.ok()) {
        AUTIL_LOG(ERROR, "query fail, sql is %s", caseInfo.query.sqls[0].c_str());
    }
    ASSERT_TRUE(status.ok()) << status.errorMessage();

    // check plan
    SqlPlanWrapper expectPlanWrapper;
    DynamicParams dynamicParams;
    dynamicParams._array = caseInfo.query.dynamicParams;
    expectPlanWrapper.convert(caseInfo.plan.plan, dynamicParams);

    SqlPlanWrapper planWrapper;
    response.sqlPlan.execParams.clear();
    planWrapper.convert(response.sqlPlan, dynamicParams);

    std::string expectStr;
    std::string actualStr;
    status = Utils::toJson(expectPlanWrapper, expectStr);
    ASSERT_TRUE(status.ok()) << status.errorMessage();
    status = Utils::toJson(planWrapper, actualStr);
    ASSERT_TRUE(status.ok()) << status.errorMessage();

    ASSERT_EQ(expectStr, actualStr)
        << "assert plan fail, sql is `" << caseInfo.query.sqls[0] << "`";
}

IquanPtr ItTest::createIquan(const SuiteInfo &suiteInfo) {
    IquanPtr pIquan = TestUtils::createIquan();
    if (pIquan == nullptr) {
        AUTIL_LOG(INFO, "pIquan create error");
        return pIquan;
    }
    Status status = pIquan->registerCatalogs(suiteInfo.catalogDefs);
    if (!status.ok()) {
        AUTIL_LOG(INFO, "register catalogdefs failed: %s", status.errorMessage().c_str());
        return IquanPtr();
    }
    return pIquan;
}

} // namespace iquan
