#include "iquan/jni/test/ptest/IquanPerfTestBase.h"

#include <thread>

#include "autil/Thread.h"
#include "autil/TimeUtility.h"
#include "iquan/jni/test/testlib/Counter.h"
#include "iquan/jni/test/testlib/TestUtils.h"

namespace iquan {

Status IquanPerfTestBase::loadCatalogData(const std::string &rootPath, CatalogData &catalogData) {
    std::string catalogRootPath = rootPath + "/catalogs";
    AUTIL_LOG(INFO, "catalogRootPath: %s", catalogRootPath.c_str());

    {
        std::string content;
        std::string filePath = catalogRootPath + "/default_catalog_path.json";
        IQUAN_ENSURE_FUNC(Utils::readFile(filePath, content));
        IQUAN_ENSURE_FUNC(Utils::fromJson(catalogData.defaultCatalogPath, content));
        AUTIL_LOG(INFO, "load default catalog path success: %s", filePath.c_str());
    }

    {
        std::string catalogInfoPath = catalogRootPath + "/catalog_infos.json";
        std::string content;
        IQUAN_ENSURE_FUNC(Utils::readFile(catalogInfoPath, content));
        std::vector<DbCatalogInfo> catalogInfos;
        IQUAN_ENSURE_FUNC(Utils::fromJson(catalogInfos, content));

        for (const auto &catalogInfo : catalogInfos) {
            AUTIL_LOG(INFO, "start to load catalog %s", catalogInfo.catalogName.c_str());
            for (const auto &databaseInfo : catalogInfo.databaseInfos) {
                AUTIL_LOG(INFO,
                          "start to load database %s.%s",
                          catalogInfo.catalogName.c_str(),
                          databaseInfo.databaseName.c_str());

                for (const auto &table : databaseInfo.tables) {
                    catalogData.ha3TableNameList.push_back(table);
                }
                for (const auto &function : databaseInfo.functions) {
                    catalogData.functionNameList.push_back(function);
                }
            }
        }
    }
    return Status::OK();
}

Status IquanPerfTestBase::registerCatalogData(const std::string &rootPath,
                                              const CatalogData &catalogData,
                                              IquanPtr &pIquan) {
    // IQUAN_ENSURE_FUNC(TestUtils::registerHa3Table(pIquan, rootPath, catalogData.ha3TableNameList,
    // catalogData.defaultCatalogPath, autil::TimeUtility::currentTime()));
    IQUAN_ENSURE_FUNC(TestUtils::registerFunction(pIquan, rootPath, catalogData.functionNameList));
    return Status::OK();
}

Status IquanPerfTestBase::loadSqlQueryInfos(const std::string &filePath,
                                            SqlQueryInfos &sqlQueryInfos) {
    std::string content;
    IQUAN_ENSURE_FUNC(Utils::readFile(filePath, content));
    IQUAN_ENSURE_FUNC(Utils::fromJson(sqlQueryInfos, content));
    bool ret = sqlQueryInfos.isValid();
    if (!ret) {
        return Status(IQUAN_FAIL, "fail");
    }
    return Status::OK();
}

Status IquanPerfTestBase::runTest(const DefaultCatalogPath &defaultCatalogPath,
                                  IquanPtr &pIquan,
                                  const RunOptions &runOptions) {
    if (runOptions.numThread <= 0 || runOptions.numCycles <= 0) {
        AUTIL_LOG(ERROR,
                  "%s: run options is not valid, num thread %d, num cycles %d",
                  runOptions.signature.c_str(),
                  runOptions.numThread,
                  runOptions.numCycles);
        return Status(IQUAN_FAIL, "fail");
    }

    SqlQueryInfos sqlList;
    IQUAN_ENSURE_FUNC(loadSqlQueryInfos(runOptions.queryFile, sqlList));

    AUTIL_LOG(INFO, "%s: start run", runOptions.signature.c_str());
    std::vector<autil::ThreadPtr> threads;
    for (int i = 0; i < runOptions.numThread; ++i) {
        autil::ThreadPtr thread = autil::Thread::createThread([&]() {
            IquanPerfTestBase::runSingleThread(defaultCatalogPath, pIquan, runOptions, sqlList);
        });
        threads.push_back(thread);
    }
    for (const auto &thread : threads) {
        thread->join();
    }
    AUTIL_LOG(INFO, "%s: end run", runOptions.signature.c_str());
    return Status::OK();
}

Status IquanPerfTestBase::runSingleThread(const DefaultCatalogPath &defaultCatalogPath,
                                          IquanPtr &pIquan,
                                          const RunOptions &runOptions,
                                          const SqlQueryInfos &sqlQueryInfos) {
    std::hash<std::thread::id> hasher;
    Status status = Status::OK();
    AUTIL_LOG(INFO,
              "%s: start run, thread id %ld",
              runOptions.signature.c_str(),
              hasher(std::this_thread::get_id()));

    for (int64_t i = 0; i < runOptions.numCycles; ++i) {
        Counter counter;
        int64_t cycleStartTime = autil::TimeUtility::currentTime();
        for (const SqlQueryInfo &queryInfo : sqlQueryInfos.querys) {
            //
            std::vector<std::string> sqls = queryInfo.sqls;
            std::vector<std::vector<autil::legacy::Any>> dynamicParams
                = Utils::shallowClone(queryInfo.dynamicParams);

            // sql params
            autil::legacy::json::JsonMap sqlParams = Utils::defaultSqlParams();
            if (!queryInfo.sqlParams.empty()) {
                autil::legacy::json::JsonMap cloneParams = Utils::shallowClone(queryInfo.sqlParams);
                for (auto &kv : cloneParams) {
                    sqlParams[kv.first] = kv.second;
                }
            }
            if (!runOptions.sqlParams.empty()) {
                autil::legacy::json::JsonMap cloneParams
                    = Utils::shallowClone(runOptions.sqlParams);
                for (auto &kv : cloneParams) {
                    sqlParams[kv.first] = kv.second;
                }
            }

            // query
            int64_t queryStartTime = autil::TimeUtility::currentTime();
            IquanDqlResponse response;
            PlanCacheStatus planCacheStatus;
            status = TestUtils::simpleQuery(pIquan,
                                            defaultCatalogPath.catalogName,
                                            defaultCatalogPath.databaseName,
                                            sqls,
                                            sqlParams,
                                            dynamicParams,
                                            response,
                                            planCacheStatus);
            int64_t queryEndTime = autil::TimeUtility::currentTime();
            if (!status.ok()) {
                AUTIL_LOG(ERROR, "query and transform fail, sql is %s", sqls[0].c_str());
                return Status(IQUAN_FAIL, "fail");
            }

            if (runOptions.debug) {
                AUTIL_LOG(INFO,
                          "%s: thread id %ld, %.3lf ms",
                          runOptions.signature.c_str(),
                          hasher(std::this_thread::get_id()),
                          ((queryEndTime - queryStartTime) * 1.0f) / 1000);
            }
        }

        counter.addCount(sqlQueryInfos.querys.size());
        counter.addElapsed(autil::TimeUtility::currentTime() - cycleStartTime);
        if (counter.count() == 0) {
            AUTIL_LOG(INFO,
                      "%s: thread id %ld, outter loop %ld, sqls num %ld",
                      runOptions.signature.c_str(),
                      hasher(std::this_thread::get_id()),
                      i,
                      counter.count());
        } else {
            AUTIL_LOG(INFO,
                      "%s: thread id %ld, outter loop %ld, sqls num %ld, avg rt %.3lf ms",
                      runOptions.signature.c_str(),
                      hasher(std::this_thread::get_id()),
                      i,
                      counter.count(),
                      counter.elapsed() * 1.0f / (counter.count() * 1000));
        }
    }

    AUTIL_LOG(INFO,
              "%s: end run, thread id %ld",
              runOptions.signature.c_str(),
              hasher(std::this_thread::get_id()));
    return Status::OK();
}

} // namespace iquan
