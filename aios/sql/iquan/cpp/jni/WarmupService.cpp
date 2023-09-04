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
#include "iquan/jni/WarmupService.h"

#include <thread>
#include <unordered_set>

#include "autil/Thread.h"
#include "autil/TimeUtility.h"
#include "iquan/common/Utils.h"
#include "iquan/jni/IquanDqlRequest.h"
#include "iquan/jni/IquanDqlResponse.h"
#include "iquan/jni/IquanImpl.h"

namespace iquan {

AUTIL_LOG_SETUP(iquan, WarmupService);

Status WarmupService::warmup(IquanImpl *pIquan, const WarmupConfig &config) {
    std::vector<IquanDqlRequest> sqlQueryRequestList;
    IQUAN_ENSURE_FUNC(readJsonQuerys(config, sqlQueryRequestList));
    std::vector<autil::ThreadPtr> threads;
    autil::AtomicCounter successCnt;

    for (int i = 0; i < config.threadNum; ++i) {
        autil::ThreadPtr thread = autil::Thread::createThread(
            [&, i]() { warmupSingleThread(pIquan, config, i, sqlQueryRequestList, successCnt); });
        threads.push_back(thread);
    }
    for (const auto &thread : threads) {
        thread->join();
    }
    if (successCnt.getValue() != config.threadNum) {
        return Status(IQUAN_FAIL, "Warmup fail, query not 100% succeed");
    }
    return Status::OK();
}

void WarmupService::warmupSingleThread(IquanImpl *pIquan,
                                       const WarmupConfig &config,
                                       int threadId,
                                       std::vector<IquanDqlRequest> &sqlQueryRequestList,
                                       autil::AtomicCounter &successCnt) {
    std::hash<std::thread::id> hasher;
    int64_t sqlNum = sqlQueryRequestList.size();
    if (sqlNum <= 0) {
        AUTIL_LOG(
            WARN, "Warmup query is empty, thread %ld exit now", hasher(std::this_thread::get_id()));
        return;
    }

    Status status;
    int64_t loopStartTime = autil::TimeUtility::currentTime();
    int64_t finishTime = loopStartTime + config.warmupSeconds * 1000000;

    int64_t failNum = 0;
    int64_t warmupNum = 0;
    for (int64_t loopNum = 0;; ++loopNum) {
        if (autil::TimeUtility::currentTime() > finishTime) {
            break;
        }
        if (loopNum * sqlNum > config.warmupQueryNum) {
            break;
        }

        // request need thread safe
        size_t i = 0;
        for (IquanDqlRequest request : sqlQueryRequestList) {
            if (autil::TimeUtility::currentTime() > finishTime) {
                break;
            }
            if (i++ % config.threadNum != threadId) {
                continue;
            }
            warmupQuery(pIquan, request, warmupNum, failNum);
            request.sqlParams[IQUAN_PLAN_CACHE_ENALE] = std::string("false");
            warmupQuery(pIquan, request, warmupNum, failNum);
        }
    }

    AUTIL_LOG(INFO,
              "Warmup running: thread id %ld, warmup num %ld, avg time %lf us",
              hasher(std::this_thread::get_id()),
              warmupNum,
              (autil::TimeUtility::currentTime() - loopStartTime) * 1.0f / warmupNum);

    AUTIL_LOG(INFO,
              "thread %ld : warmup query success ratio is [%lu/%lu], ",
              hasher(std::this_thread::get_id()),
              warmupNum - failNum,
              warmupNum);
    if (failNum > 0) {
        AUTIL_LOG(WARN, "Warmup fail, thread %ld exit now", hasher(std::this_thread::get_id()));
    } else {
        successCnt.inc();
        AUTIL_LOG(WARN, "Warmup finish, thread %ld exit now", hasher(std::this_thread::get_id()));
    }
}

bool WarmupService::warmupQuery(IquanImpl *pIquan,
                                IquanDqlRequest &request,
                                int64_t &warmupNum,
                                int64_t &failNum) {
    ++warmupNum;
    IquanDqlResponse response;
    PlanCacheStatus planCacheStatus;
    Status status = pIquan->query(request, response, planCacheStatus);
    if (!status.ok() || response.errorCode != IQUAN_OK) {
        AUTIL_LOG(WARN,
                  "Warmup fail: error code [%d], error message [%s], sql is "
                  "[%s], sql params str is [%s]",
                  status.code(),
                  status.errorMessage().c_str(),
                  request.sqls[0].c_str(),
                  autil::legacy::FastToJsonString(request.sqlParams, true).c_str());
        ++failNum;
        return false;
    }
    return true;
}

Status WarmupService::readJsonQuerys(const WarmupConfig &config,
                                     std::vector<IquanDqlRequest> &sqlQueryRequestList) {
    const auto &warmupFilePathList = config.warmupFilePathList;

    AUTIL_LOG(
        INFO, "use warmup file : %s", autil::StringUtil::toString(warmupFilePathList).c_str());
    std::vector<std::string> fileContentList;
    IQUAN_ENSURE_FUNC(Utils::readFiles(warmupFilePathList, fileContentList, true, true));

    std::unordered_set<size_t> querySet;
    for (const std::string &fileContent : fileContentList) {
        IquanDqlRequest request;
        Status status = request.fromWarmupJson(fileContent);
        if (unlikely(!status.ok())) {
            AUTIL_LOG(WARN,
                      "parse warmup query fail: %s, path: %s; error msg: %s",
                      fileContent.c_str(),
                      autil::StringUtil::toString(warmupFilePathList).c_str(),
                      status.errorMessage().c_str());
            continue;
        }
        if (request.queryHashKey == 0 || querySet.count(request.queryHashKey) == 0) {
            sqlQueryRequestList.emplace_back(request);
            querySet.insert(request.queryHashKey);
        }
    }
    AUTIL_LOG(WARN, "warmup query num is : %lu", sqlQueryRequestList.size());
    if (unlikely(sqlQueryRequestList.empty())) {
        return Status(IQUAN_FAIL,
                      "warmup query is empty: " + autil::StringUtil::toString(warmupFilePathList));
    }
    return Status::OK();
}

} // namespace iquan
