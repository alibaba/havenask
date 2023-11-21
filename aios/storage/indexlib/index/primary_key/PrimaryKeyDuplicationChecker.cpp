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
#include "indexlib/index/primary_key/PrimaryKeyDuplicationChecker.h"

#include "autil/WorkItem.h"

namespace indexlib::index {
AUTIL_LOG_SETUP(indexlib.index, PrimaryKeyDuplicationChecker);

PrimaryKeyDuplicationChecker::PrimaryKeyDuplicationChecker(const PrimaryKeyIndexReader* pkReader)
    : _pkReader(pkReader)
    , _threadPool(std::make_unique<autil::ThreadPool>(12 /*thread_num */, autil::ThreadPool::DEFAULT_QUEUESIZE,
                                                      true /*stopIfHasException*/, "CheckDuplication"))
{
}

PrimaryKeyDuplicationChecker::~PrimaryKeyDuplicationChecker() {}

bool PrimaryKeyDuplicationChecker::Start()
{
    if (!_threadPool->start("CheckDuplication")) {
        AUTIL_LOG(ERROR, "start thread pool failed");
        return false;
    }
    return true;
}

class PkCheckWorkItem : public autil::WorkItem
{
public:
    PkCheckWorkItem(const PrimaryKeyIndexReader* pkReader, std::vector<std::pair<autil::uint128_t, docid64_t>> pkHashs)
        : _pkReader(pkReader)
        , _pkHashs(pkHashs)
    {
    }
    ~PkCheckWorkItem() {}
    void process() override
    {
        for (auto& [pkHash, docId] : _pkHashs) {
            auto oldDocId = _pkReader->LookupWithDocRange(pkHash, {0, docId}, nullptr /*executor*/);
            if (oldDocId != INVALID_DOCID) {
                AUTIL_LOG(ERROR, "check failed, pkHash[%s], old docId [%ld] vs new docId [%ld]",
                          pkHash.toString().c_str(), oldDocId, docId);
                INDEXLIB_THROW(util::InconsistentStateException,
                               "check failed, pkHash[%s], old docId [%ld] vs new docId [%ld]",
                               pkHash.toString().c_str(), oldDocId, docId);
            }
        }
    }

private:
    const PrimaryKeyIndexReader* _pkReader = nullptr;
    std::vector<std::pair<autil::uint128_t, docid64_t>> _pkHashs;

private:
    AUTIL_LOG_DECLARE();
};

AUTIL_LOG_SETUP(indexlib.index, PkCheckWorkItem);

bool PrimaryKeyDuplicationChecker::WaitFinish()
{
    try {
        auto workItem = new PkCheckWorkItem(_pkReader, _pkHashs);
        auto ec = _threadPool->pushWorkItem(workItem);
        if (ec != autil::ThreadPoolBase::ERROR_NONE) {
            AUTIL_LOG(ERROR, "check failed, push workitem failed, ec [%d]", ec);
            return false;
        }
        _threadPool->waitFinish();
        _threadPool->checkException();
    } catch (const std::exception& e) {
        AUTIL_LOG(ERROR, "check failed, catch exception [%s]", e.what());
        return false;
    }
    return true;
}

bool PrimaryKeyDuplicationChecker::PushKeyImpl(const autil::uint128_t& pkHash, docid64_t docId)
{
    try {
        _pkHashs.push_back({pkHash, docId});
        if (_pkHashs.size() >= 1000) {
            auto workItem = new PkCheckWorkItem(_pkReader, _pkHashs);
            auto ec = _threadPool->pushWorkItem(workItem);
            if (ec != autil::ThreadPoolBase::ERROR_NONE) {
                AUTIL_LOG(ERROR, "check failed, push workitem failed, ec [%d]", ec);
                return false;
            }
            _pkHashs.clear();
        }
    } catch (const std::exception& e) {
        AUTIL_LOG(ERROR, "check failed, catch exception [%s]", e.what());
        return false;
    }
    return true;
}

} // namespace indexlib::index
