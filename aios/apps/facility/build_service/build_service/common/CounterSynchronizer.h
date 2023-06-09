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
#ifndef ISEARCH_BS_COUNTERSYNCHRONIZER_H
#define ISEARCH_BS_COUNTERSYNCHRONIZER_H

#include "autil/LoopThread.h"
#include "build_service/common_define.h"
#include "build_service/proto/BasicDefs.pb.h"
#include "build_service/util/Log.h"
#include "indexlib/util/counter/CounterMap.h"

namespace build_service { namespace common {

class CounterSynchronizer
{
public:
    CounterSynchronizer() {};
    virtual ~CounterSynchronizer() {};

private:
    CounterSynchronizer(const CounterSynchronizer&);
    CounterSynchronizer& operator=(const CounterSynchronizer&);

public:
    static std::string getCounterZkRoot(const std::string& appZkRoot, const proto::BuildId& buildId);
    static bool getCounterZkFilePath(const std::string& appZkRoot, const proto::PartitionId& pid,
                                     std::string& counterFilePath, bool ignoreBackupId = false);
    static std::string getCounterRedisKey(const std::string& serviceName, const proto::BuildId& buildId);

    static bool getCounterRedisHashField(const proto::PartitionId& pid, std::string& fieldName);

public:
    bool init(const indexlib::util::CounterMapPtr& counterMap)
    {
        if (!counterMap) {
            BS_LOG(ERROR, "init failed due to counterMap is NULL");
            return false;
        }
        _counterMap = counterMap;
        return true;
    }

    indexlib::util::CounterMapPtr getCounterMap() const { return _counterMap; }

    bool beginSync(int64_t syncInterval = DEFAULT_SYNC_INTERVAL);
    void stopSync();
    virtual bool sync() const = 0;

protected:
    indexlib::util::CounterMapPtr _counterMap;
    autil::LoopThreadPtr _syncThread;
    static const uint64_t DEFAULT_SYNC_INTERVAL = 30u;

private:
    BS_LOG_DECLARE();
};

BS_TYPEDEF_PTR(CounterSynchronizer);

}} // namespace build_service::common

#endif // ISEARCH_BS_COUNTERSYNCHRONIZER_H
