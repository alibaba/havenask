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
#pragma once
#include <mutex>

#include "build_service/common_define.h"
#include "build_service/proto/BasicDefs.pb.h"
#include "build_service/util/Log.h"
#include "build_service/workflow/RealtimeBuilderDefine.h"
#include "build_service/workflow/RealtimeBuilderImplV2.h"
#include "build_service/workflow/RealtimeErrorDefine.h"
#include "future_lite/NamedTaskScheduler.h"
#include "future_lite/coro/Lazy.h"
#include "indexlib/base/Status.h"
#include "indexlib/framework/VersionMeta.h"

namespace indexlibv2::framework {
class ITablet;
}

namespace build_service::workflow {

class RealtimeBuilderImplBase;

class RealtimeBuilderV2
{
public:
    RealtimeBuilderV2(std::string configPath, std::shared_ptr<indexlibv2::framework::ITablet> tablet,
                      RealtimeBuilderResource builderResource);
    virtual ~RealtimeBuilderV2() {}

public:
    bool start(const proto::PartitionId& partitionId);
    void stop();
    void suspendBuild();
    void resumeBuild();
    void forceRecover();
    void setTimestampToSkip(int64_t timestamp);
    bool isRecovered();
    void getErrorInfo(RealtimeErrorCode& errorCode, std::string& errorMsg, int64_t& errorTime) const;

    bool needCommit() const;
    std::pair<indexlib::Status, indexlibv2::framework::VersionMeta> commit();
    bool isFinished() const;

public:
    RealtimeBuilderImplV2* TEST_getImpl() const
    {
        std::lock_guard<std::mutex> lock(_implLock);
        return _impl.get();
    }
    RealtimeBuilderResource TEST_getRealtimeBuilderResource() const { return _builderResource; }

protected:
    // virtual for mock
    virtual std::string getIndexRoot() const;
    virtual bool downloadConfig(const std::string& indexRoot, KeyValueMap* kvMap) const;
    virtual RealtimeBuilderImplV2* createRealtimeBuilderImpl(const proto::PartitionId& partitionId,
                                                             const KeyValueMap& kvMap) const;

private:
    bool getRealtimeInfo(const proto::PartitionId& partitionId, KeyValueMap* kvMap);
    bool getRealtimeInfoFromDataDescription(const std::string& dataTableName, config::ResourceReader& resourceReader,
                                            KeyValueMap* kvMap);
    void checkAndReconstruct(const proto::PartitionId& partitionId);

private:
    std::string _configPath;
    std::shared_ptr<indexlibv2::framework::ITablet> _tablet;
    RealtimeBuilderResource _builderResource;
    std::unique_ptr<future_lite::NamedTaskScheduler> _tasker;

    mutable std::mutex _implLock;
    std::unique_ptr<RealtimeBuilderImplV2> _impl;

private:
    BS_LOG_DECLARE();
    friend class RealtimeBuilderV2Test;
};

} // namespace build_service::workflow
