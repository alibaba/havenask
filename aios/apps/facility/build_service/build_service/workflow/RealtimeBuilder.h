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

#include <stdint.h>
#include <string>

#include "build_service/common/ResourceContainer.h"
#include "build_service/common_define.h"
#include "build_service/proto/BasicDefs.pb.h"
#include "build_service/workflow/RealtimeBuilderDefine.h"
#include "build_service/workflow/RealtimeErrorDefine.h"
#include "indexlib/partition/index_partition.h"

BS_DECLARE_REFERENCE_CLASS(builder, Builder);

namespace build_service { namespace workflow {

class RealtimeBuilderImplBase;

// RealtimeBuilderImplBase to be recreated after load full index or force reopen.
class RealtimeBuilder
{
public:
    RealtimeBuilder(const std::string& configPath, const indexlib::partition::IndexPartitionPtr& indexPart,
                    const RealtimeBuilderResource& builderResource);
    virtual ~RealtimeBuilder();

private:
    RealtimeBuilder(const RealtimeBuilder&);
    RealtimeBuilder& operator=(const RealtimeBuilder&);

public:
    bool start(const proto::PartitionId& partitionId);
    void stop();
    void suspendBuild();
    void resumeBuild();
    bool isRecovered();
    void forceRecover();
    void setTimestampToSkip(int64_t timestamp);
    void getErrorInfo(RealtimeErrorCode& errorCode, std::string& errorMsg, int64_t& errorTime /* time in us */) const;
    builder::Builder* GetBuilder() const;

public:
    static bool loadRealtimeInfo(const std::string& root, RealtimeInfoWrapper* realtimeInfo);

private:
    virtual bool downloadConfig(const std::string& indexRoot, KeyValueMap* kvMap);
    virtual std::string getIndexRoot() const;

    bool getRealtimeInfo(KeyValueMap* kvMap);
    RealtimeBuilderImplBase* createRealtimeBuilderImpl(const proto::PartitionId& partitionId);

private:
    RealtimeBuilderImplBase* _impl;

private:
    std::string _configPath;
    indexlib::partition::IndexPartitionPtr _indexPartition;
    RealtimeBuilderResource _builderResource;
    KeyValueMap _kvMap;

private:
    BS_LOG_DECLARE();
};

}} // namespace build_service::workflow
