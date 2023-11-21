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

#include <memory>
#include <stdint.h>
#include <string>

#include "build_service/common/Locator.h"
#include "build_service/common/ResourceContainer.h"
#include "build_service/common_define.h"
#include "build_service/proto/BasicDefs.pb.h"
#include "build_service/workflow/FlowFactory.h"
#include "build_service/workflow/RealtimeBuilderDefine.h"
#include "build_service/workflow/RealtimeBuilderImplBase.h"
#include "indexlib/document/index_locator.h"
#include "indexlib/partition/index_partition.h"

namespace build_service { namespace workflow {

class DocReaderProducer;

class RawDocRtJobBuilderImpl : public RealtimeBuilderImplBase
{
public:
    RawDocRtJobBuilderImpl(const std::string& configPath, const indexlib::partition::IndexPartitionPtr& indexPart,
                           const RealtimeBuilderResource& builderResource);
    ~RawDocRtJobBuilderImpl();

private:
    RawDocRtJobBuilderImpl(const RawDocRtJobBuilderImpl&);
    RawDocRtJobBuilderImpl& operator=(const RawDocRtJobBuilderImpl&);

protected: /*override*/
    bool doStart(const proto::PartitionId& partitionId, KeyValueMap& kvMap) override;
    bool getLastTimestampInProducer(int64_t& timestamp) override;
    bool getLastReadTimestampInProducer(int64_t& timestamp) override;
    bool producerAndBuilderPrepared() const override;
    bool producerSeek(const common::Locator& locator) override;
    void externalActions() override {}

private:
    bool getBuilderAndProducer();

private:
    DocReaderProducer* _producer;
    std::unique_ptr<FlowFactory> _factory;
    indexlib::document::SrcSignature _rtSrcSignature;

private:
    BS_LOG_DECLARE();
};

BS_TYPEDEF_PTR(RawDocRtJobBuilderImpl);

}} // namespace build_service::workflow
