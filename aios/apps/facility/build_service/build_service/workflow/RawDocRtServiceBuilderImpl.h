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
#ifndef ISEARCH_BS_RAWDOCRTSERVICEBUILDERIMPL_H
#define ISEARCH_BS_RAWDOCRTSERVICEBUILDERIMPL_H

#include "build_service/common_define.h"
#include "build_service/util/Log.h"
#include "build_service/workflow/RealtimeBuilderImplBase.h"

namespace build_service { namespace workflow {

class DocReaderProducer;

class RawDocRtServiceBuilderImpl : public RealtimeBuilderImplBase
{
public:
    RawDocRtServiceBuilderImpl(const std::string& configPath, const indexlib::partition::IndexPartitionPtr& indexPart,
                               const RealtimeBuilderResource& builderResource);
    virtual ~RawDocRtServiceBuilderImpl();

private:
    RawDocRtServiceBuilderImpl(const RawDocRtServiceBuilderImpl&);
    RawDocRtServiceBuilderImpl& operator=(const RawDocRtServiceBuilderImpl&);

protected:
    bool doStart(const proto::PartitionId& partitionId, KeyValueMap& kvMap) override;
    bool getLastTimestampInProducer(int64_t& timestamp) override;
    bool getLastReadTimestampInProducer(int64_t& timestamp) override;
    bool producerAndBuilderPrepared() const override;
    bool producerSeek(const common::Locator& locator) override;
    virtual void externalActions() override;
    bool seekProducerToLatest(std::pair<int64_t, int64_t>& forceSeekInfo) override;

private:
    bool getBuilderAndProducer();

private:
    DocReaderProducer* _producer;
    std::unique_ptr<FlowFactory> _factory;
    bool _startSkipCalibrateDone;
    bool _isProducerRecovered;
    indexlib::document::SrcSignature _rtSrcSignature;

private:
    BS_LOG_DECLARE();
};

BS_TYPEDEF_PTR(RawDocRtServiceBuilderImpl);

}} // namespace build_service::workflow

#endif // ISEARCH_BS_RAWDOCRTSERVICEBUILDERIMPL_H
