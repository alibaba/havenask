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

#include "autil/Lock.h"
#include "autil/Log.h"
#include "indexlib/index/ann/aitheta2/CommonDefine.h"
#include "indexlib/index/ann/aitheta2/impl/RealtimeSegmentBuildResource.h"
#include "indexlib/index/ann/aitheta2/impl/SegmentMeta.h"
#include "indexlib/index/ann/aitheta2/util/segment_data/SegmentDataReader.h"

namespace indexlibv2::index::ann {

struct AiThetaFactoryWrapper {
public:
    AiThetaFactoryWrapper() = default;
    ~AiThetaFactoryWrapper() = default;

public:
    static bool CreateBuilder(const AithetaIndexConfig& indexConfig, size_t docCount, AiThetaBuilderPtr& builder);
    static bool CreateSearcher(const AithetaIndexConfig& indexConfig, const IndexMeta& indexMeta,
                               const IndexDataReaderPtr& reader, AiThetaSearcherPtr& flow);
    static bool CreateStreamer(const AithetaIndexConfig& indexConfig,
                               const std::shared_ptr<RealtimeIndexBuildResource>& resource,
                               AiThetaStreamerPtr& streamer);
    static bool CreateReducer(const AithetaIndexConfig& config, AiThetaReducerPtr& reducer);

private:
    static bool CreateStorage(const std::string& name, const AiThetaParams& params, AiThetaStoragePtr& storage);

private:
    static autil::SpinLock lock;

private:
    AUTIL_LOG_DECLARE();
};

} // namespace indexlibv2::index::ann
