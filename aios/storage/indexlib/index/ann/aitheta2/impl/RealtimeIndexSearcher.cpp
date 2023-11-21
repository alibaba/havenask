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

#include "indexlib/index/ann/aitheta2/impl/RealtimeIndexSearcher.h"
namespace indexlibv2::index::ann {

bool RealtimeIndexSearcher::Init() { return InitMeasure(_indexStreamer->meta().measure_name()); }

bool RealtimeIndexSearcher::Search(const AithetaQuery& query, const std::shared_ptr<AithetaAuxSearchInfoBase>& info,
                                   ResultHolder& resultHolder)
{
    return SearchImpl(_indexStreamer, query, info, resultHolder);
}

bool RealtimeIndexSearcher::ParseQueryParameter(const std::string& searchParams, AiThetaParams& aiThetaParams) const
{
    auto initializer = ParamsInitializerFactory::Create(_searcherName);
    ANN_CHECK(initializer, "create initializer factory failed");

    AithetaIndexConfig newConfig = _indexConfig;
    newConfig.realtimeConfig.indexParams = searchParams;
    return initializer->InitRealtimeSearchParams(newConfig, aiThetaParams);
}

AUTIL_LOG_SETUP(indexlib.index, RealtimeIndexSearcher);
} // namespace indexlibv2::index::ann
