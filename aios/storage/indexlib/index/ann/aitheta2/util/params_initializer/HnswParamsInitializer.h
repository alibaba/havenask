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
#include "indexlib/index/ann/aitheta2/util/params_initializer/ParamsInitializer.h"

namespace indexlibv2::index::ann {

class HnswParamsInitializer : public ParamsInitializer
{
public:
    HnswParamsInitializer(uint32_t docCount = 0) : _docCount(docCount) {}
    ~HnswParamsInitializer() = default;

public:
    bool InitSearchParams(const AithetaIndexConfig& indexConfig, AiThetaParams& indexParams) override;

private:
    uint32_t _docCount;
    AUTIL_LOG_DECLARE();
};
typedef std::shared_ptr<HnswParamsInitializer> HnswParamsInitializerPtr;

using CustomizedHnswParamsInitializer = HnswParamsInitializer;
using OswgParamsInitializer = HnswParamsInitializer;
using QGraphParamsInitializer = HnswParamsInitializer;

} // namespace indexlibv2::index::ann
