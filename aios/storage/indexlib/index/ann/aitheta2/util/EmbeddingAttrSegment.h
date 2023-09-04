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

#include "autil/NoCopyable.h"
#include "indexlib/index/ann/aitheta2/CommonDefine.h"
#include "indexlib/index/ann/aitheta2/util/EmbeddingDataExtractor.h"

namespace indexlibv2::index::ann {

class EmbeddingAttrSegmentBase: public autil::NoCopyable {
public:
    EmbeddingAttrSegmentBase() = default;
    virtual ~EmbeddingAttrSegmentBase() = default;

public:
    virtual std::shared_ptr<EmbeddingDataExtractor> CreateEmbeddingExtractor(const AithetaIndexConfig& indexConfig, const MergeTask& mergeTask) {
        return std::make_shared<EmbeddingDataExtractor>(indexConfig, mergeTask);
    }
};

} // namespace indexlibv2::index::ann
