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

#include "autil/Log.h"
#include "indexlib/index/ann/aitheta2/CommonDefine.h"
#include "indexlib/index/ann/aitheta2/impl/NormalSegment.h"
#include "indexlib/index/ann/aitheta2/util/PkDataHolder.h"

namespace indexlibv2::index::ann {

class PkDataExtractor
{
public:
    PkDataExtractor(const MergeTask& mergeTask, bool compactIndex) : _mergeTask(mergeTask), _compactIndex(compactIndex)
    {
    }
    ~PkDataExtractor() = default;

public:
    bool Extract(const std::vector<NormalSegmentPtr>& segments, PkDataHolder& result);

    bool ExtractWithoutDocIdMap(const NormalSegmentPtr& segment, PkDataHolder& result);

private:
    MergeTask _mergeTask;
    bool _compactIndex;
    AUTIL_LOG_DECLARE();
};

} // namespace indexlibv2::index::ann
