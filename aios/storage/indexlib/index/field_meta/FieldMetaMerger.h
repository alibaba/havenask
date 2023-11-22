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

#include <any>

#include "autil/Log.h"
#include "autil/NoCopyable.h"
#include "indexlib/base/Status.h"
#include "indexlib/config/IIndexConfig.h"
#include "indexlib/framework/SegmentMeta.h"
#include "indexlib/index/IIndexMerger.h"
#include "indexlib/index/field_meta/config/FieldMetaConfig.h"

namespace indexlibv2::framework {
class IndexTaskResourceManager;
}

namespace indexlib::index {

class FieldMetaMerger : public indexlibv2::index::IIndexMerger
{
public:
    FieldMetaMerger() = default;
    ~FieldMetaMerger() = default;

public:
    Status Init(const std::shared_ptr<indexlibv2::config::IIndexConfig>& indexConfig,
                const std::map<std::string, std::any>& params) override;
    Status Merge(const SegmentMergeInfos& segMergeInfos,
                 const std::shared_ptr<indexlibv2::framework::IndexTaskResourceManager>& taskResourceManager) override;

private:
    Status MergeWithSource(const SegmentMergeInfos& segMergeInfos,
                           const std::shared_ptr<indexlibv2::framework::IndexTaskResourceManager>& taskResourceManager);
    Status
    MergeWithoutSource(const SegmentMergeInfos& segMergeInfos,
                       const std::shared_ptr<indexlibv2::framework::IndexTaskResourceManager>& taskResourceManager);

private:
    std::map<std::string, std::any> _params;
    std::shared_ptr<FieldMetaConfig> _fieldMetaConfig;

private:
    AUTIL_LOG_DECLARE();
};

} // namespace indexlib::index
