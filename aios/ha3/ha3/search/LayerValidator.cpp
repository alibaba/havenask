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
#include "ha3/search/LayerValidator.h"

#include <stddef.h>
#include <memory>
#include <string>
#include <vector>

#include "alog/Logger.h"
#include "indexlib/index_base/index_meta/partition_meta.h"
#include "indexlib/misc/common.h"

#include "ha3/common/LayerDescription.h"
#include "ha3/common/RankHint.h"
#include "autil/Log.h"

using namespace indexlib::index_base;
using namespace isearch::common;

namespace isearch {
namespace search {
AUTIL_LOG_SETUP(ha3, LayerValidator);

LayerValidator::LayerValidator() { 
}

LayerValidator::~LayerValidator() { 
}

bool LayerValidator::compareRankHint(
        const indexlibv2::config::SortDescription& sortDesc,
        const RankHint* hint)
{
    if (hint->sortField == sortDesc.GetSortFieldName() 
        && hint->sortPattern == sortDesc.GetSortPattern())
    {
        return true;
    } else {
        return false;
    }
}

bool LayerValidator::validateLayerWithoutDimen(
        const PartitionMetaPtr& meta,
        const RankHint* hint)
{
    if (meta->Size() == 0) {
        return false;
    } 
    return compareRankHint(meta->GetSortDescription(0), hint);
}

bool LayerValidator::validateRankHint(
        const PartitionMetaPtr& meta,
        const RankHint* hint, 
        const LayerKeyRange *keyRange,
        size_t dimenCount)
{
    if (!hint || hint->sortField.empty()) {
        return true;
    }
    if (dimenCount == 0) {
        return validateLayerWithoutDimen(meta, hint);
    }
    
    size_t partMetaDimenCount = meta->Size();
    if (dimenCount > partMetaDimenCount) {
        AUTIL_LOG(DEBUG, "set QM_PER_LAYER because of dimenCount exceed partMetaCount");
        return false;
    }
    if (keyRange->ranges.empty() && dimenCount < partMetaDimenCount) {
        return compareRankHint(meta->GetSortDescription(dimenCount), hint);
    } else if (keyRange->values.empty() && keyRange->ranges.size() == 1) {
        return compareRankHint(meta->GetSortDescription(dimenCount - 1), hint);
    } else {
        return false;
    }
}

} // namespace search
} // namespace isearch

