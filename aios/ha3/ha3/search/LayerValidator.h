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

#include <stddef.h>
#include <memory>

#include "autil/Log.h" // IWYU pragma: keep
#include "indexlib/index_base/index_meta/partition_meta.h"
#include "indexlib/misc/common.h"

namespace isearch {
namespace common {
class LayerKeyRange;
struct RankHint;
}  // namespace common
}  // namespace isearch

namespace isearch {
namespace search {

class LayerValidator
{
public:
    LayerValidator();
    ~LayerValidator();
private:
    LayerValidator(const LayerValidator &);
    LayerValidator& operator=(const LayerValidator &);
public:
    bool validateRankHint(const indexlib::index_base::PartitionMetaPtr& meta,
                          const common::RankHint* hint, 
                          const common::LayerKeyRange* keyRange,
                          size_t dimenCount);
private:
    bool validateLayerWithoutDimen(
            const indexlib::index_base::PartitionMetaPtr& meta,
            const common::RankHint* hint);
    bool compareRankHint(
            const indexlibv2::config::SortDescription& sortDesc,
            const common::RankHint* hint);

private:
    AUTIL_LOG_DECLARE();
};

typedef std::shared_ptr<LayerValidator> LayerValidatorPtr;

} // namespace search
} // namespace isearch

