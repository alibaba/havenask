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
#ifndef __INDEXLIB_PLUGIN_PLUGINS_AITHETA_INDEX_BUILT_INDEX_H
#define __INDEXLIB_PLUGIN_PLUGINS_AITHETA_INDEX_BUILT_INDEX_H

#include <aitheta/index_factory.h>
#include "indexlib/common_define.h"
#include "indexlib/misc/log.h"
#include "indexlib/file_system/Directory.h"
#include "indexlib/indexlib.h"
#include "indexlib_plugin/plugins/aitheta/index/online_index.h"

namespace indexlib {
namespace aitheta_plugin {

struct BuiltIndex final : public OnlineIndex {
    BuiltIndex(const IndexSearcherPtr& isearcher, const IndexAttr& indexAttr, int64_t signature,
               const aitheta::IndexParams& params, bool enableGpu, ContextHolderPtr holder)
        : OnlineIndex(indexAttr, signature, params, enableGpu, holder), searcher(isearcher) {}
    ~BuiltIndex() = default;

    bool Search(const Query& query, const FilterFunc& docIdFilter, aitheta::IndexSearcher::Context*& res) override;
    bool Build(docid_t id, const EmbeddingPtr& embedding, uint32_t dimension) override;

    IndexSearcherPtr searcher;

    IE_LOG_DECLARE();
};

}
}

#endif  //  __INDEXLIB_PLUGIN_PLUGINS_AITHETA_INDEX_BUILT_INDEX_H