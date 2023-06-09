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
#ifndef __INDEXLIB_PLUGIN_PLUGINS_AITHETA_INDEX_ONLINE_INDEX_H
#define __INDEXLIB_PLUGIN_PLUGINS_AITHETA_INDEX_ONLINE_INDEX_H

#include <aitheta/index_factory.h>
#include "indexlib/common_define.h"
#include "indexlib/misc/log.h"
#include "indexlib/file_system/Directory.h"
#include "indexlib/indexlib.h"
#include "indexlib_plugin/plugins/aitheta/common_define.h"
#include "indexlib_plugin/plugins/aitheta/util/search_util.h"
#include "indexlib_plugin/plugins/aitheta/index/online_index_attr.h"

namespace indexlib {
namespace aitheta_plugin {

typedef std::function<bool(uint64_t docid)> FilterFunc;

struct OnlineIndex : public OnlineIndexAttr {
    OnlineIndex(const IndexAttr& indexAttr, int64_t sig, const aitheta::IndexParams& p, bool egpu,
                ContextHolderPtr holder)
        : OnlineIndexAttr(indexAttr, sig, p, egpu, holder) {}
    virtual ~OnlineIndex() = default;

    virtual bool Search(const Query& query, const FilterFunc& docIdFilter,
                        aitheta::IndexSearcher::Context*& results) = 0;
    virtual bool Build(docid_t id, const EmbeddingPtr& embedding, uint32_t dimension) = 0;
};

DEFINE_SHARED_PTR(OnlineIndex);

class OnlineIndexHolder {
 public:
    OnlineIndexHolder() {}
    ~OnlineIndexHolder() = default;

 public:
    bool Add(catid_t catid, const OnlineIndexPtr& attr) { return mCatid2OnlineIndex.emplace(catid, attr).second; }
    bool Has(catid_t catid) const { return mCatid2OnlineIndex.find(catid) != mCatid2OnlineIndex.end(); }
    const std::unordered_map<catid_t, OnlineIndexPtr>& Get() const { return mCatid2OnlineIndex; }
    bool Get(catid_t catid, OnlineIndexPtr& attr) const {
        auto iter = mCatid2OnlineIndex.find(catid);
        if (iter == mCatid2OnlineIndex.end() || !iter->second) {
            return false;
        }
        attr = iter->second;
        return true;
    }

 private:
    std::unordered_map<catid_t, OnlineIndexPtr> mCatid2OnlineIndex;
};

DEFINE_SHARED_PTR(OnlineIndexHolder);

}
}

#endif  //  __INDEXLIB_PLUGIN_PLUGINS_AITHETA_INDEX_ONLINE_INDEX_H