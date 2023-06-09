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
#ifndef __INDEXLIB_PLUGIN_PLUGINS_AITHETA_UTIL_EMBDD_INFO_VEC_EMBDD_INFO_VEC_ITER_H
#define __INDEXLIB_PLUGIN_PLUGINS_AITHETA_UTIL_EMBDD_INFO_VEC_EMBDD_INFO_VEC_ITER_H

#include "indexlib/indexlib.h"
#include "indexlib/common_define.h"
#include "indexlib/index/normal/inverted_index/customized_index/indexer_define.h"
#include "indexlib_plugin/plugins/aitheta/util/embed_holder.h"
#include "indexlib_plugin/plugins/aitheta/common_define.h"
#include "indexlib_plugin/plugins/aitheta/util/embed_holder_iterator/i_embed_holder_iterator_impl.h"
namespace indexlib {
namespace aitheta_plugin {

typedef IEmbedHolderIteratorImpl Node;
struct CmpFunc {
    bool operator()(const Node* lf, const Node* rt) {
        auto lf0 = const_cast<Node*>(lf);
        auto rt0 = const_cast<Node*>(rt);
        return (lf0->Value()->GetCatid() > rt0->Value()->GetCatid());
    }
};

class EmbedHolderIterator {
 public:
    EmbedHolderIterator(size_t dim = 0u, Node* node = nullptr, const std::set<int64_t>& catids = {});
    ~EmbedHolderIterator();
    EmbedHolderIterator(const EmbedHolderIterator&) = default;
    EmbedHolderIterator& operator=(const EmbedHolderIterator&) = default;

 public:
    EmbedHolderPtr Value();
    void Next();
    bool IsValid() const;

 public:
    bool Merge(const EmbedHolderIterator& other);

 private:
    bool BuildValue();

 private:
    size_t mDimension;
    std::priority_queue<Node*, std::vector<Node*>, CmpFunc> mHeap;
    std::set<catid_t> mBuildingCatids;
    EmbedHolderPtr mValue;
    IE_LOG_DECLARE();
};

}
}

#endif  // __INDEXLIB_PLUGIN_PLUGINS_AITHETA_UTIL_EMBDD_INFO_VEC_EMBDD_INFO_VEC_ITER_H
