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
#ifndef __INDEXLIB_PLUGIN_PLUGINS_AITHETA_UTIL_RAW_HOLDER_ITER_IMPL_H
#define __INDEXLIB_PLUGIN_PLUGINS_AITHETA_UTIL_RAW_HOLDER_ITER_IMPL_H

#include "indexlib/indexlib.h"
#include "indexlib/common_define.h"
#include "indexlib/index/normal/inverted_index/customized_index/indexer_define.h"
#include "indexlib_plugin/plugins/aitheta/util/embed_holder.h"
#include "indexlib_plugin/plugins/aitheta/util/embed_holder_iterator/i_embed_holder_iterator_impl.h"
#include "indexlib_plugin/plugins/aitheta/common_define.h"

namespace indexlib {
namespace aitheta_plugin {

class RawSgtHolderIteratorImpl : public IEmbedHolderIteratorImpl {
 public:
    RawSgtHolderIteratorImpl(const std::map<int64_t, EmbedHolderPtr> &m, const DocIdMapPtr &d = nullptr)
        : IEmbedHolderIteratorImpl(d), mCatId2EmbedHolder(m), isValueReady(false) {}
    ~RawSgtHolderIteratorImpl() = default;

 public:
    EmbedHolderPtr Value() override;
    void Next() override;
    bool IsValid() const override;

 private:
    std::map<int64_t, EmbedHolderPtr> mCatId2EmbedHolder;
    bool isValueReady;
    IE_LOG_DECLARE();
};

}
}

#endif  // __INDEXLIB_PLUGIN_PLUGINS_AITHETA_UTIL_RAW_HOLDER_ITER_IMPL_H
