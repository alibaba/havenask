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
#ifndef __INDEXLIB_PLUGIN_PLUGINS_AITHETA_UTIL_I_EMBED_INFO_HOLDER_ITER_IMPL_H
#define __INDEXLIB_PLUGIN_PLUGINS_AITHETA_UTIL_I_EMBED_INFO_HOLDER_ITER_IMPL_H

#include "indexlib/indexlib.h"
#include "indexlib/common_define.h"
#include "indexlib/index/normal/inverted_index/customized_index/indexer_define.h"
#include "indexlib_plugin/plugins/aitheta/util/embed_holder.h"
#include "indexlib_plugin/plugins/aitheta/common_define.h"

namespace indexlib {
namespace aitheta_plugin {

class IEmbedHolderIteratorImpl {
 public:
    IEmbedHolderIteratorImpl(const DocIdMapPtr &docIdMap) : mDocIdMap(docIdMap){};
    virtual ~IEmbedHolderIteratorImpl() = default;

 public:
    virtual EmbedHolderPtr Value() = 0;
    virtual void Next() = 0;
    virtual bool IsValid() const = 0;

 protected:
    DocIdMapPtr mDocIdMap;
};

}
}

#endif  // __INDEXLIB_PLUGIN_PLUGINS_AITHETA_UTIL_I_EMBED_INFO_HOLDER_ITER_IMPL_H
