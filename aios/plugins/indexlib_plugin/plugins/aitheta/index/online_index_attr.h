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
#ifndef __INDEXLIB_PLUGIN_PLUGINS_AITHETA_INDEX_ONLINE_INDEX_ATTR_H
#define __INDEXLIB_PLUGIN_PLUGINS_AITHETA_INDEX_ONLINE_INDEX_ATTR_H

#include <aitheta/index_factory.h>
#include "indexlib/common_define.h"
#include "indexlib/misc/log.h"
#include "indexlib/file_system/Directory.h"
#include "indexlib/indexlib.h"
#include "indexlib_plugin/plugins/aitheta/common_define.h"
#include "indexlib_plugin/plugins/aitheta/index/index_attr.h"

namespace indexlib {
namespace aitheta_plugin {

struct OnlineIndexAttr : public IndexAttr {
    OnlineIndexAttr(const IndexAttr& indexAttr, int64_t sig, const aitheta::IndexParams& p, bool egpu,
                    ContextHolderPtr holder)
        : IndexAttr(indexAttr), signature(sig), indexParams(p), enableGpu(egpu), cxtHolder(holder) {}
    virtual ~OnlineIndexAttr() = default;

    // OnlineIndexs share the same context when the signatures are the same
    int64_t signature;
    aitheta::IndexParams indexParams;
    bool enableGpu;
    ContextHolderPtr cxtHolder;
};

}
}

#endif  //  __INDEXLIB_PLUGIN_PLUGINS_AITHETA_INDEX_ONLINE_INDEX_ATTR_H