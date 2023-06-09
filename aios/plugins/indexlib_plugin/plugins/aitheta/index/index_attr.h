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
#ifndef __INDEXLIB_PLUGIN_PLUGINS_AITHETA_INDEX_INDEX_ATTR_H
#define __INDEXLIB_PLUGIN_PLUGINS_AITHETA_INDEX_INDEX_ATTR_H

#include <aitheta/index_factory.h>
#include "indexlib/common_define.h"
#include "indexlib/misc/log.h"
#include "indexlib/file_system/Directory.h"
#include "indexlib/indexlib.h"
#include "indexlib_plugin/plugins/aitheta/common_define.h"

namespace indexlib {
namespace aitheta_plugin {

struct IndexAttr {
    IndexAttr(size_t num = 0u, const aitheta::MipsReformer &rfmr = aitheta::MipsReformer(0u, 0.0f, 0.0f))
        : docNum(num), reformer(rfmr) {}
    IndexAttr(const IndexAttr &indexAttr) = default;
    IndexAttr &operator=(const IndexAttr &indexAttr) = default;

    size_t docNum;
    aitheta::MipsReformer reformer;
};

}
}

#endif  //  __INDEXLIB_PLUGIN_PLUGINS_AITHETA_INDEX_INDEX_ATTR_H