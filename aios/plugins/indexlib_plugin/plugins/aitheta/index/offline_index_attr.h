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
#ifndef __INDEXLIB_PLUGIN_PLUGINS_AITHETA_INDEX_OFFLINE_INDEX_ATTR_H
#define __INDEXLIB_PLUGIN_PLUGINS_AITHETA_INDEX_OFFLINE_INDEX_ATTR_H

#include <aitheta/index_factory.h>
#include "indexlib/common_define.h"
#include "indexlib/misc/log.h"
#include "indexlib/file_system/Directory.h"
#include "indexlib/indexlib.h"
#include "indexlib_plugin/plugins/aitheta/common_define.h"
#include "indexlib_plugin/plugins/aitheta/index/index_attr.h"

namespace indexlib {
namespace aitheta_plugin {

struct OfflineIndexAttr : public IndexAttr, public autil::legacy::Jsonizable {
    OfflineIndexAttr(catid_t id, const IndexAttr &indexAttr, size_t os, size_t sz)
        : IndexAttr(indexAttr), catid(id), offset(os), size(sz) {}
    OfflineIndexAttr() : IndexAttr(), catid(INVALID_CATEGORY_ID), offset(0ul), size(0ul) {}
    void Jsonize(JsonWrapper &json) override;
    catid_t catid;
    size_t offset;
    size_t size;
};

class OfflineIndexAttrHolder {
 public:
    OfflineIndexAttrHolder() = default;
    ~OfflineIndexAttrHolder() = default;

 public:
    void Add(catid_t catid, const IndexAttr &indexAttr, size_t offset, size_t size) {
        mOfflineIndexAttrs.emplace_back(catid, indexAttr, offset, size);
    }
    void Add(const OfflineIndexAttr &attr) { mOfflineIndexAttrs.emplace_back(attr); }
    bool Load(const indexlib::file_system::DirectoryPtr &);
    bool Dump(const indexlib::file_system::DirectoryPtr &);
    void Reset() { mOfflineIndexAttrs.clear(); }
    size_t Size() const { return mOfflineIndexAttrs.size(); }
    const std::vector<OfflineIndexAttr> &GetOfflineIndexAttrs() const { return mOfflineIndexAttrs; }
    const OfflineIndexAttr &Get(size_t idx) const { return mOfflineIndexAttrs[idx]; }

    //  public:
    //     typedef std::vector<OfflineIndexAttr>::iterator iterator;
    //     typedef std::vector<OfflineIndexAttr>::const_iterator const_iterator;

    //     iterator begin() noexcept { return mOfflineIndexAttrs.begin(); }
    //     const_iterator cbegin() const noexcept { return mOfflineIndexAttrs.cbegin(); }
    //     iterator end() noexcept { return mOfflineIndexAttrs.end(); }
    //     const_iterator cend() const noexcept { return mOfflineIndexAttrs.cend(); }

 private:
    std::vector<OfflineIndexAttr> mOfflineIndexAttrs;
    IE_LOG_DECLARE();
};

}
}

#endif  //  __INDEXLIB_PLUGIN_PLUGINS_AITHETA_INDEX_OFFLINE_INDEX_ATTR_H
