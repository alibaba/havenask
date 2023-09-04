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

#include "autil/legacy/jsonizable.h"
#include "indexlib/file_system/Directory.h"
#include "indexlib/index/ann/aitheta2/CommonDefine.h"

namespace indexlibv2::index::ann {

const static std::string INDEX_FILE = "aitheta.index";
const static std::string INDEX_ADDR_FILE = "aitheta.index.addr";

struct IndexDataAddr : public autil::legacy::Jsonizable {
    void Jsonize(JsonWrapper& json) override
    {
        json.Jsonize("offset", offset, 0ul);
        json.Jsonize("length", length, 0ul);
    }
    size_t offset = 0ul;
    size_t length = 0ul;
};

class IndexDataAddrHolder : public autil::legacy::Jsonizable
{
public:
    IndexDataAddrHolder() = default;
    ~IndexDataAddrHolder() = default;

public:
    bool Dump(const indexlib::file_system::DirectoryPtr& dir) const;
    bool Load(const indexlib::file_system::DirectoryPtr& dir);
    bool HasAddr(index_id_t id) const { return _map.find(id) != _map.end(); }
    bool GetAddr(index_id_t id, IndexDataAddr& addr) const
    {
        auto iter = _map.find(id);
        if (iter != _map.end()) {
            addr = iter->second;
            return true;
        }
        return false;
    }
    bool AddAddr(index_id_t id, const IndexDataAddr& addr) { return _map.emplace(id, addr).second; }
    void Jsonize(JsonWrapper& json) override { json.Jsonize("addr", _map, {}); }

public:
    std::map<index_id_t, IndexDataAddr>& TEST_GetIndexDataMap() { return _map; }

private:
    std::map<index_id_t, IndexDataAddr> _map;
    indexlib::file_system::DirectoryPtr _dir;
    AUTIL_LOG_DECLARE();
};

} // namespace indexlibv2::index::ann
