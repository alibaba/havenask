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
#ifndef ISEARCH_BS_CHECKPOINTWHITELIST_H
#define ISEARCH_BS_CHECKPOINTWHITELIST_H

#include "autil/legacy/jsonizable.h"
#include "build_service/common_define.h"
#include "build_service/util/Log.h"
#include "indexlib/indexlib.h"

namespace build_service { namespace config {

class CheckpointList : public autil::legacy::Jsonizable
{
public:
    CheckpointList() {}
    ~CheckpointList() {}
    CheckpointList(const CheckpointList& other) : _idSet(other._idSet) {}

    CheckpointList& operator=(const CheckpointList& other)
    {
        _idSet = other._idSet;
        return *this;
    }

public:
    void Jsonize(autil::legacy::Jsonizable::JsonWrapper& json) override;
    bool loadFromString(const std::string& content);

    const std::set<versionid_t>& getIdSet() const { return _idSet; }
    std::set<versionid_t>& getIdSet() { return _idSet; }
    void resize(size_t countLimit)
    {
        int removeCount = _idSet.size() - countLimit;
        auto it = _idSet.begin();
        while (it != _idSet.end() && removeCount > 0) {
            auto current = it++;
            _idSet.erase(current);
            removeCount--;
        }
    }
    void add(const versionid_t& versionId) { _idSet.insert(versionId); }
    bool check(const versionid_t& versionId) const { return _idSet.find(versionId) != _idSet.end(); }

    bool remove(const versionid_t& versionId);

    size_t size() const { return _idSet.size(); }

private:
    std::set<versionid_t> _idSet;

private:
    BS_LOG_DECLARE();
};

BS_TYPEDEF_PTR(CheckpointList);

}} // namespace build_service::config

#endif // ISEARCH_BS_CHECKPOINTLIST_H
