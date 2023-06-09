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

#include <map>
#include <string>

#include "indexlib/util/PathUtil.h"

namespace indexlib { namespace file_system {

template <typename T>
class DirectoryMapIterator
{
public:
    typedef std::map<std::string, T> DirectoryMap;
    typedef typename DirectoryMap::iterator Iterator;

public:
    DirectoryMapIterator(DirectoryMap& pathMap, const std::string& dir) : _directoryMap(pathMap)
    {
        std::string tmpDir = util::PathUtil::NormalizePath(dir);
        if (!tmpDir.empty() && *tmpDir.rbegin() == '/') {
            tmpDir.resize(tmpDir.size() - 1);
        }
        // not include dir self
        _it = _directoryMap.upper_bound(tmpDir);
        _dir = tmpDir + "/";
    }

    ~DirectoryMapIterator() {}

public:
    bool HasNext() const
    {
        if (_it == _directoryMap.end()) {
            return false;
        }

        const std::string& path = _it->first;
        if (path.size() <= _dir.size() || path.compare(0, _dir.size(), _dir) != 0) {
            return false;
        }
        return true;
    }

    Iterator Next() { return _it++; }

    void Erase(Iterator it) { _directoryMap.erase(it); }

private:
    DirectoryMap& _directoryMap;
    std::string _dir;
    Iterator _it;
};
}} // namespace indexlib::file_system
