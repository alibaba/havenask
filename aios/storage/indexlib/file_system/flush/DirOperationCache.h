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

#include <set>
#include <string>

#include "autil/Lock.h"
#include "autil/Log.h"

namespace indexlib { namespace file_system {

class DirOperationCache
{
public:
    DirOperationCache();
    ~DirOperationCache();

public:
    void MkParentDirIfNecessary(const std::string& path);
    // we will normalize path inside
    void Mkdir(const std::string& path);

private:
    bool Get(const std::string& path);
    void Set(const std::string& path);

public:
    // path has to be a file!
    static std::string GetParent(const std::string& path);

private:
    autil::ThreadMutex _dirSetMutex;
    autil::ThreadMutex _mkDirMutex;
    std::set<std::string> _dirs;

private:
    AUTIL_LOG_DECLARE();
    friend class DirOperationCacheTest;
};

typedef std::shared_ptr<DirOperationCache> DirOperationCachePtr;
}} // namespace indexlib::file_system
