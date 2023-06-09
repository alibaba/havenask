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
#include "indexlib/file_system/flush/DirOperationCache.h"

#include <iosfwd>
#include <vector>

#include "autil/CommonMacros.h"
#include "autil/Lock.h"
#include "autil/StringUtil.h"
#include "indexlib/file_system/ErrorCode.h"
#include "indexlib/file_system/fslib/FslibWrapper.h"
#include "indexlib/util/PathUtil.h"
#include "indexlib/util/RetryUtil.h"

using namespace std;
using namespace autil;

using namespace indexlib::util;

namespace indexlib { namespace file_system {
AUTIL_LOG_SETUP(indexlib.file_system, DirOperationCache);

DirOperationCache::DirOperationCache() {}

DirOperationCache::~DirOperationCache() {}

void DirOperationCache::MkParentDirIfNecessary(const string& path)
{
    string parent = GetParent(path);
    if (FslibWrapper::NeedMkParentDirBeforeOpen(path)) {
        Mkdir(parent);
    } else {
        Set(parent);
    }
}

void DirOperationCache::Mkdir(const string& path)
{
    if (!Get(path)) {
        ScopedLock lock(_mkDirMutex);
        if (!Get(path)) {
            ErrorCode ec;
            auto mkDirIfNotExist = [&ec, &path]() -> bool {
                ec = FslibWrapper::MkDirIfNotExist(path).Code();
                if (ec == FSEC_OK) {
                    return true;
                }
                return false;
            };
            RetryUtil::Retry(mkDirIfNotExist);
            THROW_IF_FS_ERROR(ec, "MkDirIfNotExist error, [%s]", path.c_str());
            Set(path);
        }
    }
}

bool DirOperationCache::Get(const string& path)
{
    string normalizedPath = PathUtil::NormalizeDir(path);
    ScopedLock lock(_dirSetMutex);
    return _dirs.find(normalizedPath) != _dirs.end();
}

void DirOperationCache::Set(const string& path)
{
    string normalizedPath = PathUtil::NormalizeDir(path);
    ScopedLock lock(_dirSetMutex);
    _dirs.insert(normalizedPath);
}

string DirOperationCache::GetParent(const string& path)
{
    vector<string> dirs = StringUtil::split(path, "/", false);
    dirs.pop_back();
    return StringUtil::toString(dirs, "/");
}
}} // namespace indexlib::file_system
