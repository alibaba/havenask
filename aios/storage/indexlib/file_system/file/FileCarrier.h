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

#include <assert.h>

#include "autil/Log.h"

namespace indexlib { namespace file_system {
class MmapFileNode;

class FileCarrier
{
public:
    FileCarrier() = default;
    ~FileCarrier() = default;

public:
    std::shared_ptr<MmapFileNode> GetMmapLockFileNode() { return _mmapLockFile; }
    void SetMmapLockFileNode(const std::shared_ptr<MmapFileNode>& fileNode)
    {
        assert(fileNode);
        _mmapLockFile = fileNode;
    }
    inline long GetMmapLockFileNodeUseCount() const { return _mmapLockFile.use_count(); }

private:
    std::shared_ptr<MmapFileNode> _mmapLockFile;

private:
    AUTIL_LOG_DECLARE();
};

typedef std::shared_ptr<FileCarrier> FileCarrierPtr;
}} // namespace indexlib::file_system
