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
#include "indexlib/file_system/file/MemFileNodeCreator.h"

#include <assert.h>
#include <cstddef>

#include "fslib/common/common_type.h"

using namespace std;

namespace indexlib { namespace file_system {
AUTIL_LOG_SETUP(indexlib.file_system, MemFileNodeCreator);

MemFileNodeCreator::MemFileNodeCreator(SessionFileCachePtr fileCache) : _fileCache(fileCache) {}

MemFileNodeCreator::~MemFileNodeCreator() {}

bool MemFileNodeCreator::Init(const LoadConfig& loadConfig, const util::BlockMemoryQuotaControllerPtr& memController)
{
    _memController = memController;
    _loadConfig = loadConfig;
    return true;
}

std::shared_ptr<FileNode> MemFileNodeCreator::CreateFileNode(FSOpenType type, bool readOnly,
                                                             const std::string& linkRoot)
{
    assert(type == FSOT_MEM || type == FSOT_LOAD_CONFIG || type == FSOT_MMAP);

    std::shared_ptr<MemFileNode> inMemFileNode(new MemFileNode(0, true, _loadConfig, _memController, _fileCache));
    return inMemFileNode;
}

bool MemFileNodeCreator::Match(const string& filePath, const string& lifecycle) const
{
    return _loadConfig.Match(filePath, lifecycle);
}

bool MemFileNodeCreator::MatchType(FSOpenType type) const { return (type == FSOT_MEM || type == FSOT_LOAD_CONFIG); }

bool MemFileNodeCreator::IsRemote() const { return _loadConfig.IsRemote(); }

size_t MemFileNodeCreator::EstimateFileLockMemoryUse(size_t fileLength) const noexcept { return fileLength; }
}} // namespace indexlib::file_system
