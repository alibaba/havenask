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
#include "indexlib/file_system/file/MmapFileNodeCreator.h"

#include <assert.h>
#include <cstddef>
#include <memory>

#include "alog/Logger.h"
#include "fslib/common/common_type.h"
#include "indexlib/file_system/file/MmapFileNode.h"
#include "indexlib/file_system/load_config/MmapLoadStrategy.h"

using namespace std;

namespace indexlib { namespace file_system {
AUTIL_LOG_SETUP(indexlib.file_system, MmapFileNodeCreator);

MmapFileNodeCreator::MmapFileNodeCreator() : _fullMemory(false) {}

MmapFileNodeCreator::~MmapFileNodeCreator() {}

bool MmapFileNodeCreator::Init(const LoadConfig& loadConfig, const util::BlockMemoryQuotaControllerPtr& memController)
{
    _memController = memController;
    _loadConfig = loadConfig;
    MmapLoadStrategyPtr loadStrategy = std::dynamic_pointer_cast<MmapLoadStrategy>(loadConfig.GetLoadStrategy());
    if (!loadStrategy) {
        AUTIL_LOG(ERROR, "Load strategy dynamic fail.");
        return false;
    }
    _lock = loadStrategy->IsLock();
    _fullMemory = _lock || IsRemote();
    return true;
}

std::shared_ptr<FileNode> MmapFileNodeCreator::CreateFileNode(FSOpenType type, bool readOnly,
                                                              const std::string& linkRoot)
{
    assert(type == FSOT_MMAP || type == FSOT_LOAD_CONFIG);
    std::shared_ptr<MmapFileNode> mmapFileNode(new MmapFileNode(_loadConfig, _memController, readOnly));
    return mmapFileNode;
}

bool MmapFileNodeCreator::Match(const string& filePath, const string& lifecycle) const
{
    return _loadConfig.Match(filePath, lifecycle);
}

bool MmapFileNodeCreator::MatchType(FSOpenType type) const { return type == FSOT_MMAP || type == FSOT_LOAD_CONFIG; }

bool MmapFileNodeCreator::IsRemote() const { return _loadConfig.IsRemote(); }

bool MmapFileNodeCreator::IsLock() const { return _lock; }

size_t MmapFileNodeCreator::EstimateFileLockMemoryUse(size_t fileLength) const noexcept
{
    return _fullMemory ? fileLength : 0;
}

}} // namespace indexlib::file_system
