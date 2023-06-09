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
#include "indexlib/file_system/file/BlockFileNodeCreator.h"

#include <assert.h>
#include <iosfwd>
#include <memory>
#include <string>

#include "alog/Logger.h"
#include "autil/CommonMacros.h"
#include "autil/Log.h"
#include "indexlib/file_system/FileBlockCache.h"
#include "indexlib/file_system/FileSystemDefine.h"
#include "indexlib/file_system/file/BlockFileNode.h"
#include "indexlib/file_system/file/FileNode.h"
#include "indexlib/file_system/load_config/CacheLoadStrategy.h"
#include "indexlib/file_system/load_config/LoadConfig.h"
#include "indexlib/util/byte_slice_list/ByteSliceList.h"
#include "indexlib/util/cache/BlockCache.h"
#include "indexlib/util/memory_control/BlockMemoryQuotaController.h"
#include "indexlib/util/memory_control/SimpleMemoryQuotaController.h"

using namespace std;
using namespace autil;

using namespace indexlib::util;

namespace indexlib { namespace file_system {
AUTIL_LOG_SETUP(indexlib.file_system, BlockFileNodeCreator);

BlockFileNodeCreator::BlockFileNodeCreator(FileBlockCacheContainerPtr container,
                                           const std::string& fileSystemIdentifier)
    : _useDirectIO(false)
    , _cacheDecompressFile(false)
    , _useHighPriority(false)
    , _fileBlockCacheContainer(container)
    , _fileSystemIdentifier(fileSystemIdentifier)
{
    if (!_fileBlockCacheContainer) {
        _fileBlockCacheContainer = FileBlockCacheContainerPtr(new FileBlockCacheContainer());
        util::MemoryQuotaControllerPtr controller(new util::MemoryQuotaController(std::numeric_limits<int64_t>::max()));
        _fileBlockCacheContainer->Init("", controller);
    }
}

BlockFileNodeCreator::~BlockFileNodeCreator()
{
    if (_fileBlockCache && _fileBlockCache->GetBlockCache()) {
        AUTIL_LOG(DEBUG, "cache [%s] max block count [%u], used block count [%u]", _loadConfig.GetName().c_str(),
                  _fileBlockCache->GetBlockCache()->GetMaxBlockCount(),
                  _fileBlockCache->GetBlockCache()->GetBlockCount());
    }
    CacheLoadStrategyPtr loadStrategy = std::dynamic_pointer_cast<CacheLoadStrategy>(_loadConfig.GetLoadStrategy());
    assert(loadStrategy);
    if (!loadStrategy->UseGlobalCache() && _fileBlockCacheContainer && _fileBlockCache) {
        _fileBlockCacheContainer->UnregisterLocalCache(_fileSystemIdentifier, _loadConfig.GetName());
    }
}

bool BlockFileNodeCreator::Init(const LoadConfig& loadConfig, const util::BlockMemoryQuotaControllerPtr& memController)
{
    _loadConfig = loadConfig;
    _memController = memController;

    CacheLoadStrategyPtr loadStrategy = std::dynamic_pointer_cast<CacheLoadStrategy>(loadConfig.GetLoadStrategy());
    assert(loadStrategy);
    _useDirectIO = loadStrategy->UseDirectIO();
    _cacheDecompressFile = loadStrategy->CacheDecompressFile();
    _useHighPriority = loadStrategy->UseHighPriority();
    if (loadStrategy->UseGlobalCache()) {
        if (_fileBlockCacheContainer) {
            _fileBlockCache = _fileBlockCacheContainer->GetAvailableFileCache(loadConfig.GetLifecycle());
        }
        if (_fileBlockCache) {
            return true;
        }
        AUTIL_LOG(ERROR, "load config[%s] need global file block cache", loadConfig.GetName().c_str());
        return false;
    }

    assert(!_fileBlockCache);

    util::SimpleMemoryQuotaControllerPtr quotaController(new util::SimpleMemoryQuotaController(_memController));
    _fileBlockCache = _fileBlockCacheContainer->RegisterLocalCache(
        _fileSystemIdentifier, loadConfig.GetName(), loadStrategy->GetBlockCacheOption(), quotaController);
    return _fileBlockCache.get() != nullptr;
}

std::shared_ptr<FileNode> BlockFileNodeCreator::CreateFileNode(FSOpenType type, bool readOnly,
                                                               const std::string& linkRoot)
{
    assert(type == FSOT_CACHE || type == FSOT_LOAD_CONFIG);
    assert(_fileBlockCache);
    util::BlockCache* blockCache = _fileBlockCache->GetBlockCache().get();
    assert(blockCache);

    AUTIL_LOG(DEBUG, "cache [%s] max block count [%u], used block count [%u], block size [%lu], link root [%s]",
              _loadConfig.GetName().c_str(), blockCache->GetMaxBlockCount(), blockCache->GetBlockCount(),
              blockCache->GetBlockSize(), linkRoot.c_str());

    std::shared_ptr<BlockFileNode> blockFileNode(
        new BlockFileNode(blockCache, _useDirectIO, _cacheDecompressFile, _useHighPriority, linkRoot));
    return blockFileNode;
}

bool BlockFileNodeCreator::Match(const string& filePath, const string& lifecycle) const
{
    return _loadConfig.Match(filePath, lifecycle);
}

bool BlockFileNodeCreator::MatchType(FSOpenType type) const { return (type == FSOT_CACHE || type == FSOT_LOAD_CONFIG); }

bool BlockFileNodeCreator::IsRemote() const { return _loadConfig.IsRemote(); }
}} // namespace indexlib::file_system
