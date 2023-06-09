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
#include "indexlib/file_system/file/BufferedFileNodeCreator.h"

#include <assert.h>
#include <cstddef>

#include "fslib/common/common_type.h"
#include "indexlib/file_system/file/BufferedFileNode.h"

namespace indexlib { namespace file_system {
class LoadConfig;
}} // namespace indexlib::file_system

using namespace std;

namespace indexlib { namespace file_system {
AUTIL_LOG_SETUP(indexlib.file_system, BufferedFileNodeCreator);

BufferedFileNodeCreator::BufferedFileNodeCreator(SessionFileCachePtr fileCache) : _fileCache(fileCache) {}

BufferedFileNodeCreator::~BufferedFileNodeCreator() {}

bool BufferedFileNodeCreator::Init(const LoadConfig& loadConfig,
                                   const util::BlockMemoryQuotaControllerPtr& memController)
{
    _memController = memController;
    return true;
}

std::shared_ptr<FileNode> BufferedFileNodeCreator::CreateFileNode(FSOpenType type, bool readOnly,
                                                                  const std::string& linkRoot)
{
    assert(type == FSOT_BUFFERED);
    assert(readOnly);

    std::shared_ptr<FileNode> bufferedFileNode(new BufferedFileNode(fslib::READ, _fileCache));
    return bufferedFileNode;
}

bool BufferedFileNodeCreator::Match(const string& filePath, const string& lifecycle) const

{
    // load config list not support buffered file reader
    assert(false);
    return false;
}

bool BufferedFileNodeCreator::MatchType(FSOpenType type) const
{
    assert(false);
    return false;
}

size_t BufferedFileNodeCreator::EstimateFileLockMemoryUse(size_t fileLength) const noexcept { return 0; }
}} // namespace indexlib::file_system
