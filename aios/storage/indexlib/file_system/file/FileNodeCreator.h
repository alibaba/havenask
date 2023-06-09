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
#include <stddef.h>
#include <string>

#include "autil/Log.h"
#include "indexlib/file_system/FileSystemDefine.h"
#include "indexlib/file_system/file/FileNode.h"
#include "indexlib/util/memory_control/BlockMemoryQuotaController.h"

namespace indexlib { namespace file_system {
class LoadConfig;
}} // namespace indexlib::file_system

namespace indexlib { namespace file_system {

class FileNodeCreator
{
public:
    FileNodeCreator();
    virtual ~FileNodeCreator();

public:
    virtual bool Init(const LoadConfig& loadConfig, const util::BlockMemoryQuotaControllerPtr& memController) = 0;
    virtual std::shared_ptr<FileNode> CreateFileNode(FSOpenType type, bool readOnly, const std::string& linkRoot) = 0;

    virtual bool Match(const std::string& filePath, const std::string& lifecycle) const = 0;
    virtual bool MatchType(FSOpenType type) const = 0;
    virtual bool IsRemote() const { return false; }

    // just FSOT_LOAD_CONFIG used
    virtual FSOpenType GetDefaultOpenType() const
    {
        assert(false);
        return FSOT_UNKNOWN;
    }
    virtual size_t EstimateFileLockMemoryUse(size_t fileLength) const noexcept = 0;

protected:
    util::BlockMemoryQuotaControllerPtr _memController;

private:
    AUTIL_LOG_DECLARE();
};

typedef std::shared_ptr<FileNodeCreator> FileNodeCreatorPtr;
}} // namespace indexlib::file_system
