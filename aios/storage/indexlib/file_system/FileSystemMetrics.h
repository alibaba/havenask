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
#include <memory>

#include "autil/Log.h"
#include "indexlib/file_system/FileSystemDefine.h"
#include "indexlib/file_system/StorageMetrics.h"

namespace indexlib { namespace file_system {

class FileSystemMetrics
{
public:
    FileSystemMetrics(const StorageMetrics& inputStorageMetrics, const StorageMetrics& outputStorageMetrics);
    FileSystemMetrics();
    ~FileSystemMetrics();

public:
    const StorageMetrics& GetInputStorageMetrics() const { return _inputStorageMetrics; }
    const StorageMetrics& GetOutputStorageMetrics() const { return _outputStorageMetrics; }
    const StorageMetrics& TEST_GetStorageMetrics(FSStorageType storageType) const
    {
        assert(storageType == FSST_DISK || storageType == FSST_MEM);
        return storageType == FSST_DISK ? _inputStorageMetrics : _outputStorageMetrics;
    }

private:
    StorageMetrics _inputStorageMetrics;
    StorageMetrics _outputStorageMetrics;

private:
    AUTIL_LOG_DECLARE();
};

typedef std::shared_ptr<FileSystemMetrics> FileSystemMetricsPtr;
}} // namespace indexlib::file_system
