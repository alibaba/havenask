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

#include <string>

#include "autil/Log.h"
#include "indexlib/file_system/FileSystemOptions.h"
#include "indexlib/file_system/IFileSystem.h"
#include "indexlib/file_system/fslib/FenceContext.h"

namespace indexlib {
namespace misc {
class MetricProvider;
}
namespace file_system {

class FileSystemCreator
{
public:
    // TODO: remove one
    static FSResult<std::shared_ptr<IFileSystem>>
    Create(const std::string& name, const std::string& outputRoot,
           const FileSystemOptions& fsOptions = FileSystemOptions(),
           const std::shared_ptr<util::MetricProvider>& metricProvider = nullptr, bool isOverride = false,
           FenceContext* fenceContext = FenceContext::NoFence()) noexcept;

    static FSResult<std::shared_ptr<IFileSystem>>
    CreateForRead(const std::string& name, const std::string& outputRoot,
                  const FileSystemOptions& fsOptions = FileSystemOptions(),
                  const std::shared_ptr<util::MetricProvider>& metricProvider = nullptr) noexcept;

    static FSResult<std::shared_ptr<IFileSystem>>
    CreateForWrite(const std::string& name, const std::string& outputRoot,
                   const FileSystemOptions& fsOptions = FileSystemOptions(),
                   const std::shared_ptr<util::MetricProvider>& metricProvider = nullptr,
                   bool isOverride = false) noexcept;

private:
    static FSResult<std::shared_ptr<IFileSystem>>
    DoCreate(const std::string& name, const std::string& outputRoot, const FileSystemOptions& fsOptions,
             const std::shared_ptr<util::MetricProvider>& metricProvider) noexcept;

private:
    AUTIL_LOG_DECLARE();
};

} // namespace file_system
} // namespace indexlib
