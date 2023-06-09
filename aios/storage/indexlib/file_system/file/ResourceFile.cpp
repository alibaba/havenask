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
#include "indexlib/file_system/file/ResourceFile.h"

namespace indexlib { namespace file_system {

void ResourceFile::SetFileNodeCleanCallback(std::function<void()>&& callback)
{
    if (_resourceFileNode) {
        _resourceFileNode->SetCleanCallback(std::move(callback));
    }
}

void ResourceFile::SetDirty(bool isDirty)
{
    if (_resourceFileNode) {
        _resourceFileNode->SetDirty(isDirty);
    }
}

void ResourceFile::UpdateFileLengthMetric(FileSystemMetricsReporter* reporter)
{
    if (_resourceFileNode) {
        _resourceFileNode->UpdateFileLengthMetric(reporter);
    }
}

}} // namespace indexlib::file_system
