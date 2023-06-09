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

#include <functional>
#include <memory>

#include "indexlib/file_system/file/FileWriterImpl.h"
#include "indexlib/file_system/file/ResourceFileNode.h"

namespace indexlib { namespace file_system {
class FileSystemMetricsReporter;

class ResourceFile
{
public:
    ResourceFile(std::shared_ptr<ResourceFileNode> fileNode, FileWriterImpl::UpdateFileSizeFunc updateFileSizeFunc)
        : _resourceFileNode(std::move(fileNode))
        , _updateFileSizeFunc(std::move(updateFileSizeFunc))
    {
    }

public:
    // called on creation
    void SetFileNodeCleanCallback(std::function<void()>&& callback);
    void UpdateFileLengthMetric(FileSystemMetricsReporter* reporter);
    void SetDirty(bool isDirty);

public:
    using HandleType = ResourceFileNode::HandleType;
    HandleType GetResourceHandle() const
    {
        if (_resourceFileNode) {
            return _resourceFileNode->GetResourceHandle();
        }
        return nullptr;
    }
    template <typename T>
    T* GetResource();
    template <typename T>
    void Reset(T* resource);
    template <typename T>
    void Reset(T* resource, std::function<void(T*)> destroyFunction);
    bool Empty() const;
    void UpdateMemoryUse(int64_t currentBytes);
    int64_t GetMemoryUse() const
    {
        if (_resourceFileNode) {
            return _resourceFileNode->GetMemoryUse();
        }
        return 0;
    }
    std::string GetLogicalPath() const
    {
        return _resourceFileNode != nullptr ? _resourceFileNode->GetLogicalPath() : std::string("");
    }

    std::string GetPhysicalPath() const
    {
        return _resourceFileNode != nullptr ? _resourceFileNode->GetPhysicalPath() : std::string("");
    }
    size_t GetLength() const { return _resourceFileNode != nullptr ? _resourceFileNode->GetLength() : 0; }

private:
    std::shared_ptr<ResourceFileNode> _resourceFileNode;
    FileWriterImpl::UpdateFileSizeFunc _updateFileSizeFunc;
};

/////////////////////////////////////////////////////////////////

template <typename T>
inline T* ResourceFile::GetResource()
{
    if (_resourceFileNode) {
        auto resource = _resourceFileNode->GetResource();
        return reinterpret_cast<T*>(resource);
    }
    return nullptr;
}

template <typename T>
inline void ResourceFile::Reset(T* resource, std::function<void(T*)> destroyFunction)
{
    if (_resourceFileNode) {
        _resourceFileNode->Reset(resource, [destroyFn = std::move(destroyFunction)](void* resource) {
            destroyFn(reinterpret_cast<T*>(resource));
        });
    } else {
        destroyFunction(resource);
    }
}

template <typename T>
inline void ResourceFile::Reset(T* resource)
{
    return Reset<T>(resource, [](T* resource) {
        if (resource) {
            delete resource;
        }
    });
}

inline bool ResourceFile::Empty() const { return !_resourceFileNode || _resourceFileNode->Empty(); }

inline void ResourceFile::UpdateMemoryUse(int64_t currentBytes)
{
    if (_resourceFileNode) {
        _resourceFileNode->UpdateMemoryUse(currentBytes);
    }
    if (_updateFileSizeFunc) {
        _updateFileSizeFunc(currentBytes);
    }
}

typedef std::shared_ptr<ResourceFile> ResourceFilePtr;
}} // namespace indexlib::file_system
