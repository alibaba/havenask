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

#include <memory>
#include <mutex>
#include <string>

#include "autil/Log.h"
#include "autil/NoCopyable.h"
#include "indexlib/framework/index_task/BasicDefs.h"
#include "indexlib/framework/index_task/ExtendResource.h"
#include "indexlib/framework/index_task/IIndexTaskResourceCreator.h"
#include "indexlib/framework/index_task/IndexTaskResource.h"

namespace indexlibv2::framework {

class IIndexTaskResourceCreator;

static constexpr const char* SEGMENT_DOC_READER_CREATOR = "SegmentDocReaderCreator";
static constexpr const char* ANALYZER_FACTORY = "ANALYZER_FACTORY";
static constexpr const char* INDEX_RECLAIM_PARAM_PREPARER = "INDEX_RECLAIM_PARAM_PREPARER";
static constexpr const char* DOC_READER_ITERATOR_CREATOR = "DOC_READER_ITERATOR_CREATOR";

class IndexTaskResourceManager : private autil::NoCopyable
{
public:
    IndexTaskResourceManager() {}
    ~IndexTaskResourceManager() {}

public:
    Status Init(const std::shared_ptr<indexlib::file_system::Directory>& resourceRoot, const std::string& workPrefix,
                std::shared_ptr<IIndexTaskResourceCreator> creator);

    // TODO(hanyao): add operation id
    template <typename T>
    Status LoadResource(const std::string& name, const IndexTaskResourceType& type, std::shared_ptr<T>& resource);

    template <typename T>
    Status CreateResource(const std::string& name, const IndexTaskResourceType& type, std::shared_ptr<T>& resource);
    Status CommitResource(const std::string& name);
    Status ReleaseResource(const std::string& name);
    template <typename T>
    Status AddExtendResource(const std::string& name, const std::shared_ptr<T>& extendResource);
    template <typename T>
    Status GetExtendResource(const std::string& name, std::shared_ptr<T>& resource);
    Status AddExtendResource(const std::shared_ptr<IndexTaskResource>& extendResource);
    std::map<std::string, std::shared_ptr<IndexTaskResource>> TEST_GetExtendResources() const;
    void TEST_SetExtendResources(std::map<std::string, std::shared_ptr<IndexTaskResource>>& extendResources)
    {
        std::lock_guard<std::mutex> lock(_mutex);
        _extendResources = extendResources;
    }

private:
    static std::string GetResourceDirName(const std::string& name);
    static std::string GetLinkFileName(const std::string& name);
    std::pair<Status, std::shared_ptr<IndexTaskResource>> LoadResource(const std::string& name,
                                                                       const IndexTaskResourceType& type);
    std::pair<Status, std::shared_ptr<IndexTaskResource>>
    CreateResource(const std::string& name, const IndexTaskResourceType& type, IIndexTaskResourceCreator* creator);

private:
    void TEST_AddResource(const std::shared_ptr<IndexTaskResource>& resource);

private:
    mutable std::mutex _mutex;
    std::shared_ptr<indexlib::file_system::Directory> _resourceRoot;
    std::string _workPrefix;
    std::shared_ptr<IIndexTaskResourceCreator> _creator;
    std::map<std::string, std::shared_ptr<IndexTaskResource>> _resources;
    std::map<std::string, std::shared_ptr<IndexTaskResource>> _extendResources;

private:
    AUTIL_LOG_DECLARE();
};

template <typename T>
inline Status IndexTaskResourceManager::AddExtendResource(const std::string& name,
                                                          const std::shared_ptr<T>& extendResource)
{
    std::lock_guard<std::mutex> lock(_mutex);
    auto iter = _extendResources.find(name);
    if (iter != _extendResources.end()) {
        AUTIL_LOG(WARN, "add extend resource failed, [%s] exist", name.c_str());
        return Status::Exist();
    }
    _extendResources[name] = std::make_shared<ExtendResource<T>>(name, extendResource);
    return Status::OK();
}

template <typename T>
inline Status IndexTaskResourceManager::GetExtendResource(const std::string& name, std::shared_ptr<T>& resource)
{
    std::lock_guard<std::mutex> lock(_mutex);
    auto iter = _extendResources.find(name);
    if (iter == _extendResources.end()) {
        return Status::NotFound("get extend resource [%s] failed", name.c_str());
    }

    auto extendResource = std::dynamic_pointer_cast<ExtendResource<T>>(iter->second);
    if (!extendResource) {
        auto s = Status::Corruption("convert to [%s] failed", name.c_str());
        AUTIL_LOG(ERROR, "%s", s.ToString().c_str());
        return s;
    }
    resource = extendResource->GetResource();
    return Status::OK();
}

template <typename T>
inline Status IndexTaskResourceManager::CreateResource(const std::string& name, const IndexTaskResourceType& type,
                                                       std::shared_ptr<T>& resource)
{
    assert(_creator);
    auto [status, resourceBase] = CreateResource(name, type, _creator.get());
    if (!status.IsOK()) {
        AUTIL_LOG(ERROR, "create resource [%s] failed", name.c_str());
        return status;
    }

    resource = std::dynamic_pointer_cast<T>(resourceBase);
    if (!resource) {
        AUTIL_LOG(ERROR, "convert to [%s] failed", name.c_str());
        return Status::Corruption();
    }
    return Status::OK();
}

template <typename T>
inline Status IndexTaskResourceManager::LoadResource(const std::string& name, const IndexTaskResourceType& type,
                                                     std::shared_ptr<T>& resource)
{
    auto [status, resourceBase] = LoadResource(name, type);
    if (status.IsNotFound()) {
        AUTIL_LOG(INFO, "resource [%s] not exist", name.c_str());
        return status;
    }
    if (!status.IsOK()) {
        AUTIL_LOG(ERROR, "load resource [%s] failed", name.c_str());
        return status;
    }

    resource = std::dynamic_pointer_cast<T>(resourceBase);
    if (!resource) {
        AUTIL_LOG(ERROR, "convert to [%s] failed", name.c_str());
        return Status::Corruption();
    }
    return Status::OK();
}

} // namespace indexlibv2::framework
