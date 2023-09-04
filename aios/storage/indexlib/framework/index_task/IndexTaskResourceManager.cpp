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
#include "indexlib/framework/index_task/IndexTaskResourceManager.h"

#include "indexlib/file_system/Directory.h"
#include "indexlib/file_system/IDirectory.h"

using namespace std;

namespace indexlibv2::framework {
AUTIL_LOG_SETUP(indexlib.framework, IndexTaskResourceManager);

Status IndexTaskResourceManager::Init(const std::shared_ptr<indexlib::file_system::Directory>& resourceRoot,
                                      const std::string& workPrefix, std::shared_ptr<IIndexTaskResourceCreator> creator)
{
    assert(resourceRoot);
    std::lock_guard<std::mutex> lock(_mutex);
    _resourceRoot = resourceRoot;
    _workPrefix = workPrefix;
    _creator = std::move(creator);
    _resources.clear();
    AUTIL_LOG(INFO, "resource manager created, resource root[%s], work prefix[%s]",
              resourceRoot->GetLogicalPath().c_str(), workPrefix.c_str());
    return Status::OK();
}

std::pair<Status, std::shared_ptr<IndexTaskResource>>
IndexTaskResourceManager::LoadResource(const std::string& name, const IndexTaskResourceType& type)
{
    {
        std::lock_guard<std::mutex> lock(_mutex);
        auto iter = _resources.find(name);
        if (iter != _resources.end()) {
            auto resource = iter->second;
            if (resource->GetType() != type) {
                AUTIL_LOG(ERROR, "required type[%s] mismatch with previous loaded type[%s]", type.c_str(),
                          resource->GetType().c_str());
                return {Status::Corruption("type mismatch"), nullptr};
            }
            return {Status::OK(), resource};
        }
    }
    auto linkFileName = GetLinkFileName(name);
    auto [status, isExist] = _resourceRoot->GetIDirectory()->IsExist(linkFileName).StatusWith();
    RETURN2_IF_STATUS_ERROR(status, nullptr, "is exist [%s] failed", linkFileName.c_str());
    if (!isExist) {
        AUTIL_LOG(INFO, "link file [%s/%s] not exist", _resourceRoot->GetLogicalPath().c_str(), linkFileName.c_str());
        return std::make_pair(Status::NotFound(linkFileName.c_str()), nullptr);
    }

    std::string targetWorkPrefix;
    status =
        _resourceRoot->GetIDirectory()
            ->Load(linkFileName, indexlib::file_system::ReaderOption(indexlib::file_system::FSOT_MEM), targetWorkPrefix)
            .Status();
    RETURN2_IF_STATUS_ERROR(status, nullptr, "load [%s] failed", linkFileName.c_str());

    std::shared_ptr<indexlib::file_system::IDirectory> targetWorkDir;
    std::tie(status, targetWorkDir) = _resourceRoot->GetIDirectory()->GetDirectory(targetWorkPrefix).StatusWith();
    RETURN2_IF_STATUS_ERROR(status, nullptr, "get directory [%s] failed", targetWorkPrefix.c_str());

    auto resourceDirName = GetResourceDirName(name);
    std::shared_ptr<indexlib::file_system::IDirectory> resourceDir;
    std::tie(status, resourceDir) = targetWorkDir->GetDirectory(resourceDirName).StatusWith();
    RETURN2_IF_STATUS_ERROR(status, nullptr, "get resource directory [%s] failed", resourceDirName.c_str());
    auto resource = _creator->CreateResource(name, type);
    if (!resource) {
        AUTIL_LOG(ERROR, "create resource name[%s] type[%s] failed", name.c_str(), type.c_str());
        return {Status::Corruption(type.c_str()), nullptr};
    }
    status = resource->Load(indexlib::file_system::IDirectory::ToLegacyDirectory(resourceDir));
    RETURN2_IF_STATUS_ERROR(status, nullptr, "load resource name[%s] type[%s] failed", name.c_str(), type.c_str());

    std::lock_guard<std::mutex> lock(_mutex);
    std::shared_ptr<IndexTaskResource> entry(resource.release());
    _resources[name] = entry;
    return std::make_pair(Status::OK(), entry);
}

Status IndexTaskResourceManager::AddExtendResource(const std::shared_ptr<IndexTaskResource>& extendResource)
{
    std::lock_guard<std::mutex> lock(_mutex);
    std::string type = extendResource->GetType();
    if (type != framework::ExtendResourceType) {
        AUTIL_LOG(ERROR, "add extend resource failed, [%s] not ExtendResource type", type.c_str());
        return Status::Corruption();
    }
    std::string name = extendResource->GetName();
    auto iter = _extendResources.find(name);
    if (iter != _extendResources.end()) {
        AUTIL_LOG(ERROR, "add extend resource failed, [%s] exist", name.c_str());
        return Status::Exist();
    }
    _extendResources[name] = extendResource;
    return Status::OK();
}

std::map<std::string, std::shared_ptr<IndexTaskResource>> IndexTaskResourceManager::TEST_GetExtendResources() const
{
    std::lock_guard<std::mutex> lock(_mutex);
    return _extendResources;
}

std::pair<Status, std::shared_ptr<IndexTaskResource>>
IndexTaskResourceManager::CreateResource(const std::string& name, const IndexTaskResourceType& type,
                                         IIndexTaskResourceCreator* creator)
{
    // TODO: optimize: return failed if resource already exist on disk
    std::lock_guard<std::mutex> lock(_mutex);
    if (_resources.find(name) != _resources.end()) {
        AUTIL_LOG(ERROR, "resource [%s] already exist", name.c_str());
        return {Status::Exist(name.c_str()), nullptr};
    }
    auto resource = creator->CreateResource(name, type);
    if (!resource) {
        AUTIL_LOG(ERROR, "create resource name[%s] type[%s] failed", name.c_str(), type.c_str());
        return {Status::Corruption(type.c_str()), nullptr};
    }
    std::shared_ptr<IndexTaskResource> entry(resource.release());
    _resources[name] = entry;
    return std::make_pair(Status::OK(), entry);
}

Status IndexTaskResourceManager::CommitResource(const std::string& name)
{
    auto linkFileName = GetLinkFileName(name);
    auto [existStatus, exist] = _resourceRoot->GetIDirectory()->IsExist(linkFileName).StatusWith();
    RETURN_IF_STATUS_ERROR(existStatus, "check resource exist failed");
    if (exist) {
        AUTIL_LOG(WARN, "resource [%s] already committed, return ok directly", name.c_str());
        return Status::OK();
    }

    std::shared_ptr<IndexTaskResource> resource;
    {
        std::lock_guard<std::mutex> lock(_mutex);
        auto iter = _resources.find(name);
        if (iter == _resources.end()) {
            AUTIL_LOG(ERROR, "resource [%s] not exist", name.c_str());
            return Status::NotFound(name.c_str());
        }
        resource = iter->second;
    }
    auto [status, workDir] = _resourceRoot->GetIDirectory()
                                 ->MakeDirectory(_workPrefix, indexlib::file_system::DirectoryOption())
                                 .StatusWith();
    RETURN_IF_STATUS_ERROR(status, "create directory[%s] failed", _workPrefix.c_str());

    auto resourceDirName = GetResourceDirName(name);
    indexlib::file_system::RemoveOption removeOption = indexlib::file_system::RemoveOption::MayNonExist();
    status = workDir->RemoveDirectory(resourceDirName, /*mayNonExist*/ removeOption).Status();
    RETURN_IF_STATUS_ERROR(status, "remove [%s] failed", resourceDirName.c_str());

    std::shared_ptr<indexlib::file_system::IDirectory> resourceDir;
    std::tie(status, resourceDir) =
        workDir->MakeDirectory(resourceDirName, indexlib::file_system::DirectoryOption()).StatusWith();
    RETURN_IF_STATUS_ERROR(status, "create directory[%s] failed", resourceDirName.c_str());

    status = resource->Store(indexlib::file_system::IDirectory::ToLegacyDirectory(resourceDir));
    RETURN_IF_STATUS_ERROR(status, "store resource[%s] failed", name.c_str());

    status = workDir->RemoveFile(linkFileName, /*mayNonExist*/ removeOption).Status();
    RETURN_IF_STATUS_ERROR(status, "remove [%s] failed", linkFileName.c_str());

    std::string linkContent = _workPrefix;
    status = workDir->Store(linkFileName, linkContent, indexlib::file_system::WriterOption()).Status();
    RETURN_IF_STATUS_ERROR(status, "store [%s] failed", linkFileName.c_str());

    status = workDir->Rename(linkFileName, _resourceRoot->GetIDirectory(), /*destPath*/ "").Status();
    RETURN_IF_STATUS_ERROR(status, "rename [%s] failed", linkFileName.c_str());
    return Status::OK();
}

Status IndexTaskResourceManager::ReleaseResource(const std::string& name)
{
    std::lock_guard<std::mutex> lock(_mutex);
    auto iter = _resources.find(name);
    if (iter == _resources.end()) {
        AUTIL_LOG(ERROR, "no resource [%s] found", name.c_str());
        return Status::NotFound(name.c_str());
    }
    _resources.erase(iter);
    return Status::OK();
}

std::string IndexTaskResourceManager::GetLinkFileName(const std::string& name)
{
    // eg. bucketmap.__link__
    return name + ".__link__";
}

std::string IndexTaskResourceManager::GetResourceDirName(const std::string& name)
{
    // eg. resource__bucketmap/
    return std::string("resource__") + name + "/";
}

void IndexTaskResourceManager::TEST_AddResource(const std::shared_ptr<IndexTaskResource>& resource)
{
    _resources[resource->GetName()] = resource;
}

} // namespace indexlibv2::framework
