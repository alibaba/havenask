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
#ifndef ISEARCH_BS_RESOURCECONTAINER_H
#define ISEARCH_BS_RESOURCECONTAINER_H

#include "autil/Lock.h"
#include "build_service/common_define.h"
#include "build_service/util/Log.h"

namespace build_service { namespace common {

class TaskResourceItem
{
public:
    TaskResourceItem() {}
    virtual ~TaskResourceItem() {}
};
BS_TYPEDEF_PTR(TaskResourceItem);

template <typename T>
class TaskResourceItemTyped : public TaskResourceItem
{
public:
    TaskResourceItemTyped(const T& res) : mResource(res) {}
    const T& getResource() const { return mResource; }

private:
    T mResource;
};

//////////////////////////////////////////
class ResourceContainer
{
public:
    ResourceContainer();
    ~ResourceContainer();

private:
    ResourceContainer(const ResourceContainer&);
    ResourceContainer& operator=(const ResourceContainer&);

public:
    template <typename T>
    bool addResource(const std::string& resId, const T& res);

    template <typename T>
    bool addResource(const T& res);

    template <typename T>
    bool getResource(T& res);

    template <typename T>
    bool getResource(const std::string& resId, T& res);
    void setLogPrefix(const std::string& str) { _logPrefix = str; }
    const std::string& getLogPrefix() const { return _logPrefix; }

public:
    // for test
    template <typename T>
    void addResourceIgnoreExist(const T& res);

private:
    typedef std::map<std::string, TaskResourceItemPtr> ResourceMap;
    ResourceMap _resourceMap;
    mutable autil::ThreadMutex _lock;
    std::string _logPrefix;

private:
    BS_LOG_DECLARE();
};

BS_TYPEDEF_PTR(ResourceContainer);

///////////////////////////////////////////////////////////////////////////
template <typename T>
inline bool ResourceContainer::addResource(const T& res)
{
    return addResource(typeid(T).name(), res);
}

template <typename T>
inline bool ResourceContainer::getResource(T& res)
{
    return getResource(typeid(T).name(), res);
}

#define LOG_PREFIX _logPrefix.c_str()

template <typename T>
inline bool ResourceContainer::addResource(const std::string& resId, const T& res)
{
    autil::ScopedLock lock(_lock);
    ResourceMap::const_iterator iter = _resourceMap.find(resId);
    if (iter != _resourceMap.end()) {
        BS_PREFIX_LOG(ERROR, "resource id [%s] already exist!", resId.c_str());
        return false;
    }

    TaskResourceItemPtr item(new TaskResourceItemTyped<T>(res));
    _resourceMap.insert(std::make_pair(resId, item));
    return true;
}

template <typename T>
inline void ResourceContainer::addResourceIgnoreExist(const T& res)
{
    std::string resId = typeid(T).name();
    TaskResourceItemPtr item(new TaskResourceItemTyped<T>(res));
    autil::ScopedLock lock(_lock);
    _resourceMap[resId] = item;
}

template <typename T>
inline bool ResourceContainer::getResource(const std::string& resId, T& res)
{
    autil::ScopedLock lock(_lock);
    ResourceMap::const_iterator iter = _resourceMap.find(resId);
    if (iter == _resourceMap.end()) {
        return false;
    }

    TaskResourceItemTyped<T>* item = dynamic_cast<TaskResourceItemTyped<T>*>(iter->second.get());
    if (item == NULL) {
        BS_PREFIX_LOG(ERROR, "resource Id [%s] not match typeid [%s]", resId.c_str(), typeid(T).name());
        return false;
    }
    res = item->getResource();
    return true;
}

#undef LOG_PREFIX

}}     // namespace build_service::common
#endif // ISEARCH_BS_RESOURCECONTAINER_H
