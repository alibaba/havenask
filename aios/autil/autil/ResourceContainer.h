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

#include "autil/Lock.h"
#include "autil/Log.h"

namespace autil {

class TaskResourceItem {
public:
    TaskResourceItem() {}
    virtual ~TaskResourceItem() {}
};

template <typename T>
class TaskResourceItemTyped : public TaskResourceItem {
public:
    TaskResourceItemTyped(const T &res) : _resource(res) {}
    const T &getResource() const { return _resource; }

private:
    T _resource;
};

//////////////////////////////////////////
class ResourceContainer {
public:
    ResourceContainer() = default;
    ~ResourceContainer() = default;

private:
    ResourceContainer(const ResourceContainer &);
    ResourceContainer &operator=(const ResourceContainer &);

public:
    template <typename T>
    bool addResource(const std::string &resId, const T &res);

    template <typename T>
    bool addResource(const T &res);

    template <typename T>
    bool getResource(T &res) const;

    template <typename T>
    bool getResource(const std::string &resId, T &res) const;
    void setLogPrefix(const std::string &str) { _logPrefix = str; }
    const std::string &getLogPrefix() const { return _logPrefix; }

public:
    // for test
    template <typename T>
    void addResourceIgnoreExist(const T &res);

private:
    std::map<std::string, std::shared_ptr<TaskResourceItem>> _resourceMap;
    mutable autil::ThreadMutex _lock;
    std::string _logPrefix;

private:
    AUTIL_LOG_DECLARE();
};

inline AUTIL_LOG_SETUP(autil, ResourceContainer);

template <typename T>
inline bool ResourceContainer::addResource(const T &res) {
    return addResource(typeid(T).name(), res);
}

template <typename T>
inline bool ResourceContainer::getResource(T &res) const {
    return getResource(typeid(T).name(), res);
}

template <typename T>
inline bool ResourceContainer::addResource(const std::string &resId, const T &res) {
    autil::ScopedLock lock(_lock);
    if (_resourceMap.find(resId) != _resourceMap.end()) {
        AUTIL_LOG(ERROR, "[%s] resource id [%s] already exist!", _logPrefix.c_str(), resId.c_str());
        return false;
    }
    _resourceMap[resId] = std::make_shared<TaskResourceItemTyped<T>>(res);
    return true;
}

template <typename T>
inline void ResourceContainer::addResourceIgnoreExist(const T &res) {
    std::string resId = typeid(T).name();
    autil::ScopedLock lock(_lock);
    _resourceMap[resId] = std::make_shared<TaskResourceItemTyped<T>>(res);
}

template <typename T>
inline bool ResourceContainer::getResource(const std::string &resId, T &res) const {
    autil::ScopedLock lock(_lock);
    auto iter = _resourceMap.find(resId);
    if (iter == _resourceMap.end()) {
        return false;
    }

    TaskResourceItemTyped<T> *item = dynamic_cast<TaskResourceItemTyped<T> *>(iter->second.get());
    if (item == NULL) {
        AUTIL_LOG(
            ERROR, "[%s] resource Id [%s] not match typeid [%s]", _logPrefix.c_str(), resId.c_str(), typeid(T).name());
        return false;
    }
    res = item->getResource();
    return true;
}

} // namespace autil
