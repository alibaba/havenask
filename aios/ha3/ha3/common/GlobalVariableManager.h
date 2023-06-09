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

#include <stddef.h>
#include <map>
#include <string>
#include <memory>
#include <type_traits>
#include <utility>

#include "matchdoc/Trait.h"

#include "ha3/common/AttributeItem.h"
#include "autil/Log.h" // IWYU pragma: keep

namespace isearch {
namespace common {

class GlobalVariableManager
{
public:
    GlobalVariableManager();
    ~GlobalVariableManager();
private:
    GlobalVariableManager(const GlobalVariableManager &);
    GlobalVariableManager& operator=(const GlobalVariableManager &);
public:
    template<typename T>
    T* declareGlobalVariable(const std::string &variName,
                             bool needSerialize = false);

    template<typename T>
    T* findGlobalVariable(const std::string &variName) const;

    AttributeItemMapPtr getNeedSerializeGlobalVariables() const;

private:
    typedef std::map<std::string, std::pair<bool, AttributeItemPtr> > GlobalVariableMap;
private:
    GlobalVariableMap _globalVariableMap;
private:
    AUTIL_LOG_DECLARE();
};

typedef std::shared_ptr<GlobalVariableManager> GlobalVariableManagerPtr;
//////////////////////////////////////////////////////////////////
template<typename T>
inline T *GlobalVariableManager::declareGlobalVariable(const std::string &variName,
        bool needSerialize)
{
    if (needSerialize && std::is_same<typename matchdoc::ToStringTypeTraits<T>::Type,
            matchdoc::ToStringType>::value)
    {
        return NULL;
    }
    std::pair<bool, AttributeItemPtr> &v = _globalVariableMap[variName];
    if (v.second != NULL) {
        AttributeItemTyped<T> *p = dynamic_cast<AttributeItemTyped<T> * >(v.second.get());
        if (p) {
            v.first = v.first || needSerialize;
            return p->getPointer();
        } else {
            return NULL;
        }
    } else {
        AttributeItemTyped<T> *p = new AttributeItemTyped<T>(T());
        v.first = needSerialize;
        v.second.reset(p);
        return p->getPointer();
    }
}
template<typename T>
inline T* GlobalVariableManager::findGlobalVariable(const std::string &variName) const {
    GlobalVariableMap::const_iterator it = _globalVariableMap.find(variName);
    if (it == _globalVariableMap.end()) {
        return NULL;
    }
    AttributeItemTyped<T> *p =
        dynamic_cast<AttributeItemTyped<T> * >(it->second.second.get());
    if (!p) {
        return NULL;
    }
    return p->getPointer();
}
} // namespace common
} // namespace isearch
