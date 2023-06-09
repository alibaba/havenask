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

#include <map>
#include <string>
#include <memory>
#include <utility>

#include "ha3/common/AttributeItem.h"
#include "ha3/common/GlobalVariableManager.h"
#include "autil/Log.h" // IWYU pragma: keep


namespace isearch {
namespace rank {
} // namespace rank
} // namespace isearch

namespace isearch {
namespace search {
} // namespace search
} // namespace isearch

namespace isearch {
namespace common {

class DataProvider
{
public:
    DataProvider();
    DataProvider(const GlobalVariableManagerPtr &globalVariableManagerPtr);
    ~DataProvider();
    DataProvider(const DataProvider &other);
    DataProvider& operator=(const DataProvider &other);

    template<typename T>
    T* declareGlobalVariable(const std::string &variName,
                             bool needSerialize = false);

    template<typename T>
    T* findGlobalVariable(const std::string &variName) const;

    AttributeItemMapPtr getNeedSerializeGlobalVariables() const;

    bool isSubScope() const {
        return _isSubScope;
    }

    void setSubScope(bool isSubScope) {
        _isSubScope = isSubScope;
    }

    void unsetSubScope() {
        _isSubScope = false;
    }

    void setGlobalVariableManager(const common::GlobalVariableManagerPtr &globalVariableManagerPtr) {
        _globalVariableMgr = globalVariableManagerPtr;
    }

    GlobalVariableManagerPtr getGlobalVariableManager() {
        return _globalVariableMgr;
    }
private:
    typedef std::map<std::string, std::pair<bool, AttributeItemPtr> > GlobalVariableMap;
private:
    //GlobalVariableMap _globalVariableMap;
    GlobalVariableManagerPtr _globalVariableMgr;
    bool _isSubScope;
private:
    AUTIL_LOG_DECLARE();
};

typedef std::shared_ptr<DataProvider> DataProviderPtr;
//////////////////////////////////////////////////////////////////

template<typename T>
inline T *DataProvider::declareGlobalVariable(const std::string &variName,
        bool needSerialize)
{
    return _globalVariableMgr->declareGlobalVariable<T>(variName, needSerialize);
}
template<typename T>
inline T* DataProvider::findGlobalVariable(const std::string &variName) const {
    return _globalVariableMgr->findGlobalVariable<T>(variName);
}
} // namespace common
} // namespace isearch

