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
#include "ha3/common/DataProvider.h"

#include "ha3/common/AttributeItem.h"
#include "ha3/common/GlobalVariableManager.h"
#include "autil/Log.h"

namespace isearch {
namespace common {
AUTIL_LOG_SETUP(ha3, DataProvider);

DataProvider::DataProvider(const DataProvider &other) {
    this->_globalVariableMgr = other._globalVariableMgr;
    this->_isSubScope = other._isSubScope;
}

DataProvider& DataProvider::operator=(const DataProvider &other) {
    this->_globalVariableMgr = other._globalVariableMgr;
    this->_isSubScope = other._isSubScope;
    return *this;
}

DataProvider::DataProvider()
    : _isSubScope(false)
{
    _globalVariableMgr.reset(new GlobalVariableManager());
}

DataProvider::DataProvider(const GlobalVariableManagerPtr &globalVariableManagerPtr)
    : _globalVariableMgr(globalVariableManagerPtr)
    , _isSubScope(false)
{
}

DataProvider::~DataProvider() {
}

AttributeItemMapPtr DataProvider::getNeedSerializeGlobalVariables() const {
    return _globalVariableMgr->getNeedSerializeGlobalVariables();
}

} // namespace common
} // namespace isearch
