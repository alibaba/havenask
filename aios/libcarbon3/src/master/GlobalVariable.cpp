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
#include "master/GlobalVariable.h"

using namespace std;
using namespace autil;
BEGIN_CARBON_NAMESPACE(master);

CARBON_LOG_SETUP(master, GlobalVariable);

CarbonInfo GlobalVariable::_carbonInfo;
ReadWriteLock GlobalVariable::_rwLock;;

GlobalVariable::GlobalVariable() {
}

GlobalVariable::~GlobalVariable() {
}

void GlobalVariable::setCarbonInfo(const CarbonInfo &carbonInfo) {
    ScopedWriteLock lock(_rwLock);
    _carbonInfo = carbonInfo;
}
    
CarbonInfo GlobalVariable::getCarbonInfo() {
    ScopedReadLock lock(_rwLock);
    return _carbonInfo;
}

const string GlobalVariable::genUniqIdentifier(const nodeid_t &nodeId) {
    ScopedReadLock lock(_rwLock);
    return _carbonInfo.hippoZk + ':' + _carbonInfo.appId + ':' + nodeId;
}

END_CARBON_NAMESPACE(master);

