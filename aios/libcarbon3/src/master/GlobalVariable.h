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
#ifndef CARBON_GLOBALVARIABLE_H
#define CARBON_GLOBALVARIABLE_H

#include "common/common.h"
#include "carbon/Log.h"
#include "carbon/Status.h"
#include "autil/Lock.h"

BEGIN_CARBON_NAMESPACE(master);

class GlobalVariable
{
public:
    static void setCarbonInfo(const CarbonInfo &carbonInfo);
    static CarbonInfo getCarbonInfo();
    static const std::string genUniqIdentifier(const nodeid_t &nodeId);
    ~GlobalVariable();
private:
    GlobalVariable();
    GlobalVariable(const GlobalVariable &);
    GlobalVariable& operator=(const GlobalVariable &);

public:
    static CarbonInfo _carbonInfo;
    static autil::ReadWriteLock _rwLock;
private:
    CARBON_LOG_DECLARE();
};

CARBON_TYPEDEF_PTR(GlobalVariable);

END_CARBON_NAMESPACE(master);

#endif //CARBON_GLOBALVARIABLE_H
