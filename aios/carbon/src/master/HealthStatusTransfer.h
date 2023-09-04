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
#ifndef CARBON_HEALTHSTATUSTRANSFER_H
#define CARBON_HEALTHSTATUSTRANSFER_H

#include "common/common.h"
#include "carbon/CommonDefine.h"
#include "carbon/Status.h"

BEGIN_CARBON_NAMESPACE(master);

struct CheckResult;

class HealthStatusTransfer
{
public:
    HealthStatusTransfer(int64_t lostTimeout);
    ~HealthStatusTransfer();
private:
    HealthStatusTransfer(const HealthStatusTransfer &);
    HealthStatusTransfer& operator=(const HealthStatusTransfer &);
    
public:
    bool transfer(bool touched, int64_t curTime, 
                  CheckResult *checkResult) const;
private:
    int64_t _lostTimeout;
};

CARBON_TYPEDEF_PTR(HealthStatusTransfer);

END_CARBON_NAMESPACE(master);

#endif //CARBON_HEALTHSTATUSTRANSFER_H
