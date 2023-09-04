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

#include "navi/engine/Resource.h"
#include "navi/engine/TimeoutChecker.h"

namespace navi {

class TimeoutCheckerR : public Resource
{
public:
    TimeoutCheckerR();
    TimeoutCheckerR(TimeoutChecker *timeoutChecker);
    ~TimeoutCheckerR();
    TimeoutCheckerR(const TimeoutCheckerR &) = delete;
    TimeoutCheckerR &operator=(const TimeoutCheckerR &) = delete;
public:
    void def(ResourceDefBuilder &builder) const override;
    bool config(ResourceConfigContext &ctx) override;
    ErrorCode init(ResourceInitContext &ctx) override;
public:
    TimeoutChecker *getTimeoutChecker() const {
        return _timeoutChecker;
    }
public:
    static const std::string RESOURCE_ID;
private:
    TimeoutChecker *_timeoutChecker = nullptr;
};

NAVI_TYPEDEF_PTR(TimeoutCheckerR);

}

