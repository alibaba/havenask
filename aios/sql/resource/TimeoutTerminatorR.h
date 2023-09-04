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

#include <string>

#include "navi/common.h"
#include "navi/engine/Resource.h"
#include "navi/engine/ResourceConfigContext.h"
#include "navi/resource/TimeoutCheckerR.h"

namespace autil {
class TimeoutTerminator;
} // namespace autil
namespace navi {
class ResourceDefBuilder;
class ResourceInitContext;
} // namespace navi

namespace sql {

class TimeoutTerminatorR : public navi::Resource {
public:
    TimeoutTerminatorR();
    ~TimeoutTerminatorR();
    TimeoutTerminatorR(const TimeoutTerminatorR &) = delete;
    TimeoutTerminatorR &operator=(const TimeoutTerminatorR &) = delete;

public:
    void def(navi::ResourceDefBuilder &builder) const override;
    bool config(navi::ResourceConfigContext &ctx) override;
    navi::ErrorCode init(navi::ResourceInitContext &ctx) override;

public:
    autil::TimeoutTerminator *getTimeoutTerminator() const {
        return _timeoutTerminator;
    }

public:
    static const std::string RESOURCE_ID;

private:
    RESOURCE_DEPEND_DECLARE();

private:
    RESOURCE_DEPEND_ON(navi::TimeoutCheckerR, _timeoutCheckerR);
    autil::TimeoutTerminator *_timeoutTerminator;
};

NAVI_TYPEDEF_PTR(TimeoutTerminatorR);

} // namespace sql
