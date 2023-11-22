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

#include <stdint.h>
#include <string>
#include <utility>

#include "build_service/util/Log.h"

namespace build_service { namespace admin {

class LocatorDetectStrategy
{
public:
    explicit LocatorDetectStrategy(const std::string& condition, int64_t gap);
    ~LocatorDetectStrategy() = default;

public:
    bool detect(int64_t locator, const std::string& nodeIdentifier) const;
    static const std::string CURRENT_MICROSECOND_CONDITION;

private:
    std::pair<bool, int64_t> getBaseValue() const;

private:
    std::string _condition;
    int64_t _gap;

public:
    BS_LOG_DECLARE();
};

}} // namespace build_service::admin
