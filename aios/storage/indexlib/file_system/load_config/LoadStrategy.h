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
#include <string>

#include "autil/Log.h"
#include "autil/legacy/jsonizable.h"

namespace indexlib { namespace file_system {
class LoadStrategy;
typedef std::shared_ptr<LoadStrategy> LoadStrategyPtr;

class LoadStrategy : public autil::legacy::Jsonizable
{
public:
    LoadStrategy();
    ~LoadStrategy();

public:
    virtual void Check() = 0;
    virtual bool EqualWith(const LoadStrategyPtr& loadStrategy) const = 0;

public:
    virtual const std::string& GetLoadStrategyName() const = 0;
    virtual void SetEnableLoadSpeedLimit(const std::shared_ptr<bool>& enableLoadSpeedLimit) = 0;

private:
    AUTIL_LOG_DECLARE();
};

extern const std::string READ_MODE_MMAP;
extern const std::string READ_MODE_CACHE;
extern const std::string READ_MODE_GLOBAL_CACHE;
extern const std::string READ_MODE_MEM;

}} // namespace indexlib::file_system
