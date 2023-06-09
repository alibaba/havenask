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

#include "autil/Log.h"
#include "indexlib/file_system/load_config/LoadStrategy.h"

namespace indexlib { namespace file_system {

class MemLoadStrategy : public LoadStrategy
{
public:
    MemLoadStrategy(uint32_t slice, uint32_t interval, const std::shared_ptr<bool>& enableLoadSpeedLimit);
    ~MemLoadStrategy();

public:
    void Jsonize(autil::legacy::Jsonizable::JsonWrapper& json) override;
    void Check() override;
    bool EqualWith(const LoadStrategyPtr& loadStrategy) const override;

public:
    const std::string& GetLoadStrategyName() const override { return READ_MODE_MEM; }
    void SetEnableLoadSpeedLimit(const std::shared_ptr<bool>& enableLoadSpeedLimit) override {}

    uint32_t GetSlice() const { return _slice; }
    uint32_t GetInterval() const;

private:
    uint32_t _slice;
    uint32_t _interval;
    std::shared_ptr<bool> _enableLoadSpeedLimit;

private:
    AUTIL_LOG_DECLARE();
};
typedef std::shared_ptr<MemLoadStrategy> MemLoadStrategyPtr;

}} // namespace indexlib::file_system
