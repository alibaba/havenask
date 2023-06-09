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
#include "autil/legacy/jsonizable.h"
#include "indexlib/file_system/load_config/LoadStrategy.h"

namespace indexlib { namespace file_system {
class MemLoadStrategy;
}} // namespace indexlib::file_system

namespace indexlib { namespace file_system {

class MmapLoadStrategy : public LoadStrategy
{
public:
    MmapLoadStrategy();
    MmapLoadStrategy(bool isLock, bool adviseRandom, uint32_t slice, uint32_t interval);
    ~MmapLoadStrategy();

public:
    void Jsonize(autil::legacy::Jsonizable::JsonWrapper& json) override;
    void Check() override;
    bool EqualWith(const LoadStrategyPtr& loadStrategy) const override;
    bool operator==(const MmapLoadStrategy& loadStrategy) const;

public:
    const std::string& GetLoadStrategyName() const override { return READ_MODE_MMAP; }
    void SetEnableLoadSpeedLimit(const std::shared_ptr<bool>& enableLoadSpeedLimit) override;

    bool IsLock() const { return _isLock; }
    bool IsAdviseRandom() const { return _adviseRandom; }
    uint32_t GetSlice() const { return _slice; }
    uint32_t GetInterval() const;
    MemLoadStrategy* CreateMemLoadStrategy() const noexcept;

private:
    bool _isLock;
    bool _adviseRandom;
    uint32_t _slice;
    uint32_t _interval;
    std::shared_ptr<bool> _enableLoadSpeedLimit;

private:
    AUTIL_LOG_DECLARE();
};
typedef std::shared_ptr<MmapLoadStrategy> MmapLoadStrategyPtr;

}} // namespace indexlib::file_system
