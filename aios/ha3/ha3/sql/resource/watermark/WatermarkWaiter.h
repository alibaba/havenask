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

#include "autil/Log.h" // IWYU pragma: keep
#include "ha3/sql/common/GenericWaiter.h"

namespace indexlibv2::framework {
class ITablet;
} // namespace indexlibv2::framework

namespace isearch {
namespace sql {

class WatermarkWaiter;

struct WatermarkWaiterPack {
    using ClassT = WatermarkWaiter;
    using StatusT = std::shared_ptr<indexlibv2::framework::ITablet>;
    using CallbackParamT = std::shared_ptr<indexlibv2::framework::ITablet>;
};

class WatermarkWaiter final : public GenericWaiter<WatermarkWaiterPack>
{
public:
    WatermarkWaiter(std::shared_ptr<indexlibv2::framework::ITablet> tablet, uint64_t srcSignature);
    ~WatermarkWaiter() override;
    WatermarkWaiter(const WatermarkWaiter &) = delete;
    WatermarkWaiter& operator=(const WatermarkWaiter &) = delete;

public:
    std::string getDesc() const override;

public: // impl for base class
    std::shared_ptr<indexlibv2::framework::ITablet> getCurrentStatusImpl() const;
    int64_t transToKeyImpl(const std::shared_ptr<indexlibv2::framework::ITablet> &status) const;
    std::shared_ptr<indexlibv2::framework::ITablet>
    transToCallbackParamImpl(const std::shared_ptr<indexlibv2::framework::ITablet> &status) const;

public:
    static constexpr int64_t workerCheckLoopIntervalUs = 5 * 1000; // 5ms

private:
    std::shared_ptr<indexlibv2::framework::ITablet> _tablet;
    uint64_t _srcSignature = 0;
private:
    AUTIL_LOG_DECLARE();
};

typedef std::shared_ptr<WatermarkWaiter> WatermarkWaiterPtr;

} // namespace sql
} // namespace isearch
