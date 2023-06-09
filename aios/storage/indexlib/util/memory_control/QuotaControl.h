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

#include "autil/Log.h"
namespace indexlib { namespace util {

class QuotaControl
{
public:
    QuotaControl(size_t totalQuota) noexcept;
    ~QuotaControl() noexcept;

public:
    size_t GetTotalQuota() const noexcept { return _totalQuota; }
    size_t GetLeftQuota() const noexcept { return _leftQuota; }

    bool AllocateQuota(size_t quota) noexcept
    {
        if (quota > _leftQuota) {
            return false;
        }
        _leftQuota -= quota;
        return true;
    }

private:
    size_t _totalQuota;
    size_t _leftQuota;

private:
    AUTIL_LOG_DECLARE();
};

typedef std::shared_ptr<QuotaControl> QuotaControlPtr;
}} // namespace indexlib::util
