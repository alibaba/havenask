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
#ifndef __INDEXLIB_KV_ONLINE_CONFIG_H
#define __INDEXLIB_KV_ONLINE_CONFIG_H

#include <memory>

#include "autil/legacy/jsonizable.h"
#include "indexlib/common_define.h"
#include "indexlib/indexlib.h"

namespace indexlib { namespace config {

class KVOnlineConfig : public autil::legacy::Jsonizable
{
public:
    KVOnlineConfig();
    ~KVOnlineConfig();

public:
    void Jsonize(autil::legacy::Jsonizable::JsonWrapper& json) override;

public:
    static const uint32_t INVALID_COUNT_LIMITS = std::numeric_limits<uint32_t>::max();

public:
    // for kkv
    uint32_t countLimits = INVALID_COUNT_LIMITS;
    uint32_t buildProtectThreshold = INVALID_COUNT_LIMITS;

private:
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(KVOnlineConfig);
}} // namespace indexlib::config

#endif //__INDEXLIB_KV_ONLINE_CONFIG_H
