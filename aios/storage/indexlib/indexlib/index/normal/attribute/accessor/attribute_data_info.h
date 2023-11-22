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

#include "autil/legacy/jsonizable.h"
#include "indexlib/common_define.h"
#include "indexlib/indexlib.h"

namespace indexlib { namespace index {

class AttributeDataInfo : public autil::legacy::Jsonizable
{
public:
    AttributeDataInfo(uint32_t count = 0, uint32_t length = 0) : uniqItemCount(count), maxItemLen(length) {}

    ~AttributeDataInfo() {}

public:
    void Jsonize(autil::legacy::Jsonizable::JsonWrapper& json) override
    {
        json.Jsonize("uniq_item_count", uniqItemCount);
        json.Jsonize("max_item_length", maxItemLen);
    }

    std::string ToString() const { return ToJsonString(*this); }

    void FromString(const std::string& str) { FromJsonString(*this, str); }

public:
    uint32_t uniqItemCount;
    uint32_t maxItemLen;

private:
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(AttributeDataInfo);
}} // namespace indexlib::index
