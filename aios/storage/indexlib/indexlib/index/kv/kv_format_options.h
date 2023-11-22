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

class KVFormatOptions : public autil::legacy::Jsonizable
{
public:
    KVFormatOptions() : mShortOffset(false), mUseCompactBucket(false) {}

    ~KVFormatOptions() {}

public:
    bool IsShortOffset() const { return mShortOffset; }
    bool UseCompactBucket() const { return mUseCompactBucket; }
    void SetShortOffset(bool flag) { mShortOffset = flag; }
    void SetUseCompactBucket(bool flag) { mUseCompactBucket = flag; }

    std::string ToString() { return ToJsonString(*this); }

    void FromString(const std::string& str) { FromJsonString(*this, str); }

public:
    void Jsonize(autil::legacy::Jsonizable::JsonWrapper& json) override
    {
        json.Jsonize("is_short_offset", mShortOffset);
        json.Jsonize("use_compact_bucket", mUseCompactBucket, mUseCompactBucket);
    }

private:
    bool mShortOffset;
    bool mUseCompactBucket;

private:
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(KVFormatOptions);
}} // namespace indexlib::index
