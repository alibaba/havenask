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

#include "autil/legacy/jsonizable.h"
#include "indexlib/base/Status.h"
#include "indexlib/config/SortParam.h"
#include "indexlib/util/Exception.h"

namespace indexlib::config {

enum class KKVKeyType {
    PREFIX,
    SUFFIX,
};

class KKVIndexFieldInfo : public autil::legacy::Jsonizable
{
public:
    KKVIndexFieldInfo();
    ~KKVIndexFieldInfo() {}

public:
    void Jsonize(autil::legacy::Jsonizable::JsonWrapper& json) override;
    indexlibv2::Status CheckEqual(const KKVIndexFieldInfo& other) const;
    bool NeedTruncate() const { return countLimits != INVALID_COUNT_LIMITS; }

private:
    std::string KKVKeyTypeToString(KKVKeyType keyType);
    KKVKeyType StringToKKVKeyType(const std::string& str);

public:
    std::string fieldName;
    KKVKeyType keyType;
    uint32_t countLimits;
    uint32_t skipListThreshold;
    uint32_t protectionThreshold;
    SortParams sortParams;
    bool enableStoreOptimize;
    bool enableKeepSortSequence;

public:
    static constexpr uint32_t INVALID_COUNT_LIMITS = std::numeric_limits<uint32_t>::max();
    static constexpr uint32_t INVALID_SKIPLIST_THRESHOLD = std::numeric_limits<uint32_t>::max();
    static constexpr uint32_t DEFAULT_PROTECTION_THRESHOLD = std::numeric_limits<uint32_t>::max();
    static constexpr const char KKV_BUILD_PROTECTION_THRESHOLD[] = "build_protection_threshold";

private:
    AUTIL_LOG_DECLARE();
};
} // namespace indexlib::config
