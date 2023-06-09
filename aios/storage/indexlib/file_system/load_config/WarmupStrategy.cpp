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
#include "indexlib/file_system/load_config/WarmupStrategy.h"

#include "indexlib/util/Exception.h"

using namespace std;

namespace indexlib { namespace file_system {
AUTIL_LOG_SETUP(indexlib.file_system, WarmupStrategy);

static const string WARMUP_NONE_TYPE_STRING = string("none");
static const string WARMUP_SEQUENTIAL_TYPE_STRING = string("sequential");

WarmupStrategy::WarmupStrategy() : _warmupType(WARMUP_NONE) {}

WarmupStrategy::~WarmupStrategy() {}

WarmupStrategy::WarmupType WarmupStrategy::FromTypeString(const string& typeStr)
{
    if (typeStr == WARMUP_NONE_TYPE_STRING) {
        return WarmupStrategy::WARMUP_NONE;
    }
    if (typeStr == WARMUP_SEQUENTIAL_TYPE_STRING) {
        return WarmupStrategy::WARMUP_SEQUENTIAL;
    }
    INDEXLIB_THROW(util::BadParameterException, "unsupported warmup strategy [ %s ]", typeStr.c_str());
    return WarmupStrategy::WARMUP_NONE;
}

string WarmupStrategy::ToTypeString(const WarmupType& type)
{
    if (type == WARMUP_NONE) {
        return WARMUP_NONE_TYPE_STRING;
    }
    if (type == WARMUP_SEQUENTIAL) {
        return WARMUP_SEQUENTIAL_TYPE_STRING;
    }
    INDEXLIB_THROW(util::BadParameterException, "unsupported enum warmup type [ %d ]", type);
    return WARMUP_NONE_TYPE_STRING;
}
}} // namespace indexlib::file_system
