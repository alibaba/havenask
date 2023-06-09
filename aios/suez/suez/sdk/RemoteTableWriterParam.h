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

#include <cstdint>
#include <functional>
#include <limits>
#include <string>
#include <vector>

#include "autil/result/Result.h"
#include "multi_call/common/common.h"

namespace multi_call {
class QuerySession;
} // namespace multi_call

namespace suez {

struct WriteCallbackParam {
    int64_t maxCp = std::numeric_limits<int64_t>::min();
    int64_t maxBuildGap = std::numeric_limits<int64_t>::min();
    std::string firstResponseInfo;
};

struct MessageWriteParam {
    std::vector<std::pair<uint16_t, std::string>> docs;
    std::function<void(autil::result::Result<suez::WriteCallbackParam>)> cb;
    int64_t timeoutUs = 1000;
    std::shared_ptr<multi_call::QuerySession> querySession = nullptr;
};

typedef std::pair<uint16_t, std::string> WriteDoc;
typedef std::map<multi_call::PartIdTy, std::vector<WriteDoc>> PartDocMap;

struct RemoteTableWriterParam {
    RemoteTableWriterParam() { tagInfo.type = multi_call::TMT_REQUIRE; }
    std::string bizName;
    std::string sourceId;
    std::string tableName;
    std::string format;
    std::vector<WriteDoc> docs;
    int64_t timeoutUs;
    multi_call::TagMatchInfo tagInfo;
};

} // namespace suez
