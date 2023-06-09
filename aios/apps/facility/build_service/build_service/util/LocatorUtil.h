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

#include <vector>

#include "build_service/common/Locator.h"
#include "build_service/util/Log.h"
#include "indexlib/base/Progress.h"
#include "swift/protocol/SwiftMessage.pb.h"

namespace build_service::util {

class LocatorUtil
{
public:
    static std::pair<bool, std::vector<indexlibv2::base::Progress>>
    convertSwiftProgress(const swift::protocol::ReaderProgress& swiftProgress, bool isMultiTopic);
    static swift::protocol::ReaderProgress
    convertLocatorProgress(const std::vector<indexlibv2::base::Progress>& progress, const std::string& topicName,
                           const std::vector<std::pair<uint8_t, uint8_t>>& maskFilterPairs, bool useBSInnerMaskFilter);
    static std::vector<indexlibv2::base::Progress>
    ComputeMinProgress(const std::vector<indexlibv2::base::Progress>& lastProgress,
                       const std::vector<indexlibv2::base::Progress>& progress);

private:
    LocatorUtil();
    ~LocatorUtil();

private:
    BS_LOG_DECLARE();
};

} // namespace build_service::util
