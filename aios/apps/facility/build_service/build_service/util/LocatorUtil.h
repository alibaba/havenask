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
    ComputeProgress(const std::vector<indexlibv2::base::Progress>& lastProgress,
                    const std::vector<indexlibv2::base::Progress>& progress,
                    std::function<indexlibv2::base::Progress::Offset(indexlibv2::base::Progress::Offset,
                                                                     indexlibv2::base::Progress::Offset)>
                        compareFunc);

    static std::string progress2DebugString(const std::vector<indexlibv2::base::Progress> progress);

    // for stream topic mode
    static bool EncodeOffset(int8_t streamIdx, int64_t timestamp, int64_t* offset);
    static void DecodeOffset(int64_t encodedOffset, int8_t* streamIdx, int64_t* timestamp);

    static int64_t GetSwiftWatermark(const common::Locator& locator);

    static indexlibv2::base::Progress::Offset minOffset(indexlibv2::base::Progress::Offset left,
                                                        indexlibv2::base::Progress::Offset right)
    {
        return std::min(left, right);
    }
    static indexlibv2::base::Progress::Offset maxOffset(indexlibv2::base::Progress::Offset left,
                                                        indexlibv2::base::Progress::Offset right)
    {
        return std::max(left, right);
    }

private:
    static const int64_t TIMESTAMP_BIT = 56;
    static const int64_t MAX_TIMESTAMP = (1L << TIMESTAMP_BIT);
    static const int64_t INVALID_TIMESTAMP = -1;
    static const int64_t TIMESTAMP_MASK = MAX_TIMESTAMP - 1;
    static const int64_t INVALID_STREAM_IDX = -1;
    LocatorUtil();
    ~LocatorUtil();

private:
    BS_LOG_DECLARE();
};

} // namespace build_service::util
