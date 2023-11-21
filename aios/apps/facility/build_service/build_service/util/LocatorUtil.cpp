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
#include "build_service/util/LocatorUtil.h"

#include <assert.h>
#include <cstdint>
#include <ext/alloc_traits.h>
#include <memory>
#include <ostream>
#include <stddef.h>

#include "alog/Logger.h"
#include "autil/CommonMacros.h"
#include "build_service/common/Locator.h"

namespace build_service::util {
BS_LOG_SETUP(util, LocatorUtil);

std::pair<bool, indexlibv2::base::ProgressVector>
LocatorUtil::convertSwiftProgress(const swift::protocol::ReaderProgress& swiftProgress, bool isMultiTopic)
{
    indexlibv2::base::ProgressVector progress;
    if (!isMultiTopic) {
        if (swiftProgress.topicprogress_size() != 1) {
            BS_LOG(WARN, "size of topic progress not equal 1 [%s]", swiftProgress.ShortDebugString().c_str());
            return {false, progress};
        }
        for (const auto& partProgress : swiftProgress.topicprogress(0).partprogress()) {
            progress.emplace_back(indexlibv2::base::Progress(
                partProgress.from(), partProgress.to(), {partProgress.timestamp(), partProgress.offsetinrawmsg()}));
        }
        std::sort(progress.begin(), progress.end(), [](const auto& a, const auto& b) { return a.from < b.from; });

    } else {
        std::vector<std::pair<uint64_t, indexlibv2::base::Progress>> mark;
        for (const auto& topicProgress : swiftProgress.topicprogress()) {
            uint64_t maskMark = (topicProgress.uint8filtermask() << 8) + topicProgress.uint8maskresult();
            for (const auto& partProgress : topicProgress.partprogress()) {
                mark.emplace_back(std::make_pair(
                    maskMark, indexlibv2::base::Progress(partProgress.from(), partProgress.to(),
                                                         {partProgress.timestamp(), partProgress.offsetinrawmsg()})));
            }
        }
        std::sort(mark.begin(), mark.end(), [](const auto& a, const auto& b) {
            if (a.first == b.first) {
                return a.second.from < b.second.from;
            }
            return a.first < b.first;
        });
        int64_t lastSrc = -1;
        indexlibv2::base::ProgressVector nextProgress;
        for (int i = 0; i < mark.size(); i++) {
            if (lastSrc != -1 && lastSrc != mark[i].first) {
                progress = computeProgress(progress, nextProgress, minOffset);
                nextProgress.clear();
            }
            nextProgress.push_back(mark[i].second);
            lastSrc = mark[i].first;
        }
        if (!nextProgress.empty()) {
            progress = computeProgress(progress, nextProgress, minOffset);
        }
    }
    return {true, progress};
}

std::pair<bool, indexlibv2::base::MultiProgress>
LocatorUtil::convertSwiftMultiProgress(const swift::protocol::ReaderProgress& swiftProgress)
{
    indexlibv2::base::MultiProgress multiProgress;
    multiProgress.reserve(swiftProgress.topicprogress().size());
    for (const auto& topicProgress : swiftProgress.topicprogress()) {
        multiProgress.emplace_back();
        for (const auto& partProgress : topicProgress.partprogress()) {
            multiProgress.rbegin()->emplace_back(indexlibv2::base::Progress(
                partProgress.from(), partProgress.to(), {partProgress.timestamp(), partProgress.offsetinrawmsg()}));
        }
        std::sort(multiProgress.rbegin()->begin(), multiProgress.rbegin()->end(),
                  [](const auto& a, const auto& b) { return a.from < b.from; });
    }
    return {true, multiProgress};
}

swift::protocol::ReaderProgress
LocatorUtil::convertLocatorProgress(const indexlibv2::base::ProgressVector& progress, const std::string& topicName,
                                    const std::vector<std::pair<uint8_t, uint8_t>>& maskFilterPairs,
                                    bool useBSInnerMaskFilter)
{
    swift::protocol::ReaderProgress swiftProgress;
    for (const auto& [mask, result] : maskFilterPairs) {
        auto topicProgress = swiftProgress.add_topicprogress();
        topicProgress->set_topicname(topicName);
        topicProgress->set_uint8filtermask(useBSInnerMaskFilter ? 0 : mask);
        topicProgress->set_uint8maskresult(useBSInnerMaskFilter ? 0 : result);
        for (const auto& singleProgress : progress) {
            auto partProgress = topicProgress->add_partprogress();
            partProgress->set_from(singleProgress.from);
            partProgress->set_to(singleProgress.to);
            partProgress->set_timestamp(singleProgress.offset.first);
            partProgress->set_offsetinrawmsg(singleProgress.offset.second);
        }
    }
    return swiftProgress;
}

swift::protocol::ReaderProgress LocatorUtil::convertLocatorMultiProgress(
    const indexlibv2::base::MultiProgress& multiProgress, const std::string& topicName,
    const std::vector<std::pair<uint8_t, uint8_t>>& maskFilterPairs, bool useBSInnerMaskFilter)
{
    swift::protocol::ReaderProgress swiftProgress;
    assert(multiProgress.size() == maskFilterPairs.size());
    for (size_t i = 0; i < multiProgress.size(); i++) {
        auto topicProgress = swiftProgress.add_topicprogress();
        topicProgress->set_topicname(topicName);
        topicProgress->set_uint8filtermask(useBSInnerMaskFilter ? 0 : maskFilterPairs[i].first);
        topicProgress->set_uint8maskresult(useBSInnerMaskFilter ? 0 : maskFilterPairs[i].second);
        for (const auto& singleProgress : multiProgress[i]) {
            auto partProgress = topicProgress->add_partprogress();
            partProgress->set_from(singleProgress.from);
            partProgress->set_to(singleProgress.to);
            partProgress->set_timestamp(singleProgress.offset.first);
            partProgress->set_offsetinrawmsg(singleProgress.offset.second);
        }
    }
    return swiftProgress;
}

std::optional<indexlibv2::base::MultiProgress>
LocatorUtil::computeMultiProgress(const indexlibv2::base::MultiProgress& lastProgress,
                                  const indexlibv2::base::MultiProgress& progress,
                                  std::function<indexlibv2::base::Progress::Offset(indexlibv2::base::Progress::Offset,
                                                                                   indexlibv2::base::Progress::Offset)>
                                      compareFunc)
{
    if (unlikely(lastProgress.size() != progress.size())) {
        BS_LOG(WARN, "multi progress size [%lu] != [%lu]: lastProgress [%s], progress [%s]", lastProgress.size(),
               progress.size(), progress2DebugString(lastProgress).c_str(), progress2DebugString(progress).c_str());
        assert(false);
        return std::nullopt;
    }
    indexlibv2::base::MultiProgress result;
    for (size_t i = 0; i < lastProgress.size(); ++i) {
        result.emplace_back(computeProgress(lastProgress[i], progress[i], compareFunc));
    }
    return {result};
}

indexlibv2::base::ProgressVector
LocatorUtil::computeProgress(const indexlibv2::base::ProgressVector& lastProgress,
                             const indexlibv2::base::ProgressVector& progress,
                             std::function<indexlibv2::base::Progress::Offset(indexlibv2::base::Progress::Offset,
                                                                              indexlibv2::base::Progress::Offset)>
                                 compareFunc)
{
    indexlibv2::base::ProgressVector newProgress;
    size_t curPos = 0;
    size_t i = 0;
    indexlibv2::base::Progress::Offset lastOffset = indexlibv2::base::Progress::INVALID_OFFSET;
    int32_t lastTo = -1;
    while (curPos < progress.size() || i < lastProgress.size()) {
        indexlibv2::base::Progress curProgress;
        if (curPos < progress.size() && i < lastProgress.size()) {
            uint32_t from = std::max(lastProgress[i].from, (uint32_t)(lastTo + 1));
            uint32_t curFrom = std::max(progress[curPos].from, (uint32_t)(lastTo + 1));
            if (from > curFrom) {
                curProgress.from = curFrom;
                curProgress.to = std::min(from - 1, progress[curPos].to);
                curProgress.offset = progress[curPos].offset;
            } else if (from < curFrom) {
                curProgress.from = from;
                curProgress.to = std::min(curFrom - 1, lastProgress[i].to);
                curProgress.offset = lastProgress[i].offset;
            } else {
                assert(from == curFrom);
                curProgress.from = from;
                curProgress.to = std::min(progress[curPos].to, lastProgress[i].to);
                curProgress.offset = compareFunc(progress[curPos].offset, lastProgress[i].offset);
            }
        } else if (curPos < progress.size()) {
            curProgress.from = std::max(progress[curPos].from, (uint32_t)(lastTo + 1));
            curProgress.to = progress[curPos].to;
            curProgress.offset = progress[curPos].offset;
        } else {
            curProgress.from = std::max(lastProgress[i].from, (uint32_t)(lastTo + 1));
            curProgress.to = lastProgress[i].to;
            curProgress.offset = lastProgress[i].offset;
        }
        assert(curProgress.from <= curProgress.to);
        if (curProgress.offset == lastOffset && curProgress.from == (uint32_t)(lastTo + 1)) {
            newProgress.rbegin()->to = curProgress.to;
        } else {
            newProgress.push_back(curProgress);
        }
        lastTo = newProgress.rbegin()->to;
        lastOffset = newProgress.rbegin()->offset;
        if (curPos < progress.size() && lastTo >= progress[curPos].to) {
            curPos++;
        }
        if (i < lastProgress.size() && lastTo >= lastProgress[i].to) {
            i++;
        }
    }
    return newProgress;
}

bool LocatorUtil::encodeOffset(int8_t streamIdx, int64_t timestamp, int64_t* offset)
{
    assert(offset);
    if (streamIdx < INVALID_STREAM_IDX || timestamp < INVALID_TIMESTAMP || timestamp >= MAX_TIMESTAMP) {
        return false;
    }
    if (streamIdx == INVALID_STREAM_IDX && timestamp == INVALID_TIMESTAMP) {
        *offset = -1;
        return true;
    }
    if (streamIdx == INVALID_STREAM_IDX || timestamp == INVALID_TIMESTAMP) {
        return false;
    }
    *offset = (((int64_t)streamIdx) << TIMESTAMP_BIT) | timestamp;
    return true;
}
void LocatorUtil::decodeOffset(int64_t encodedOffset, int8_t* streamIdx, int64_t* timestamp)
{
    assert(streamIdx);
    assert(timestamp);
    if (encodedOffset == INVALID_TIMESTAMP) {
        *streamIdx = INVALID_STREAM_IDX;
        *timestamp = INVALID_TIMESTAMP;
    } else {
        *streamIdx = encodedOffset >> TIMESTAMP_BIT;
        *timestamp = encodedOffset & TIMESTAMP_MASK;
    }
}

int64_t LocatorUtil::getSwiftWatermark(const common::Locator& locator)
{
    int64_t watermark = 0;
    int8_t streamIdx = 0;
    decodeOffset(locator.GetOffset().first, &streamIdx, &watermark);
    return watermark;
}

std::string LocatorUtil::progress2DebugString(const indexlibv2::base::MultiProgress& multiProgress)
{
    std::stringstream ss;
    ss << "[";
    for (size_t i = 0; i < multiProgress.size(); ++i) {
        if (i != 0) {
            ss << "; ";
        }
        ss << progress2DebugString(multiProgress[i]);
    }
    ss << "]";
    return ss.str();
}

std::string LocatorUtil::progress2DebugString(const indexlibv2::base::ProgressVector& progress)
{
    std::stringstream stream;
    stream << "[";
    for (size_t i = 0; i < progress.size(); i++) {
        if (i != 0) {
            stream << ",";
        }
        stream << "{" << progress[i].from << "," << progress[i].to << "," << progress[i].offset.first << ","
               << progress[i].offset.second << "}";
    }
    stream << "]";
    return stream.str();
}

} // namespace build_service::util
