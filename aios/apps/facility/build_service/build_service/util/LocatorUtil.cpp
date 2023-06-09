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

#include "build_service/common/Locator.h"
#include "indexlib/framework/Locator.h"

namespace build_service::util {

std::pair<bool, std::vector<indexlibv2::base::Progress>>
LocatorUtil::convertSwiftProgress(const swift::protocol::ReaderProgress& swiftProgress, bool isMultiTopic)
{
    std::vector<indexlibv2::base::Progress> progress;
    if (!isMultiTopic) {
        for (const auto& singleProgress : swiftProgress.progress()) {
            progress.emplace_back(indexlibv2::base::Progress(singleProgress.filter().from(),
                                                             singleProgress.filter().to(), singleProgress.timestamp()));
        }
        std::sort(progress.begin(), progress.end(), [](const auto& a, const auto& b) { return a.from < b.from; });
    } else {
        std::vector<std::pair<uint64_t, indexlibv2::base::Progress>> mark;
        for (const auto& singleProgress : swiftProgress.progress()) {
            uint64_t maskMark =
                (singleProgress.filter().uint8filtermask() << 8) + singleProgress.filter().uint8maskresult();
            mark.emplace_back(std::make_pair(maskMark, indexlibv2::base::Progress(singleProgress.filter().from(),
                                                                                  singleProgress.filter().to(),
                                                                                  singleProgress.timestamp())));
        }
        std::sort(mark.begin(), mark.end(), [](const auto& a, const auto& b) {
            if (a.first == b.first) {
                return a.second.from < b.second.from;
            }
            return a.first < b.first;
        });
        int64_t lastSrc = -1;
        std::vector<indexlibv2::base::Progress> nextProgress;
        for (int i = 0; i < mark.size(); i++) {
            if (lastSrc != -1 && lastSrc != mark[i].first) {
                progress = ComputeMinProgress(progress, nextProgress);
                nextProgress.clear();
            }
            nextProgress.push_back(mark[i].second);
            lastSrc = mark[i].first;
        }
        if (!nextProgress.empty()) {
            progress = ComputeMinProgress(progress, nextProgress);
        }
    }
    return {true, progress};
}

swift::protocol::ReaderProgress LocatorUtil::convertLocatorProgress(
    const std::vector<indexlibv2::base::Progress>& progress, const std::string& topicName,
    const std::vector<std::pair<uint8_t, uint8_t>>& maskFilterPairs, bool useBSInnerMaskFilter)
{
    swift::protocol::ReaderProgress swiftProgress;
    for (const auto& singleProgress : progress) {
        for (const auto& [mask, result] : maskFilterPairs) {
            swift::protocol::SingleReaderProgress* singleSwiftProgress = swiftProgress.add_progress();
            singleSwiftProgress->set_topic(topicName);
            swift::protocol::Filter* filter = singleSwiftProgress->mutable_filter();
            filter->set_from(singleProgress.from);
            filter->set_to(singleProgress.to);
            filter->set_uint8filtermask(useBSInnerMaskFilter ? 0 : mask);
            filter->set_uint8maskresult(useBSInnerMaskFilter ? 0 : result);
            singleSwiftProgress->set_timestamp(singleProgress.offset);
        }
    }
    return swiftProgress;
}

std::vector<indexlibv2::base::Progress>
LocatorUtil::ComputeMinProgress(const std::vector<indexlibv2::base::Progress>& lastProgress,
                                const std::vector<indexlibv2::base::Progress>& progress)
{
    std::vector<indexlibv2::base::Progress> newProgress;
    size_t curPos = 0;
    size_t i = 0;
    int64_t lastOffset = -1;
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
                curProgress.offset = std::min(progress[curPos].offset, lastProgress[i].offset);
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
} // namespace build_service::util
