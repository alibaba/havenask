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
#include "indexlib/index_base/index_meta/temperature_doc_info.h"

#include "indexlib/common/term_hint_parser.h"
#include "indexlib/index_base/segment/join_segment_directory.h"
#include "indexlib/index_base/segment/realtime_segment_directory.h"

using namespace std;
using namespace indexlib::index;

namespace indexlib { namespace index_base {
IE_LOG_SETUP(index_base, TemperatureDocInfo);

TemperatureDocInfo::TemperatureDocInfo() {}

TemperatureDocInfo::~TemperatureDocInfo() {}

index::TemperatureProperty TemperatureDocInfo::GetTemperaturePropery(segmentid_t segId)
{
    auto iter = mTemperatureSegInfo.find(segId);
    if (iter != mTemperatureSegInfo.end()) {
        return iter->second;
    }
    return TemperatureProperty::HOT;
}

void TemperatureDocInfo::Init(const Version& version, const index_base::SegmentDataVector& segmentDatas,
                              const std::vector<index_base::InMemorySegmentPtr>& dumpingSegments, docid_t totalDocCount)
{
    if (version.GetSegTemperatureMetas().empty()) {
        return;
    }
    std::map<segmentid_t, docid_t> segIdToBaseDocId;
    InitBaseDocIds(segmentDatas, dumpingSegments, segIdToBaseDocId);

    DocIdRangeVector hotRanges, warmRanges, coldRanges;
    string lastTemperature;
    string currentTemperature;
    DocIdRange currentRange(INVALID_DOCID, INVALID_DOCID);
    auto pushDocRange = [&](const string& temperature) {
        if (temperature == "HOT") {
            hotRanges.push_back(currentRange);
        } else if (temperature == "WARM") {
            warmRanges.push_back(currentRange);
        } else {
            coldRanges.push_back(currentRange);
        }
    };

    auto pushSegmentInfo = [&](const string& temperature, segmentid_t segId) {
        if (temperature == "HOT") {
            mTemperatureSegInfo[segId] = TemperatureProperty::HOT;
        } else if (temperature == "WARM") {
            mTemperatureSegInfo[segId] = TemperatureProperty::WARM;
        } else {
            mTemperatureSegInfo[segId] = TemperatureProperty::COLD;
        }
    };

    for (auto iter = segIdToBaseDocId.begin(); iter != segIdToBaseDocId.end(); iter++) {
        segmentid_t segmentId = iter->first;
        SegmentTemperatureMeta meta;
        if (version.GetSegmentTemperatureMeta(segmentId, meta)) {
            currentTemperature = meta.segTemperature;
        } else if (JoinSegmentDirectory::IsJoinSegmentId(segmentId)) {
            continue;
        } else {
            assert(RealtimeSegmentDirectory::IsRtSegmentId(segmentId));
            currentTemperature = "HOT";
        }

        pushSegmentInfo(currentTemperature, segmentId);
        if (currentTemperature != lastTemperature) {
            if (hotRanges.empty() && warmRanges.empty() && coldRanges.empty()) {
                if (currentRange.first == INVALID_DOCID) {
                    currentRange.first = iter->second;
                    lastTemperature = currentTemperature;
                } else {
                    currentRange.second = iter->second;
                    pushDocRange(lastTemperature);
                    lastTemperature = currentTemperature;
                    currentRange = DocIdRange(INVALID_DOCID, INVALID_DOCID);
                    currentRange.first = iter->second;
                }
            } else {
                currentRange.second = iter->second;
                pushDocRange(lastTemperature);
                lastTemperature = currentTemperature;
                currentRange = DocIdRange(INVALID_DOCID, INVALID_DOCID);
                currentRange.first = iter->second;
            }
        }
    }
    if (currentRange.second == INVALID_DOCID) {
        currentRange.second = totalDocCount;
        pushDocRange(currentTemperature);
    }
    mTemperatureDocRange[index::TemperatureProperty::HOT] = hotRanges;
    mTemperatureDocRange[index::TemperatureProperty::WARM] = warmRanges;
    mTemperatureDocRange[index::TemperatureProperty::COLD] = coldRanges;
}

bool TemperatureDocInfo::GetTemperatureDocIdRanges(int32_t hintValues, DocIdRangeVector& ranges) const
{
    if (IsEmptyInfo()) {
        return false;
    }
    ranges.clear();
    bool hasTemperature = false;
    auto temperaturePropertys = common::TermHintParser::GetHintProperty(hintValues);
    for (auto temperature : temperaturePropertys) {
        auto iter = mTemperatureDocRange.find(temperature);
        if (iter == mTemperatureDocRange.end()) {
            continue;
        }
        ranges = CombineDocIdRange(iter->second, ranges);
        hasTemperature = true;
    }
    if (!hasTemperature) {
        return false;
    }
    return true;
}

DocIdRangeVector TemperatureDocInfo::CombineDocIdRange(const DocIdRangeVector& currentRange,
                                                       const DocIdRangeVector& range) const
{
    if (currentRange.empty()) {
        return range;
    }
    if (range.empty()) {
        return currentRange;
    }
    size_t i = 0, j = 0;
    DocIdRangeVector targetRange;
    while (i < currentRange.size() && j < range.size()) {
        if (currentRange[i].first > range[j].second) {
            targetRange.push_back(range[j]);
            j++;
        } else if (currentRange[i].second < range[j].first) {
            targetRange.push_back(currentRange[i]);
            i++;
        } else {
            docid_t leftDoc = min(currentRange[i].first, range[j].first);
            docid_t rightDoc = max(currentRange[i].second, currentRange[j].second);
            DocIdRange docRange(leftDoc, rightDoc);
            targetRange.push_back(docRange);
            i++;
            j++;
        }
    }
    while (i < currentRange.size()) {
        targetRange.push_back(currentRange[i]);
        i++;
    }
    while (j < range.size()) {
        targetRange.push_back(range[j]);
        j++;
    }

    DocIdRangeVector ret;
    for (size_t i = 1; i < targetRange.size() + 1; i++) {
        DocIdRange range(targetRange[i - 1].first, targetRange[i - 1].second);
        while (i < targetRange.size() && targetRange[i - 1].second == targetRange[i].first) {
            i++;
        }
        range.second = targetRange[i - 1].second;
        ret.push_back(range);
    }
    return ret;
}

void TemperatureDocInfo::InitBaseDocIds(const index_base::SegmentDataVector& segmentDatas,
                                        const std::vector<index_base::InMemorySegmentPtr>& dumpingSegments,
                                        map<segmentid_t, docid_t>& segIdToBaseDocId)
{
    uint32_t totalDocCount = 0;
    for (size_t i = 0; i < segmentDatas.size(); i++) {
        const SegmentData& segData = segmentDatas[i];
        segIdToBaseDocId.insert(make_pair(segData.GetSegmentId(), segData.GetBaseDocId()));
        totalDocCount += segData.GetSegmentInfo()->docCount;
    }
    for (const InMemorySegmentPtr& inMemSegment : dumpingSegments) {
        segIdToBaseDocId.insert(make_pair(inMemSegment->GetSegmentId(), totalDocCount));
        totalDocCount += inMemSegment->GetSegmentInfo()->docCount;
    }
}

void TemperatureDocInfo::AddNewSegmentInfo(index::TemperatureProperty property, segmentid_t segId, docid_t startDocId,
                                           docid_t endDocId)
{
    mTemperatureSegInfo[segId] = property;
    DocIdRange newDocRange(startDocId, endDocId);
    DocIdRangeVector& oldDocRanges = mTemperatureDocRange[index::TemperatureProperty::HOT];
    if (!oldDocRanges.empty()) {
        DocIdRange& lastDocRange = oldDocRanges[oldDocRanges.size() - 1];
        if (lastDocRange.second == MAX_DOCID) {
            lastDocRange.second = startDocId;
        }
        if (lastDocRange.second != lastDocRange.first) {
            oldDocRanges.push_back(newDocRange);
        }
    } else {
        oldDocRanges.push_back(newDocRange);
    }
}

}} // namespace indexlib::index_base
