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
#include "indexlib/index/operation_log/TargetSegmentsRedoStrategy.h"

#include <cassert>

#include "indexlib/base/Constant.h"
#include "indexlib/framework/TabletData.h"
#include "indexlib/index/operation_log/OperationBase.h"

namespace indexlib::index {
namespace {
} // namespace

AUTIL_LOG_SETUP(indexlib.index, TargetSegmentsRedoStrategy);

TargetSegmentsRedoStrategy::TargetSegmentsRedoStrategy(bool isIncConsistentWithRealtime)
    : _isIncConsistentWithRealtime(isIncConsistentWithRealtime)
{
}

TargetSegmentsRedoStrategy::~TargetSegmentsRedoStrategy() {}

Status TargetSegmentsRedoStrategy::Init(const indexlibv2::framework::TabletData* tabletData,
                                        segmentid_t currentSegmentId, const std::vector<segmentid_t>& targetSegment)

{
    assert(tabletData);
    _deleteTargetDocRanges.clear();
    _updateTargetDocRanges.clear();

    auto slice = tabletData->CreateSlice();
    if (!_isIncConsistentWithRealtime) {
        docid_t docCountDeleteRedo = 0;
        for (auto segment : slice) {
            if (segment->GetSegmentId() >= currentSegmentId) {
                break;
            }
            auto docCount = segment->GetSegmentInfo()->docCount;
            docCountDeleteRedo += docCount;
        }
        if (docCountDeleteRedo != 0) {
            _deleteTargetDocRanges.push_back({0, docCountDeleteRedo});
        }
    }

    if (targetSegment.empty()) {
        return Status::OK();
    }

    auto currentTargetSegmentIter = targetSegment.begin();
    docid_t currentBaseDocId = 0;
    for (auto segmentPtr : slice) {
        if (currentTargetSegmentIter == targetSegment.end()) {
            break;
        }
        segmentid_t segmentId = segmentPtr->GetSegmentId();
        auto docCount = segmentPtr->GetSegmentInfo()->docCount;

        if (segmentId == *currentTargetSegmentIter) {
            if (segmentPtr->GetSegmentStatus() == indexlibv2::framework::Segment::SegmentStatus::ST_BUILDING) {
                AUTIL_LOG(ERROR, "target segment [%d] is building, cannot process to building segment", segmentId);
                return Status::InvalidArgs("target segment is building");
            }
            docid_t begin = currentBaseDocId, end = currentBaseDocId + docCount;
            if (!_updateTargetDocRanges.empty() && begin == _updateTargetDocRanges.rbegin()->second) {
                _updateTargetDocRanges.rbegin()->second = end;
            } else {
                _updateTargetDocRanges.push_back(std::make_pair(currentBaseDocId, currentBaseDocId + docCount));
            }
            currentTargetSegmentIter++;
        }
        currentBaseDocId += docCount;
    }
    if (currentTargetSegmentIter != targetSegment.end()) {
        AUTIL_LOG(ERROR, "segment [%d] is not in tablet data", *currentTargetSegmentIter);
        return Status::InvalidArgs("segment missing in tablet data");
    }

    if (_isIncConsistentWithRealtime) {
        _deleteTargetDocRanges = _updateTargetDocRanges;
    }
    AUTIL_LOG(INFO, "target update docid ranges [%s], delete docid ranges [%s], isIncConsistentWithRealtime [%d]",
              autil::legacy::ToJsonString(_updateTargetDocRanges, true).c_str(),
              autil::legacy::ToJsonString(_deleteTargetDocRanges, true).c_str(), _isIncConsistentWithRealtime);
    return Status::OK();
}

bool TargetSegmentsRedoStrategy::NeedRedo(segmentid_t operationSegment, OperationBase* operation,
                                          std::vector<std::pair<docid_t, docid_t>>& targetDocRanges) const
{
    auto getNeedRedo = [](const std::vector<std::pair<docid_t, docid_t>>& checkDocRanges,
                          std::vector<std::pair<docid_t, docid_t>>& targetDocRanges) -> bool {
        if (checkDocRanges.empty()) {
            return false;
        }
        targetDocRanges = checkDocRanges;
        return true;
    };
    if (operation->GetDocOperateType() == DocOperateType::DELETE_DOC) {
        return getNeedRedo(_deleteTargetDocRanges, targetDocRanges);
    } else {
        assert(operation->GetDocOperateType() == DocOperateType::UPDATE_FIELD);
        return getNeedRedo(_updateTargetDocRanges, targetDocRanges);
    }
}

} // namespace indexlib::index
