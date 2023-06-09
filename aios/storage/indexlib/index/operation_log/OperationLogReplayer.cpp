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
#include "indexlib/index/operation_log/OperationLogReplayer.h"

#include "autil/UnitUtil.h"
#include "indexlib/base/MemoryQuotaController.h"
#include "indexlib/index/operation_log/OperationBase.h"
#include "indexlib/index/operation_log/OperationCursor.h"
#include "indexlib/index/operation_log/OperationIterator.h"
#include "indexlib/index/operation_log/OperationLogIndexer.h"
#include "indexlib/index/operation_log/OperationLogProcessor.h"
#include "indexlib/index/operation_log/OperationRedoStrategy.h"

namespace indexlib::index {
namespace {
using indexlibv2::framework::Locator;
} // namespace

AUTIL_LOG_SETUP(indexlib.index, OperationLogReplayer);

OperationLogReplayer::OperationLogReplayer(const std::vector<std::shared_ptr<OperationLogIndexer>>& indexers,
                                           const std::shared_ptr<OperationLogConfig>& indexConfig)
    : _indexers(indexers)
    , _indexConfig(indexConfig)
{
}

OperationLogReplayer::~OperationLogReplayer() {}

std::pair<Status, OperationCursor> OperationLogReplayer::RedoOperationsFromOneSegment(const RedoParams& params,
                                                                                      segmentid_t segmentId,
                                                                                      const Locator& locator)
{
    OperationCursor skipCursor(segmentId, -1);
    OperationCursor cursorEnd(segmentId + 1, 0);
    AUTIL_LOG(INFO, "redo from segment [%d]", segmentId);
    return InnerDoOperation(params, skipCursor, cursorEnd, locator);
}
std::pair<Status, OperationCursor> OperationLogReplayer::RedoOperationsFromCursor(const RedoParams& params,
                                                                                  const OperationCursor& skipCursor,
                                                                                  const Locator& locator)
{
    OperationCursor cursorEnd = skipCursor;
    AUTIL_LOG(INFO, "redo from cursur [%d] [%d]", skipCursor.segId, skipCursor.pos);
    if (!_indexers.empty()) {
        cursorEnd = OperationCursor((*_indexers.rbegin())->GetSegmentId() + 1, 0);
    }

    return InnerDoOperation(params, skipCursor, cursorEnd, locator);
}

std::pair<Status, OperationCursor> OperationLogReplayer::InnerDoOperation(const RedoParams& params,
                                                                          const OperationCursor& skipCursor,
                                                                          const OperationCursor& cursorEnd,
                                                                          const Locator& locator) const
{
    AUTIL_LOG(INFO, "begin redo, skip cursor [%d] [%d], cursorEnd [%d] [%d] locator [%s]", skipCursor.segId,
              skipCursor.pos, cursorEnd.segId, cursorEnd.pos, locator.DebugString().c_str());
    OperationCursor invalidCursor;
    auto pkReader = params.pkReader;
    auto processor = params.processor;
    auto redoStrategy = params.redoStrategy;
    auto memoryQuotaController = params.memoryQuotaController;
    if (!pkReader || !processor || !redoStrategy || !memoryQuotaController) {
        AUTIL_LOG(ERROR, "pkReader [%p] processor [%p] redoStrategy [%p] memoryQuotaController [%p] has nullptr",
                  pkReader, processor, redoStrategy, memoryQuotaController);
        return {Status::InvalidArgs("one of pkReader, processor, redoStrategy is nullptr"), invalidCursor};
    }
    OperationIterator iter(_indexers, _indexConfig);
    RETURN2_IF_STATUS_ERROR(iter.Init(skipCursor, locator), invalidCursor, "init operation iterator failed");
    OperationCursor lastCursur = skipCursor;
    size_t updateCount = 0, deleteCount = 0, skipUpdateCount = 0, skipDeleteCount = 0;
    while (true) {
        auto [nextStatus, hasNext] = iter.HasNext();
        RETURN2_IF_STATUS_ERROR(nextStatus, invalidCursor, "move to next failed");
        if (!hasNext) {
            break;
        }
        auto [opStatus, operation] = iter.Next();
        RETURN2_IF_STATUS_ERROR(opStatus, invalidCursor, "get next operation failed");
        auto docType = operation->GetDocOperateType();
        auto currentCursor = iter.GetLastCursor();
        if (currentCursor < cursorEnd) {
            std::vector<std::pair<docid_t, docid_t>> targetDocRanges;
            if (!redoStrategy->NeedRedo(iter.GetCurrentSegmentId(), operation, targetDocRanges)) {
                if (docType == DELETE_DOC) {
                    skipDeleteCount++;
                } else if (docType == UPDATE_FIELD) {
                    skipUpdateCount++;
                } else {
                    assert(false);
                }
                lastCursur = currentCursor;
                continue;
            }
            int64_t leftMemory = memoryQuotaController->GetFreeQuota();
            if (leftMemory <= 0) {
                AUTIL_LOG(ERROR, "left memory [%s] is not enough", autil::UnitUtil::GiBDebugString(leftMemory).c_str());
                return {Status::NoMem("left memory is not enough"), invalidCursor};
            }
            if (!RedoOneOperation(pkReader, processor, operation, targetDocRanges)) {
                AUTIL_LOG(ERROR, "redo operation failed");
                return {Status::InternalError("redo operation failed"), invalidCursor};
            }
            if (docType == DELETE_DOC) {
                deleteCount++;
            } else if (docType == UPDATE_FIELD) {
                updateCount++;
            } else {
                assert(false);
            }
            lastCursur = currentCursor;
        } else {
            break;
        }
    }
    AUTIL_LOG(
        INFO,
        "update redo count [%lu], delete redo count [%lu], skip update redo count[%lu], skip delete redo count [%lu]",
        updateCount, deleteCount, skipUpdateCount, skipDeleteCount);
    return {Status::OK(), lastCursur};
}

bool OperationLogReplayer::RedoOneOperation(const PrimaryKeyIndexReader* pkReader, OperationLogProcessor* processor,
                                            OperationBase* operation,
                                            const std::vector<std::pair<docid_t, docid_t>>& targetRanges) const
{
    [[maybe_unused]] bool processResult = operation->Process(pkReader, processor, targetRanges);
    return true;
}

} // namespace indexlib::index
