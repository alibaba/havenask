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

#include "autil/Log.h"
#include "indexlib/base/Status.h"
#include "indexlib/base/Types.h"

namespace indexlibv2 {
class MemoryQuotaController;
}
namespace indexlibv2 { namespace framework {
class Locator;
}} // namespace indexlibv2::framework
namespace indexlib::index {
class PrimaryKeyIndexReader;
class OperationLogIndexer;
class OperationLogProcessor;
class OperationLogConfig;
class OperationRedoStrategy;
class OperationBase;
struct OperationCursor;

class OperationLogReplayer
{
public:
    OperationLogReplayer(const std::vector<std::shared_ptr<OperationLogIndexer>>& indexers,
                         const std::shared_ptr<OperationLogConfig>& indexConfig);
    ~OperationLogReplayer();
    struct RedoParams {
        const PrimaryKeyIndexReader* pkReader;
        OperationLogProcessor* processor;
        const OperationRedoStrategy* redoStrategy;
        indexlibv2::MemoryQuotaController* memoryQuotaController;
    };

public:
    std::pair<Status, OperationCursor> RedoOperationsFromOneSegment(const RedoParams& params, segmentid_t segmentId,
                                                                    const indexlibv2::framework::Locator& locator);
    std::pair<Status, OperationCursor> RedoOperationsFromCursor(const RedoParams& params,
                                                                const OperationCursor& skipCursor,
                                                                const indexlibv2::framework::Locator& locator);

private:
    // cursorEnd will not process
    // if status is not ok, cursor is invalid
    std::pair<Status, OperationCursor> InnerDoOperation(const RedoParams& params, const OperationCursor& cursorBegin,
                                                        const OperationCursor& cursorEnd,
                                                        const indexlibv2::framework::Locator& locator) const;

    bool RedoOneOperation(const PrimaryKeyIndexReader* pkReader, OperationLogProcessor* processor,
                          OperationBase* operation, const std::vector<std::pair<docid_t, docid_t>>& targetRange) const;

private:
    std::vector<std::shared_ptr<OperationLogIndexer>> _indexers;
    std::shared_ptr<OperationLogConfig> _indexConfig;

private:
    AUTIL_LOG_DECLARE();
};

} // namespace indexlib::index
