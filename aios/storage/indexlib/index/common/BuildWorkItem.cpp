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
#include "indexlib/index/common/BuildWorkItem.h"

#include "autil/TimeUtility.h"

namespace indexlib::index {
AUTIL_LOG_SETUP(indexlib.index, BuildWorkItem);

BuildWorkItem::BuildWorkItem(const std::string& name, BuildWorkItemType type,
                             indexlibv2::document::IDocumentBatch* documentBatch)
    : _name(name)
    , _type(type)
    , _documentBatch(documentBatch)
{
    assert(!_name.empty());
    assert(_documentBatch != nullptr);
}

void BuildWorkItem::process()
{
    autil::ScopedLock lock(_lock);
    int64_t startTime = autil::TimeUtility::currentTimeInMicroSeconds();

    Status st = doProcess();
    if (!st.IsOK()) {
        AUTIL_LOG(ERROR, "Build work item [%s] encountered error, docCount[%lu], error[%s]", _name.c_str(),
                  _documentBatch->GetBatchSize(), st.ToString().c_str());
        return;
    }

    int64_t endTime = autil::TimeUtility::currentTimeInMicroSeconds();
    int64_t processTimeInMs = (endTime - startTime) / 1000;
    if (unlikely(processTimeInMs > 10)) {
        AUTIL_LOG(DEBUG, "Finish long time build work item [%s], cost [%ld]ms > 10ms, docCount[%lu]", _name.c_str(),
                  processTimeInMs, _documentBatch->GetBatchSize());
    }
    return;
}

} // namespace indexlib::index
