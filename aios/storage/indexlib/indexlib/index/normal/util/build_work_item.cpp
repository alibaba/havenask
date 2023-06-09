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
#include "indexlib/index/normal/util/build_work_item.h"

#include "indexlib/document/document.h"
#include "indexlib/document/document_collector.h"

namespace indexlib { namespace index::legacy {
IE_LOG_SETUP(index, BuildWorkItem);

BuildWorkItem::BuildWorkItem(const std::string& name, BuildWorkItemType type, bool isSub,
                             docid_t buildingSegmentBaseDocId, const document::DocumentCollectorPtr& docCollector)
    : _name(name)
    , _type(type)
    , _isSub(isSub)
    , _cost(0u)
    , _buildingSegmentBaseDocId(buildingSegmentBaseDocId)
    , _sharedDocumentCollector(docCollector)
    , _docs(&docCollector->GetDocuments())
{
    assert(!_name.empty());
    assert(_docs != nullptr);
}

void BuildWorkItem::process()
{
    autil::ScopedLock lock(_lock);
    // size_t docCount = 0;
    int64_t startTime = autil::TimeUtility::currentTimeInMicroSeconds();
    try {
        doProcess();
    } catch (const std::exception& e) {
        IE_LOG(ERROR, "BuildWorkItem unexpected build exception[%s]:[%s]", _name.c_str(), e.what());
    } catch (...) {
        IE_LOG(ERROR, "BuildWorkItem unexpected build exception[%s]", _name.c_str());
    }

    int64_t endTime = autil::TimeUtility::currentTimeInMicroSeconds();
    int64_t processTimeInMs = (endTime - startTime) / 1000;
    if (unlikely(processTimeInMs > 10)) {
        IE_LOG(DEBUG, "Finish long time build work item [%s], cost [%ld]ms > 10ms, docCount[%lu]", _name.c_str(),
               processTimeInMs, _docs->size());
    }
}

}} // namespace indexlib::index::legacy
