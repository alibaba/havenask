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

#include <limits>
#include <memory>

#include "autil/Lock.h"
#include "autil/TimeUtility.h"
#include "autil/WorkItem.h"
#include "indexlib/common_define.h"
#include "indexlib/indexlib.h"

DECLARE_REFERENCE_CLASS(document, Document);
DECLARE_REFERENCE_CLASS(document, DocumentCollector);

namespace indexlib { namespace index::legacy {

// batch build时进行并行build的线程任务单元，分为Index, Attribute, Summary, Source等类型。默认每一个字段会分成一个work
// item。

class BuildWorkItem : public autil::WorkItem
{
public:
    enum BuildWorkItemType {
        UNKNOWN = 0,
        ATTR = 1,
        INDEX = 2,
        SUMMARY = 3,
        SOURCE = 4,
    };

public:
    BuildWorkItem(const std::string& name, BuildWorkItemType type, bool isSub, docid_t buildingSegmentBaseDocId,
                  const document::DocumentCollectorPtr& docCollector);
    ~BuildWorkItem() {}

public:
    void process() override;
    void destroy() override { delete this; }
    void drop() override { destroy(); }
    virtual void doProcess() = 0;

public:
    const std::string& Name() { return _name; }
    void SetCost(size_t cost) { _cost = cost; }
    size_t Cost() const { return _cost; };
    BuildWorkItemType ItemType() const { return _type; }

protected:
    std::string _name;
    BuildWorkItemType _type;
    bool _isSub;
    size_t _cost;
    docid_t _buildingSegmentBaseDocId;
    document::DocumentCollectorPtr _sharedDocumentCollector;
    const std::vector<document::DocumentPtr>* _docs;
    mutable autil::ThreadMutex _lock;

private:
    IE_LOG_DECLARE();
};
}} // namespace indexlib::index::legacy
