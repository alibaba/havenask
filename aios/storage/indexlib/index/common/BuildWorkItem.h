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

#include <string>

#include "autil/Lock.h"
#include "autil/Log.h"
#include "autil/WorkItem.h"
#include "indexlib/base/Status.h"
#include "indexlib/document/IDocumentBatch.h"

namespace indexlib::index {

// batch build时进行并行build的线程任务单元，分为Index, Attribute, Summary, Source等类型。默认每一个字段会分成一个work
// item。
class BuildWorkItem : public autil::WorkItem
{
public:
    enum BuildWorkItemType {
        UNKNOWN = 0,
        PRIMARY_KEY = 1,
        DELETION_MAP = 2,
        OPERATION_LOG = 3,
        ATTRIBUTE = 4,
        INVERTED_INDEX = 5,
        SUMMARY = 6,
        SOURCE = 7,
        AITHETA = 8,
    };

public:
    BuildWorkItem(const std::string& name, BuildWorkItemType type,
                  const indexlibv2::document::IDocumentBatch* documentBatch);
    virtual ~BuildWorkItem() {}

public:
    void process() override;
    void destroy() override { delete this; }
    void drop() override { destroy(); }
    virtual Status doProcess() = 0;

public:
    const std::string& Name() { return _name; }
    BuildWorkItemType ItemType() const { return _type; }

protected:
    std::string _name;
    BuildWorkItemType _type;
    const indexlibv2::document::IDocumentBatch* _documentBatch;
    mutable autil::ThreadMutex _lock;

private:
    AUTIL_LOG_DECLARE();
};
} // namespace indexlib::index
