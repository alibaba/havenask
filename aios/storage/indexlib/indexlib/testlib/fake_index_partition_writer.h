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

#include "indexlib/common_define.h"
#include "indexlib/indexlib.h"
#include "indexlib/partition/partition_writer.h"

namespace indexlib { namespace testlib {

class FakeIndexPartitionWriter : public partition::PartitionWriter
{
public:
    FakeIndexPartitionWriter(const config::IndexPartitionOptions& options) : partition::PartitionWriter(options)
    {
        _baseDocId = 0;
    }
    ~FakeIndexPartitionWriter() {}

private:
    FakeIndexPartitionWriter(const FakeIndexPartitionWriter&);
    FakeIndexPartitionWriter& operator=(const FakeIndexPartitionWriter&);

public:
    void Open(const config::IndexPartitionSchemaPtr& schema, const index_base::PartitionDataPtr& partitionData,
              const partition::PartitionModifierPtr& modifier) override
    {
    }

    void ReOpenNewSegment(const partition::PartitionModifierPtr& modifier) override { assert(false); }

    void Close() override {}

    bool BuildDocument(const document::DocumentPtr& doc) override
    {
        _allDocument.push_back(doc);
        return true;
    }

    void RewriteDocument(const document::DocumentPtr& doc) override {}

    void DumpSegment() override
    {
        uint32_t totalDocCount = _allDocument.size();
        _segDocCount.push_back(totalDocCount - _baseDocId);
        _baseDocId = totalDocCount;
    }

    bool NeedDump(size_t memoryQuota, const document::DocumentCollector* docCollector) const override
    {
        assert(false);
        return false;
    }

    uint64_t GetTotalMemoryUse() const override
    {
        assert(false);
        return 0;
    }

    void ReportMetrics() const override {}

    void SetEnableReleaseOperationAfterDump(bool releaseOperations) override { assert(false); }

    bool IsDirty() const { return _allDocument.size() >= (size_t)_baseDocId; }

public:
    // for test
    const std::vector<uint32_t>& getSegDocCount() const { return _segDocCount; }
    const std::vector<document::DocumentPtr>& getAllDocument() const { return _allDocument; }

private:
    std::vector<document::DocumentPtr> _allDocument;
    docid_t _baseDocId;
    std::vector<uint32_t> _segDocCount;
};

DEFINE_SHARED_PTR(FakeIndexPartitionWriter);
}} // namespace indexlib::testlib
