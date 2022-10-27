#ifndef __INDEXLIB_FAKE_INDX_PARTITION_WRITER_H
#define __INDEXLIB_FAKE_INDX_PARTITION_WRITER_H

#include "indexlib/common_define.h"
#include "indexlib/indexlib.h"

#include "indexlib/partition/partition_writer.h"

IE_NAMESPACE_BEGIN(testlib);

class FakeIndexPartitionWriter : public partition::PartitionWriter
{
public:
    FakeIndexPartitionWriter(const config::IndexPartitionOptions& options)
        : partition::PartitionWriter(options)
    {
        _baseDocId = 0;
    }
    ~FakeIndexPartitionWriter() {}

private:
    FakeIndexPartitionWriter(const FakeIndexPartitionWriter &);
    FakeIndexPartitionWriter& operator=(const FakeIndexPartitionWriter &);
public:
    void Open(const config::IndexPartitionSchemaPtr& schema, 
                             const index_base::PartitionDataPtr& partitionData,
                             const partition::PartitionModifierPtr& modifier) override {}

    void ReOpenNewSegment(const partition::PartitionModifierPtr& modifier) override
    { assert(false); }

    void Close() override {}

    bool BuildDocument(const document::DocumentPtr& doc) override {
        _allDocument.push_back(doc);
        return true;
    }

    void RewriteDocument(
            const document::DocumentPtr& doc) override
    {}

    void DumpSegment() override
    {
        uint32_t totalDocCount = _allDocument.size();
        _segDocCount.push_back(totalDocCount - _baseDocId);
        _baseDocId = totalDocCount;
    }

    bool NeedDump(size_t memoryQuota) const override {
        assert(false);
        return false;
    }

    uint64_t GetTotalMemoryUse() const override
    {
        assert(false);
        return 0;
    }

    void ReportMetrics() const override {}

    void SetEnableReleaseOperationAfterDump(bool releaseOperations) override
    { assert(false); }
    
    bool IsDirty() const {
        return _allDocument.size() >= (size_t)_baseDocId;
    }

public:
    // for test
    const std::vector<uint32_t> &getSegDocCount() const { return _segDocCount; }
    const std::vector<document::DocumentPtr> &getAllDocument() const { return _allDocument; }
private:
    std::vector<document::DocumentPtr> _allDocument;
    docid_t _baseDocId;
    std::vector<uint32_t> _segDocCount;
};

DEFINE_SHARED_PTR(FakeIndexPartitionWriter);

IE_NAMESPACE_END(testlib);

#endif //__INDEXLIB_FAKE_INDX_PARTITION_WRITER_H
