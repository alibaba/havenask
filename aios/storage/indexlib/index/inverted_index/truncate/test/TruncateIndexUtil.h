#pragma once

#include "autil/Log.h"
#include "autil/NoCopyable.h"
#include "indexlib/framework/Segment.h"
#include "indexlib/index/IIndexMerger.h"
#include "indexlib/index/attribute/AttributeDiskIndexer.h"

namespace indexlib::index {

class FakeSegment : public indexlibv2::framework::Segment
{
public:
    FakeSegment(indexlibv2::framework::Segment::SegmentStatus segmentStatus)
        : indexlibv2::framework::Segment(segmentStatus)
    {
    }
    std::pair<Status, std::shared_ptr<indexlibv2::index::IIndexer>> GetIndexer(const std::string& type,
                                                                               const std::string& indexName) override
    {
        auto ret = _indexers[{type, indexName}];
        auto status = ret ? Status::OK() : Status::NotFound();
        return {status, ret};
    }

    void AddIndexer(const std::string& type, const std::string& indexName,
                    std::shared_ptr<indexlibv2::index::IIndexer> indexer) override
    {
        _indexers[{type, indexName}] = indexer;
    }
    std::pair<Status, size_t> EstimateMemUsed(const std::shared_ptr<indexlibv2::config::ITabletSchema>& schema) override
    {
        return {Status::OK(), 0};
    }
    size_t EvaluateCurrentMemUsed() override { return 0; }

private:
    std::map<std::pair<std::string, std::string>, std::shared_ptr<indexlibv2::index::IIndexer>> _indexers;
};

class FakeSingleAttributeDiskIndexer : public indexlibv2::index::AttributeDiskIndexer
{
public:
    FakeSingleAttributeDiskIndexer(const std::vector<int32_t>& attrValues) : _attrValues(attrValues) {}
    Status Open(const std::shared_ptr<indexlibv2::config::IIndexConfig>&,
                const std::shared_ptr<indexlib::file_system::IDirectory>&) override
    {
        return Status::OK();
    }
    bool IsInMemory() const override { return false; }
    bool Updatable() const override { return false; }
    bool UpdateField(docid_t, const autil::StringView&, bool, const uint64_t*) override { return false; }
    std::pair<Status, uint64_t> GetOffset(docid_t, const std::shared_ptr<ReadContextBase>&) const override
    {
        return {Status::OK(), 0};
    }
    std::shared_ptr<ReadContextBase> CreateReadContextPtr(autil::mem_pool::Pool*) const override { return nullptr; };
    size_t EstimateMemUsed(const std::shared_ptr<indexlibv2::config::IIndexConfig>&,
                           const std::shared_ptr<indexlib::file_system::IDirectory>&) override
    {
        return 0;
    };
    size_t EvaluateCurrentMemUsed() override { return 0; }
    Status SetPatchReader(const std::shared_ptr<indexlibv2::index::AttributePatchReader>& patchReader,
                          docid_t patchBaseDocId) override
    {
        return Status::OK();
    }
    bool Read(docid_t docId, const std::shared_ptr<ReadContextBase>&, uint8_t* buf, uint32_t, uint32_t& dataLen,
              bool& isNull) override
    {
        using T = int32_t;
        dataLen = sizeof(T);
        *((T*)buf) = _attrValues[docId];
        isNull = false;
        return true;
    }
    bool Read(docid_t docId, std::string* value, autil::mem_pool::Pool* pool) override { return false; }
    bool ReadBinaryValue(docid_t docId, autil::StringView* value, autil::mem_pool::Pool* pool) override
    {
        return false;
    }
    uint32_t TEST_GetDataLength(docid_t, autil::mem_pool::Pool*) const override { return sizeof(int32_t); }

private:
    std::vector<int32_t> _attrValues;
};

class TruncateIndexUtil : private autil::NoCopyable
{
public:
    TruncateIndexUtil() = default;
    ~TruncateIndexUtil() = default;

public:
    static indexlibv2::index::IIndexMerger::SegmentMergeInfos GetSegmentMergeInfos(const std::string& attr);
};

} // namespace indexlib::index
