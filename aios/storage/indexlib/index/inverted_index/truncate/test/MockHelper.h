#include "gmock/gmock.h"
#include <memory>

#include "indexlib/index/attribute/AttributeDiskIndexer.h"
#include "indexlib/index/inverted_index/PostingIteratorImpl.h"
#include "indexlib/index/inverted_index/TermMatchData.h"
#include "indexlib/index/inverted_index/truncate/ITruncateWriterScheduler.h"
#include "indexlib/index/inverted_index/truncate/TruncateAttributeReader.h"
#include "indexlib/index/inverted_index/truncate/TruncateIndexWriter.h"
#include "indexlib/index/inverted_index/truncate/TruncateMetaReader.h"

namespace indexlib::index {

class MockPostingIterator : public PostingIteratorImpl
{
    MockPostingIterator() : PostingIteratorImpl(/*pool*/ nullptr, /*trace*/ nullptr) {};
    ~MockPostingIterator() = default;
    MOCK_METHOD(docid_t, SeekDoc, (docid_t), (override));
    MOCK_METHOD(docpayload_t, GetDocPayload, (), (override));
    MOCK_METHOD(void, Reset, (), (override));
    MOCK_METHOD(PostingIterator*, Clone, (), (const, override));
    MOCK_METHOD(void, Unpack, (TermMatchData&), (override));
    MOCK_METHOD(bool, HasPosition, (), (const, override));
    MOCK_METHOD(index::ErrorCode, SeekDocWithErrorCode, (docid_t, docid_t&), (override));
};

class MockTruncateMetaReader : public TruncateMetaReader
{
    MockTruncateMetaReader() : TruncateMetaReader(/*desc*/ false) {}
    ~MockTruncateMetaReader() = default;
    MOCK_METHOD(bool, Lookup, (const index::DictKeyInfo&, int64_t&, int64_t&), (const, override));
};

class MockTruncateAttributeReader : public TruncateAttributeReader
{
    MockTruncateAttributeReader() : TruncateAttributeReader(/*docMapper*/ nullptr, /*attrConfig*/ nullptr) {}

    MOCK_METHOD(std::shared_ptr<indexlibv2::index::AttributeDiskIndexer::ReadContextBase>, CreateReadContextPtr,
                (autil::mem_pool::Pool*), (const, override));
    MOCK_METHOD(bool, Read,
                (docid_t, const std::shared_ptr<indexlibv2::index::AttributeDiskIndexer::ReadContextBase>&, uint8_t*,
                 uint32_t, uint32_t&, bool&),
                (override));
};

class MockTruncateIndexWriter : public TruncateIndexWriter
{
    MockTruncateIndexWriter() = default;
    ~MockTruncateIndexWriter() = default;
    MOCK_METHOD(void, Init, (const std::vector<std::shared_ptr<indexlibv2::framework::SegmentMeta>>&), (override));
    MOCK_METHOD(bool, NeedTruncate, (const TruncateTriggerInfo&), (const, override));
    MOCK_METHOD(Status, AddPosting, (const DictKeyInfo&, const std::shared_ptr<PostingIterator>&, df_t), (override));
    MOCK_METHOD(void, EndPosting, (), (override));
    MOCK_METHOD(int64_t, EstimateMemoryUse, (int64_t, uint32_t, size_t), (const, override));
};

class MockTruncateWriterScheduler : public ITruncateWriterScheduler
{
    MOCK_METHOD(Status, PushWorkItem, (autil::WorkItem*), (override));
    MOCK_METHOD(Status, WaitFinished, (), (override));
};
} // namespace indexlib::index
