#pragma once

#include <memory>

#include "indexlib/common_define.h"
#include "indexlib/index/inverted_index/BufferedPostingIterator.h"
#include "indexlib/index/inverted_index/format/dictionary/TieredDictionaryReader.h"
#include "indexlib/index/normal/attribute/accessor/attribute_segment_reader.h"
#include "indexlib/index/normal/inverted_index/accessor/in_mem_normal_index_segment_reader.h"
#include "indexlib/index/normal/inverted_index/accessor/multi_field_index_reader.h"
#include "indexlib/index/normal/inverted_index/accessor/normal_index_reader.h"
#include "indexlib/index/normal/inverted_index/builtin_index/bitmap/bitmap_index_reader.h"
#include "indexlib/indexlib.h"
#include "indexlib/test/unittest.h"

namespace indexlib { namespace index {

class MockMultiFieldIndexReader : public legacy::MultiFieldIndexReader
{
public:
    MOCK_METHOD(const std::shared_ptr<index::InvertedIndexReader>&, GetInvertedIndexReader, (const std::string& field),
                (const, override));
};
DEFINE_SHARED_PTR(MockMultiFieldIndexReader);

class MockNormalIndexReader : public NormalIndexReader
{
public:
    future_lite::coro::Lazy<index::Result<bool>>
    GetSegmentPostingAsync(const index::DictKeyInfo& key, uint32_t segmentIdx, SegmentPosting& segPosting,
                           file_system::ReadOption option, InvertedIndexSearchTracer* tracer) noexcept override
    {
        co_return DoGetSegmentPosting(key, segmentIdx, segPosting);
    }
    MOCK_METHOD(bool, DoGetSegmentPosting,
                (const index::DictKeyInfo& key, uint32_t segmentIdx, SegmentPosting& segPosting), ());
    MOCK_METHOD(bool, FillSegmentPosting,
                (const index::Term& term, const index::DictKeyInfo& key, uint32_t segmentIdx,
                 SegmentPosting& segPosting),
                ());
    using NormalIndexReader::LoadSegments;
    MOCK_METHOD(bool, LoadSegments,
                (const index_base::PartitionDataPtr& partitionData,
                 std::vector<NormalIndexSegmentReaderPtr>& segmentReaders),
                ());

    MOCK_METHOD(BufferedPostingIterator*, CreateBufferedPostingIterator,
                (autil::mem_pool::Pool * sessionPool, util::PooledUniquePtr<InvertedIndexSearchTracer> tracer),
                (const, override));
};
DEFINE_SHARED_PTR(MockNormalIndexReader);

class MockBitmapIndexReader : public legacy::BitmapIndexReader
{
public:
    index::Result<bool> GetSegmentPosting(const index::DictKeyInfo& key, uint32_t segmentIdx,
                                          SegmentPosting& segPosting,
                                          file_system::ReadOption option) const noexcept override
    {
        return DoGetSegmentPosting(key, segmentIdx, segPosting);
    }
    MOCK_METHOD(bool, DoGetSegmentPosting,
                (const index::DictKeyInfo& key, uint32_t segmentIdx, SegmentPosting& segPosting), (const));
};

class MockInMemNormalIndexSegmentReader : public InMemNormalIndexSegmentReader
{
public:
    MockInMemNormalIndexSegmentReader()
        : InMemNormalIndexSegmentReader(NULL, std::shared_ptr<index::AttributeSegmentReader>(),
                                        std::shared_ptr<index::InMemBitmapIndexSegmentReader>(),
                                        std::shared_ptr<index::InMemDynamicIndexSegmentReader>(),
                                        LegacyIndexFormatOption(), nullptr, nullptr)
    {
    }

public:
    MOCK_METHOD(bool, GetSegmentPosting,
                (const index::DictKeyInfo& key, docid64_t baseDocId, SegmentPosting& segPosting,
                 autil::mem_pool::Pool* sessionPool, InvertedIndexSearchTracer*),
                (const, override));
};
DEFINE_SHARED_PTR(MockInMemNormalIndexSegmentReader);

class MockBufferedPostingIterator : public BufferedPostingIterator
{
public:
    MockBufferedPostingIterator(const PostingFormatOption& postingFormatOption, autil::mem_pool::Pool* sessionPool)
        : BufferedPostingIterator(postingFormatOption, sessionPool, nullptr)
    {
    }

public:
    using BufferedPostingIterator::Init;
    MOCK_METHOD(bool, Init,
                (const SegmentPostingVector& segPostings, const index::SectionAttributeReader* sectionReader,
                 const uint32_t statePoolSize),
                ());
};

class MockTieredDictionaryReader : public TieredDictionaryReader
{
public:
    MockTieredDictionaryReader() : TieredDictionaryReader() {}
    index::Result<bool> InnerLookup(dictkey_t key, indexlib::file_system::ReadOption readOption,
                                    dictvalue_t& value) noexcept override
    {
        return DoInnerLookup(key, value);
    }

public:
    MOCK_METHOD(index::Result<bool>, DoInnerLookup, (dictkey_t key, dictvalue_t& value), ());
};

class MockBuilingSegmentReader : public IndexSegmentReader
{
public:
    MOCK_METHOD(bool, GetSegmentPosting,
                (const index::DictKeyInfo& key, docid64_t baseDocId, SegmentPosting& segPosting,
                 autil::mem_pool::Pool* sessionPool, InvertedIndexSearchTracer*),
                (const, override));
};

DEFINE_SHARED_PTR(MockTieredDictionaryReader);
}} // namespace indexlib::index
