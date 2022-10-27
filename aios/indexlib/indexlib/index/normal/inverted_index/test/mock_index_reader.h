#ifndef __INDEXLIB_MOCK_INDEX_READER_H
#define __INDEXLIB_MOCK_INDEX_READER_H

#include <tr1/memory>
#include "indexlib/indexlib.h"
#include "indexlib/common_define.h"
#include "indexlib/test/unittest.h"
#include "indexlib/index/normal/inverted_index/accessor/multi_field_index_reader.h"
#include "indexlib/index/normal/inverted_index/accessor/buffered_posting_iterator.h"
#include "indexlib/index/normal/inverted_index/accessor/normal_index_reader.h"
#include "indexlib/index/normal/inverted_index/accessor/in_mem_normal_index_segment_reader.h"
#include "indexlib/index/normal/inverted_index/format/dictionary/tiered_dictionary_reader.h"
#include "indexlib/index/normal/inverted_index/builtin_index/bitmap/bitmap_index_reader.h"
#include "indexlib/index/normal/attribute/accessor/attribute_segment_reader.h"

IE_NAMESPACE_BEGIN(index);

class MockMultiFieldIndexReader : public index::MultiFieldIndexReader
{
public:
    MOCK_CONST_METHOD1(GetIndexReader, const index::IndexReaderPtr&(const std::string& field));
};
DEFINE_SHARED_PTR(MockMultiFieldIndexReader);


class MockNormalIndexReader : public NormalIndexReader
{
public:
    MOCK_METHOD3(GetSegmentPosting, bool(dictkey_t key, uint32_t segmentIdx, 
                    index::SegmentPosting &segPosting));
    MOCK_METHOD4(FillSegmentPosting, bool(const common::Term &term, dictkey_t key,
                    uint32_t segmentIdx, index::SegmentPosting &segPosting));

    MOCK_METHOD2(LoadSegments, bool(const index_base::PartitionDataPtr& partitionData,
                                    std::vector<NormalIndexSegmentReaderPtr>& segmentReaders));

    MOCK_CONST_METHOD1(CreateBufferedPostingIterator, 
                       index::BufferedPostingIterator*(autil::mem_pool::Pool *sessionPool));

};
DEFINE_SHARED_PTR(MockNormalIndexReader);

class MockBitmapIndexReader : public index::BitmapIndexReader
{
public:
    MOCK_CONST_METHOD3(GetSegmentPosting, bool(dictkey_t key, uint32_t segmentIdx, 
                    index::SegmentPosting &segPosting));
};

class MockInMemNormalIndexSegmentReader : public InMemNormalIndexSegmentReader
{
public:
    MockInMemNormalIndexSegmentReader()
        : InMemNormalIndexSegmentReader(NULL, index::AttributeSegmentReaderPtr(),
                index::InMemBitmapIndexSegmentReaderPtr(), IndexFormatOption())
    {}
public:
    MOCK_CONST_METHOD4(GetSegmentPosting, bool(dictkey_t key, docid_t baseDocId, 
                    index::SegmentPosting &segPosting, 
                    autil::mem_pool::Pool* sessionPool));

};
DEFINE_SHARED_PTR(MockInMemNormalIndexSegmentReader);

class MockBufferedPostingIterator : public index::BufferedPostingIterator 
{
public:
    MockBufferedPostingIterator(const index::PostingFormatOption& postingFormatOption,
                                autil::mem_pool::Pool *sessionPool)
        : index::BufferedPostingIterator(postingFormatOption, sessionPool)
    {}
public:
    MOCK_METHOD3(Init, bool(const index::SegmentPostingVector &segPostings,
                            const index::SectionAttributeReader* sectionReader,
                            const uint32_t statePoolSize));
};

class MockTieredDictionaryReader : public index::TieredDictionaryReader
{
public:
    MockTieredDictionaryReader()
        : index::TieredDictionaryReader()
    {}

public:
    MOCK_METHOD2(Lookup, bool(dictkey_t key, dictvalue_t& value));
};

class MockBuilingSegmentReader : public IndexSegmentReader
{
public:
    MOCK_CONST_METHOD4(GetSegmentPosting, bool(dictkey_t key, docid_t baseDocId, 
                    index::SegmentPosting &segPosting,
                    autil::mem_pool::Pool *sessionPool));


};

DEFINE_SHARED_PTR(MockTieredDictionaryReader);

IE_NAMESPACE_END(index);

#endif //__INDEXLIB_MOCK_INDEX_READER_H
