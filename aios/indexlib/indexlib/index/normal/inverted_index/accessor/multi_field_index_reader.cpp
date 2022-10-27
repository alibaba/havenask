#include "indexlib/index/normal/inverted_index/accessor/multi_field_index_reader.h"
#include "indexlib/index/normal/inverted_index/accessor/index_reader.h"
#include "indexlib/index/normal/inverted_index/accessor/index_accessory_reader.h"
#include "indexlib/index/normal/inverted_index/index_metrics.h"
#include "indexlib/index/normal/inverted_index/accessor/multi_field_index_segment_reader.h"
#include "indexlib/index/normal/inverted_index/accessor/normal_index_reader.h"
#include "indexlib/config/index_config.h"

IE_NAMESPACE_USE(common);

IE_NAMESPACE_USE(config);

IE_NAMESPACE_BEGIN(index);
IE_LOG_SETUP(index, MultiFieldIndexReader);

using namespace std;
using namespace autil;

IndexReaderPtr MultiFieldIndexReader::RET_EMPTY_PTR = IndexReaderPtr();

MultiFieldIndexReader::MultiFieldIndexReader(IndexMetrics* indexMetrics) 
    : mIndexMetrics(indexMetrics)
    , mEnableAccessCountors(false)
{
}

MultiFieldIndexReader::MultiFieldIndexReader(
        const MultiFieldIndexReader& multiFieldIndexReader)
{
    mIndexReaders.resize(multiFieldIndexReader.mIndexReaders.size());
    for (size_t i = 0; i < multiFieldIndexReader.mIndexReaders.size(); ++i)
    {
        mIndexReaders[i] = IndexReaderPtr(
                multiFieldIndexReader.mIndexReaders[i]->Clone());
    }

    mName2PosMap = multiFieldIndexReader.mName2PosMap;
    mIndexId2PosMap = multiFieldIndexReader.mIndexId2PosMap;
    assert(mIndexReaders.size() == mName2PosMap.size());
    assert(mIndexReaders.size() == mIndexId2PosMap.size());
    mAccessoryReader = multiFieldIndexReader.mAccessoryReader;
    mIndexMetrics = multiFieldIndexReader.mIndexMetrics;
    mEnableAccessCountors = multiFieldIndexReader.mEnableAccessCountors;
}

MultiFieldIndexReader::~MultiFieldIndexReader() 
{
}

PostingIterator *MultiFieldIndexReader::Lookup(const Term& term, 
        uint32_t statePoolSize, PostingType type, autil::mem_pool::Pool *sessionPool)
{
    return LookupAsync(&term, statePoolSize, type, sessionPool).get();
}

future_lite::Future<PostingIterator*> MultiFieldIndexReader::LookupAsync(const common::Term* term,
    uint32_t statePoolSize, PostingType type, autil::mem_pool::Pool* sessionPool)
{
    if (!term)
    {
        return future_lite::makeReadyFuture<PostingIterator*>(nullptr);
    }
    const IndexReaderPtr& reader = GetIndexReader(term->GetIndexName());
    if (reader)
    {
        return reader->LookupAsync(term, statePoolSize, type, sessionPool);
    }
    return future_lite::makeReadyFuture<PostingIterator*>(nullptr);
}

PostingIterator* MultiFieldIndexReader::PartialLookup(const common::Term& term, const DocIdRangeVector& ranges,
        uint32_t statePoolSize, PostingType type, autil::mem_pool::Pool* pool)
{
    const IndexReaderPtr& reader = GetIndexReader(term.GetIndexName());
    if (reader)
    {
        return reader->PartialLookup(term, ranges, statePoolSize, type, pool);
    }
    return nullptr;
}

const SectionAttributeReader* MultiFieldIndexReader::GetSectionReader(
        const string& indexName) const
{
    const IndexReaderPtr& reader = GetIndexReader(indexName);
    if(reader)
    {
        return reader->GetSectionReader(indexName);
    }
    return NULL;
}

void MultiFieldIndexReader::AddIndexReader(
        const IndexConfigPtr& indexConf, const IndexReaderPtr& reader)
{
    const string& indexName = indexConf->GetIndexName();
    indexid_t indexId = indexConf->GetIndexId();

    if (mName2PosMap.find(indexName) != mName2PosMap.end())
    {
        INDEXLIB_FATAL_ERROR(Duplicate, "index reader for [%s] already exist!",
                             indexName.c_str());
    }

    if (mIndexId2PosMap.find(indexId) != mIndexId2PosMap.end())
    {
        INDEXLIB_FATAL_ERROR(Duplicate, "index reader for [id:%d] already exist!",
                             indexId);
    }
        
    mName2PosMap.insert(std::make_pair(indexName, mIndexReaders.size()));
    mIndexId2PosMap.insert(std::make_pair(indexId, mIndexReaders.size()));
    mIndexReaders.push_back(reader);

    reader->SetMultiFieldIndexReader(this);
    reader->SetAccessoryReader(mAccessoryReader);
}

const IndexReaderPtr& MultiFieldIndexReader::GetIndexReader(const string& field) const
{
    IndexName2PosMap::const_iterator it = mName2PosMap.find(field);
    if (it != mName2PosMap.end())
    {
        if (likely(mIndexMetrics != NULL && mEnableAccessCountors))
        {
            mIndexMetrics->IncAccessCounter(field);
        }
        return mIndexReaders[it->second];
    }
    return RET_EMPTY_PTR;
}

const IndexReaderPtr& MultiFieldIndexReader::GetIndexReaderWithIndexId(
        indexid_t indexId) const
{
    IndexId2PosMap::const_iterator it = mIndexId2PosMap.find(indexId);
    if (it != mIndexId2PosMap.end())
    {
        return mIndexReaders[it->second];
    }
    return RET_EMPTY_PTR;
}

IndexReader* MultiFieldIndexReader::Clone() const
{
    return new MultiFieldIndexReader(*this);
}

void MultiFieldIndexReader::SetAccessoryReader(
        const IndexAccessoryReaderPtr& accessoryReader)
{
    mAccessoryReader = accessoryReader;
    for (size_t i = 0; i < mIndexReaders.size(); ++i)
    {
        mIndexReaders[i]->SetAccessoryReader(accessoryReader);
    }
}

string MultiFieldIndexReader::GetIndexName() const
{
    INDEXLIB_THROW(misc::UnImplementException,
                 "GetIndexName is not supported in MultifieldIndexReader");
    return "";
}

KeyIteratorPtr MultiFieldIndexReader::CreateKeyIterator(const string& indexName)
{
    const IndexReaderPtr& indexReader = GetIndexReader(indexName);
    if (indexReader)
    {
        return indexReader->CreateKeyIterator(indexName);
    }
    return KeyIteratorPtr();
}
 
bool MultiFieldIndexReader::GenFieldMapMask(const std::string &indexName,
        const std::vector<std::string>& termFieldNames,
        fieldmap_t &targetFieldMap)
{
    const IndexReaderPtr& reader = GetIndexReader(indexName);
    if(reader != NULL)
    {
        return reader->GenFieldMapMask(indexName, 
                termFieldNames, targetFieldMap);
    }
    return false;
}

IE_NAMESPACE_END(index);

