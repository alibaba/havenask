#include "indexlib/index/normal/inverted_index/accessor/normal_index_reader.h"
#include "indexlib/index/normal/inverted_index/accessor/buffered_posting_iterator.h"
#include "indexlib/index/normal/inverted_index/accessor/segment_posting.h"
#include "indexlib/index/normal/inverted_index/accessor/multi_field_index_reader.h"
#include "indexlib/index/normal/inverted_index/accessor/index_segment_reader.h"
#include "indexlib/index/normal/inverted_index/accessor/normal_index_segment_reader.h"
#include "indexlib/index/normal/inverted_index/accessor/building_index_reader.h"
#include "indexlib/index/normal/inverted_index/accessor/index_accessory_reader.h"
#include "indexlib/index/normal/inverted_index/accessor/doc_range_partitioner.h"
#include "indexlib/index/normal/inverted_index/builtin_index/bitmap/bitmap_index_reader.h"
#include "indexlib/index/normal/attribute/accessor/section_attribute_reader_impl.h"
#include "indexlib/index_base/partition_data.h"
#include "indexlib/index_base/segment/in_memory_segment.h"
#include "indexlib/index/in_memory_segment_reader.h"
#include "indexlib/config/high_frequency_vocabulary.h"
#include "indexlib/util/key_hasher_factory.h"
#include "indexlib/misc/exception.h"
#include "indexlib/util/future_executor.h"
#include <future_lite/MoveWrapper.h>

using namespace std;
using namespace autil;

using future_lite::Future;
using future_lite::Promise;
using future_lite::Try;
using future_lite::Unit;

IE_NAMESPACE_USE(index);
IE_NAMESPACE_USE(config);
IE_NAMESPACE_USE(index_base);
IE_NAMESPACE_USE(common);
IE_NAMESPACE_USE(misc);
IE_NAMESPACE_USE(util);

IE_NAMESPACE_BEGIN(index);
IE_LOG_SETUP(index, NormalIndexReader);

NormalIndexReader::NormalIndexReader()
    : mBitmapIndexReader(NULL)
    , mMultiFieldIndexReader(NULL)
    , mExecutor(nullptr)
{
}

NormalIndexReader::NormalIndexReader(const NormalIndexReader& other)
    :IndexReader((const IndexReader&)other)
{
    mIndexFormatOption = other.mIndexFormatOption;
    mHasher = other.mHasher;

    mIndexConfig = other.mIndexConfig;
    mHighFreqVol = other.mHighFreqVol;
    mBitmapIndexReader = NULL;

    mSegmentReaders = other.mSegmentReaders;

    if (other.mAccessoryReader)
    {
        mAccessoryReader = other.mAccessoryReader;
    }
    if (other.mBitmapIndexReader)
    {
        mBitmapIndexReader = other.mBitmapIndexReader->Clone();
    }
    mMultiFieldIndexReader = NULL;
    mBuildingIndexReader = other.mBuildingIndexReader;
}

NormalIndexReader::~NormalIndexReader()
{
    DELETE_AND_SET_NULL(mBitmapIndexReader);
}


IndexReader* NormalIndexReader::Clone() const
{
    return new NormalIndexReader(*this);
}

PostingIterator *NormalIndexReader::Lookup(
        const Term& term, uint32_t statePoolSize, PostingType type,
        autil::mem_pool::Pool *sessionPool)
{
    auto postingIter = DoLookupAsync(&term, {}, statePoolSize, type, sessionPool).get();
    BEGIN_PROFILE_SECTION(NormalIndexReader);
    if (postingIter)
    {
        TermMeta *termMeta = postingIter->GetTermMeta();
        THREAD_PROFILE_STRING_VALUE_TRACE(IndexReader, QueryTerm, term.GetWord());
        THREAD_PROFILE_INT32_VALUE_TRACE(IndexReader, QueryTermDF,
                termMeta->GetDocFreq());
        THREAD_PROFILE_INT32_VALUE_TRACE(IndexReader, QueryTermTTF,
                termMeta->GetTotalTermFreq());
        
        THREAD_PROFILE_COUNT(IndexReader, DocFreq, termMeta->GetDocFreq());
        THREAD_PROFILE_COUNT(IndexReader, TotalTermFreq,
                termMeta->GetTotalTermFreq());
        (void)termMeta;
    }
    END_PROFILE_SECTION(NormalIndexReader);
    return postingIter;
}

future_lite::Future<PostingIterator*> NormalIndexReader::LookupAsync(
    const Term* term, uint32_t statePoolSize, PostingType type, autil::mem_pool::Pool* sessionPool)
{
    return DoLookupAsync(term, {}, statePoolSize, type, sessionPool);
}

future_lite::Future<PostingIterator*> NormalIndexReader::DoLookupAsync(const Term* term,
    const DocIdRangeVector& ranges, uint32_t statePoolSize, PostingType type,
    autil::mem_pool::Pool* sessionPool)
{
    if (!term)
    {
        return future_lite::makeReadyFuture<PostingIterator*>(nullptr);
    }
    if (!DocRangePartitioner::Validate(ranges))
    {
        return future_lite::makeReadyFuture<PostingIterator*>(nullptr);        
    }
    bool retNormal = true;
    if (type == pt_default && mHighFreqVol
        && mHighFreqVol->Lookup(*term))
    {
        retNormal = false;
    }
    else if (type == pt_bitmap && mBitmapIndexReader != NULL)
    {
        retNormal = false;
    }

    if (retNormal)
    {
        return CreatePostingIteratorAsync(term, ranges, statePoolSize, sessionPool);
    }
    else
    {
        return future_lite::makeReadyFuture<PostingIterator*>(
            mBitmapIndexReader->Lookup(*term, statePoolSize, sessionPool));
    }
}

PostingIterator* NormalIndexReader::PartialLookup(const Term& term, const DocIdRangeVector& ranges,
    uint32_t statePoolSize, PostingType type, autil::mem_pool::Pool* sessionPool)
{
    return DoLookupAsync(&term, ranges, statePoolSize, type, sessionPool).get();
}

bool NormalIndexReader::FillTruncSegmentPosting(
        const Term &term, dictkey_t key,
        uint32_t segmentIdx, SegmentPosting &segPosting)
{
    SegmentPosting truncSegPosting;
    bool hasTruncateSeg = GetSegmentPostingFromTruncIndex(
            term, key, segmentIdx, truncSegPosting);

    SegmentPosting mainChainSegPosting;
    bool hasMainChainSeg = GetSegmentPosting(key, segmentIdx, mainChainSegPosting);

    if (!hasTruncateSeg)
    {
        if (hasMainChainSeg)
        {
            segPosting = mainChainSegPosting;
            return true;
        }
        else
        {
            return false;
        }
    }
    else
    {
        segPosting = truncSegPosting;
    }

    TermMeta termMeta;
    bool ret = GetMainChainTermMeta(mainChainSegPosting, key, segmentIdx, termMeta);
    if (ret)
    {
        segPosting.SetMainChainTermMeta(termMeta);
    }
    return ret;
}

bool NormalIndexReader::GetSegmentPostingFromTruncIndex(const Term &term, dictkey_t key,
        uint32_t segmentIdx, SegmentPosting &segPosting)
{
    const LiteTerm* liteTerm = term.GetLiteTerm();    
    IndexReaderPtr truncateIndexReader;
    if (term.HasTruncateName())
    {
        truncateIndexReader = mMultiFieldIndexReader->GetIndexReader(
                term.GetTruncateIndexName());
    }
    else if (liteTerm && liteTerm->GetTruncateIndexId() != INVALID_INDEXID)
    {
        truncateIndexReader = mMultiFieldIndexReader->GetIndexReaderWithIndexId(
                liteTerm->GetTruncateIndexId());
    }

    if (!truncateIndexReader)
    {
        return false;
    }
    return truncateIndexReader->GetSegmentPosting(
            key, segmentIdx, segPosting);
}

bool NormalIndexReader::GetMainChainTermMeta(const SegmentPosting& mainChainSegPosting,
        dictkey_t key, uint32_t segmentIdx, TermMeta &termMeta)
{
    if (mainChainSegPosting.GetSliceListPtr() || mainChainSegPosting.IsDictInline())
    {
        termMeta = mainChainSegPosting.GetMainChainTermMeta();
        return true;
    }

    // TODO: get bitmap chain, to get original df
    if (!mBitmapIndexReader)
    {
        IE_LOG(ERROR, "Has truncate chain but no main chain, should has bitmap chain");
        return false;
    }

    const PostingFormatOption& postingFormatOption =
        mainChainSegPosting.GetPostingFormatOption();

    SegmentPosting bitmapSegPosting(
            postingFormatOption.GetBitmapPostingFormatOption());

    bool hasBitmapChainSeg = mBitmapIndexReader->GetSegmentPosting(
            key, segmentIdx, bitmapSegPosting);
    if (!hasBitmapChainSeg)
    {
        IE_LOG(ERROR, "Has truncate chain but no main chain, should has bitmap chain");
        return false;
    }        

    termMeta = bitmapSegPosting.GetMainChainTermMeta();
    return true;
}

bool NormalIndexReader::FillSegmentPosting(
        const Term &term, dictkey_t key, 
        uint32_t segmentIdx, SegmentPosting &segPosting)
{
    if (NeedTruncatePosting(term))
    {
        return FillTruncSegmentPosting(term, key, segmentIdx, segPosting);
    }
    return GetSegmentPosting(key, segmentIdx, segPosting);
}

future_lite::Future<bool> NormalIndexReader::FillSegmentPostingAsync(
    const Term* term, dictkey_t key, uint32_t segmentIdx, index::SegmentPosting& segPosting)
{
    if (NeedTruncatePosting(*term))
    {
        // TODO: support truncate
        auto ret = FillTruncSegmentPosting(*term, key, segmentIdx, segPosting);
        return future_lite::makeReadyFuture<bool>(ret);
    }
    return GetSegmentPostingAsync(key, segmentIdx, segPosting);
}

future_lite::Future<index::PostingIterator*> NormalIndexReader::CreatePostingIteratorByHashKey(
    const Term* term, dictkey_t termHashKey, const DocIdRangeVector& ranges,
    uint32_t statePoolSize, autil::mem_pool::Pool* sessionPool)
{
    std::vector<Future<bool>> futures;
    futures.reserve(mSegmentReaders.size());
    SegmentPostingVector segmentPostings;
    segmentPostings.reserve(mSegmentReaders.size() + 1); // for building segments
    bool needBuildingSegment = true;
    if (!ranges.empty())
    {
        FillRangeByBuiltSegments(term, termHashKey, ranges, futures, segmentPostings, needBuildingSegment);
    }
    else
    {
        for (uint32_t i = 0; i < mSegmentReaders.size(); i++)
        {
            segmentPostings.emplace_back();
            futures.push_back(FillSegmentPostingAsync(term, termHashKey, i, segmentPostings.back()));
        }
    }
    return future_lite::collectAll(futures.begin(), futures.end())
        .thenValue([this, termHashKey, statePoolSize, sessionPool,
                    segPostings = std::move(segmentPostings), needBuildingSegment](
                       std::vector<future_lite::Try<bool>>&& results) mutable -> PostingIterator* {
            assert(results.size() == segPostings.size());
            // SegmentPostingVectorPtr resultSegPostings(
            //         IE_POOL_COMPATIBLE_NEW_CLASS(sessionPool, SegmentPostingVector),
            //         [sessionPool](SegmentPostingVector* vec) {
            //             IE_POOL_COMPATIBLE_DELETE_CLASS(sessionPool, vec);
            //         });
            SegmentPostingVectorPtr resultSegPostings(new SegmentPostingVector);
            size_t reserveCount = segPostings.size();
            if (mBuildingIndexReader)
            {
                reserveCount += mBuildingIndexReader->GetSegmentCount();
            }
            resultSegPostings->reserve(reserveCount);
            for (size_t i = 0; i < results.size(); ++i)
            {
                if (results[i].value()) // will throw if FillSegmentPosting throws
                {
                    resultSegPostings->push_back(std::move(segPostings[i]));
                }
            }

            if (mBuildingIndexReader && needBuildingSegment)
            {
                mBuildingIndexReader->GetSegmentPosting(termHashKey, *resultSegPostings, sessionPool);
            }

            if (resultSegPostings->size() == 0)
            {
                return nullptr;
            }

            SectionAttributeReaderImpl* pSectionReader = NULL;
            if (mAccessoryReader)
            {
                pSectionReader
                    = mAccessoryReader->GetSectionReader(mIndexConfig->GetIndexName()).get();
            }

            std::unique_ptr<BufferedPostingIterator, std::function<void(BufferedPostingIterator*)>>
                iter(CreateBufferedPostingIterator(sessionPool),
                    [sessionPool](BufferedPostingIterator* iter) {
                        IE_POOL_COMPATIBLE_DELETE_CLASS(sessionPool, iter);
                    });

            if (iter->Init(resultSegPostings, pSectionReader, statePoolSize))
            {
                return iter.release();
            }
            return nullptr;
        });
}

void NormalIndexReader::FillRangeByBuiltSegments(const common::Term* term, dictkey_t termHashKey,
    const DocIdRangeVector& ranges, vector<Future<bool>>& futures,
    SegmentPostingVector& segPostings, bool& needBuildingSegment)
{
    size_t currentRangeIdx = 0;
    size_t currentSegmentIdx = 0;
    bool currentSegmentFilled = false;
    while (currentSegmentIdx < mSegmentReaders.size() && currentRangeIdx < ranges.size())
    {
        const auto& range = ranges[currentRangeIdx];
        const auto& segData = mSegmentReaders[currentSegmentIdx]->GetSegmentData();
        docid_t segBegin = segData.GetBaseDocId();
        docid_t segEnd = segData.GetBaseDocId() + segData.GetSegmentInfo().docCount;
        if (segEnd <= range.first)
        {
            ++currentSegmentIdx;
            currentSegmentFilled = false;            
            continue;
        }
        if (segBegin >= range.second)
        {
            ++currentRangeIdx;
            continue;
        }
        if (!currentSegmentFilled)
        {
            segPostings.emplace_back();
            futures.push_back(
                FillSegmentPostingAsync(term, termHashKey, currentSegmentIdx, segPostings.back()));
            currentSegmentFilled = true;
        }

        auto minEnd = std::min(segEnd, range.second);
        if (segEnd == minEnd)
        {
            ++currentSegmentIdx;
            currentSegmentFilled = false;
        }
        if (range.second == minEnd)
        {
            ++currentRangeIdx;
        }
    }
    needBuildingSegment = (currentRangeIdx < ranges.size());
}

PostingIterator *NormalIndexReader::CreatePostingIterator(
    const Term &term, const DocIdRangeVector& ranges, uint32_t statePoolSize,
        autil::mem_pool::Pool *sessionPool)
{
    return CreatePostingIteratorAsync(&term, ranges, statePoolSize, sessionPool).get();
}

future_lite::Future<PostingIterator*> NormalIndexReader::CreatePostingIteratorAsync(
    const Term* term, const DocIdRangeVector& ranges, uint32_t statePoolSize,
    autil::mem_pool::Pool* sessionPool)
{
    dictkey_t termHashKey;
    if (!mHasher->GetHashKey(*term, termHashKey))
    {
        IE_LOG(WARN, "invalid term [%s], index name [%s]",
               term->GetWord().c_str(), GetIndexName().c_str());
        return future_lite::makeReadyFuture<PostingIterator*>(nullptr);
    }
    if (!NeedTruncatePosting(*term) && mIndexConfig->IsBitmapOnlyTerm(termHashKey))
    {
        return future_lite::makeReadyFuture<PostingIterator*>(nullptr);
    }
    return CreatePostingIteratorByHashKey(term, termHashKey, ranges, statePoolSize, sessionPool);
}

BufferedPostingIterator* NormalIndexReader::CreateBufferedPostingIterator(
        autil::mem_pool::Pool *sessionPool) const
{
    return IE_POOL_COMPATIBLE_NEW_CLASS(sessionPool,
            BufferedPostingIterator, mIndexFormatOption.GetPostingFormatOption(),
            sessionPool);
}

PostingIterator* NormalIndexReader::CreateMainPostingIterator(
        dictkey_t key, uint32_t statePoolSize,
        autil::mem_pool::Pool *sessionPool)
{
    SegmentPostingVectorPtr segPostings(new SegmentPostingVector);
    segPostings->reserve(mSegmentReaders.size());
    for (uint32_t i = 0; i < mSegmentReaders.size(); i++)
    {
        SegmentPosting segPosting;
        if (GetSegmentPosting(key, i, segPosting))
        {
            segPostings->push_back(std::move(segPosting));
        }
    }

    BufferedPostingIterator* iter = IE_POOL_COMPATIBLE_NEW_CLASS(sessionPool,
            BufferedPostingIterator, mIndexFormatOption.GetPostingFormatOption(), sessionPool);

    if (iter->Init(segPostings, NULL, statePoolSize))
    {
        return iter;
    }
    IE_POOL_COMPATIBLE_DELETE_CLASS(sessionPool, iter);
    return NULL;
}

bool NormalIndexReader::NeedTruncatePosting(const Term& term) const
{
    if (mMultiFieldIndexReader
        && !term.GetIndexName().empty()
        && !term.GetTruncateName().empty())
    {
        return true;
    }
    
    const LiteTerm* liteTerm = term.GetLiteTerm();
    if (liteTerm)
    {
        return mMultiFieldIndexReader &&
            liteTerm->GetTruncateIndexId() != INVALID_INDEXID;
    }
    return false;
}

const SectionAttributeReader* NormalIndexReader::GetSectionReader(
        const string& indexName) const
{
    if (mAccessoryReader)
    {
        return mAccessoryReader->GetSectionReader(indexName).get();
    }
    return NULL;
}

void NormalIndexReader::SetAccessoryReader(
        const IndexAccessoryReaderPtr &accessoryReader)
{
    IndexType indexType = mIndexConfig->GetIndexType();
    if (indexType == it_pack || indexType == it_expack)
    {
        mAccessoryReader = accessoryReader;
    }
    else
    {
        mAccessoryReader.reset();
    }
}

vector<DictionaryReaderPtr> NormalIndexReader::GetDictReaders() const
{
    vector<DictionaryReaderPtr> dictReaders;
    for (size_t i = 0; i < mSegmentReaders.size(); ++i)
    {
        dictReaders.push_back(mSegmentReaders[i]->GetDictionaryReader());
    }
    return dictReaders;
}

void NormalIndexReader::Open(const config::IndexConfigPtr& indexConfig,
                             const index_base::PartitionDataPtr& partitionData)
{
    assert(indexConfig);
    mHasher.reset(util::KeyHasherFactory::Create(indexConfig->GetIndexType()));

    mIndexConfig = indexConfig;
    mIndexFormatOption.Init(mIndexConfig);
    vector<NormalIndexSegmentReaderPtr> segmentReaders;

    if (!LoadSegments(partitionData, segmentReaders))
    {
        IE_LOG(ERROR, "Open loadSegments FAILED");
        return;
    }

    mSegmentReaders.swap(segmentReaders);
    
    mHighFreqVol = indexConfig->GetHighFreqVocabulary();
    if (mHighFreqVol)
    {
        mBitmapIndexReader = new BitmapIndexReader();
        if (!mBitmapIndexReader->Open(indexConfig, partitionData))
        {
            IE_LOG(ERROR, "Open bitmap index [%s] FAILED",
                   indexConfig->GetIndexName().c_str());
            return;
        }
    }

    // TODO: do not get executor if parallel lookup is off
    mExecutor = util::FutureExecutor::GetInternalExecutor();
    if (!mExecutor)
    {
        IE_LOG(INFO, "internal executor not created, use serial mode, index[%s]",
               indexConfig->GetIndexName().c_str());
    }
}

bool NormalIndexReader::LoadSegments(const index_base::PartitionDataPtr& partitionData,
                                     vector<NormalIndexSegmentReaderPtr>& segmentReaders)
{
    PartitionSegmentIteratorPtr iter = partitionData->CreateSegmentIterator();
    assert(iter);
    while (iter->IsValid())
    {
        if (iter->GetType() == SIT_BUILT)
        {
            SegmentData segData = iter->GetSegmentData();
            if (segData.GetSegmentInfo().docCount == 0)
            {
                iter->MoveToNext();
                continue;
            }
            NormalIndexSegmentReaderPtr segmentReader = CreateSegmentReader();
            try
            {
                segmentReader->Open(mIndexConfig, segData);
            }
            catch (const ExceptionBase& e)
            {
                const string& segmentPath = segData.GetDirectory()->GetPath();
                IE_LOG(ERROR, "Load segment [%s] FAILED, reason: [%s]", 
                       segmentPath.c_str(), e.what());
                throw;
            }
            segmentReaders.push_back(segmentReader);
        }
        else
        {
            assert(iter->GetType() == SIT_BUILDING);
            AddBuildingSegmentReader(iter->GetBaseDocId(), iter->GetInMemSegment());
            IE_LOG(DEBUG, "Add In-Memory SegmentReader for segment [%d] by index [%s]",
                   iter->GetSegmentId(), GetIndexName().c_str());
        }
        iter->MoveToNext();
    }
    return true;
}

//TODO: add ut for truncate
void NormalIndexReader::AddBuildingSegmentReader(
        docid_t baseDocId, const index_base::InMemorySegmentPtr& inMemSegment)
{
    if (!inMemSegment) { return; }

    string buildingIndexName = GetIndexName();
    const string& nonTruncIndexName = mIndexConfig->GetNonTruncateIndexName();
    if (!nonTruncIndexName.empty())
    {
        buildingIndexName = nonTruncIndexName;
    }

    if (!mBuildingIndexReader)
    {
        mBuildingIndexReader = CreateBuildingIndexReader();
    }
    InMemorySegmentReaderPtr reader = inMemSegment->GetSegmentReader();
    mBuildingIndexReader->AddSegmentReader(baseDocId,
            reader->GetSingleIndexSegmentReader(buildingIndexName));
}

void NormalIndexReader::SetMultiFieldIndexReader(IndexReader *multiFieldIndexReader)
{ 
    mMultiFieldIndexReader = dynamic_cast<MultiFieldIndexReader*>(
            multiFieldIndexReader); 
    assert(mMultiFieldIndexReader);
}

BuildingIndexReaderPtr NormalIndexReader::CreateBuildingIndexReader()
{
    return BuildingIndexReaderPtr(new BuildingIndexReader(
                    mIndexFormatOption.GetPostingFormatOption()));
}

NormalIndexSegmentReaderPtr NormalIndexReader::CreateSegmentReader()
{
    return NormalIndexSegmentReaderPtr(new NormalIndexSegmentReader);
}

IE_NAMESPACE_END(index);

