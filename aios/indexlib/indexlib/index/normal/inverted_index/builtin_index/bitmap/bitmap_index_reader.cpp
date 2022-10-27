#include "indexlib/index/normal/inverted_index/builtin_index/bitmap/bitmap_index_reader.h"
#include "indexlib/index/normal/inverted_index/builtin_index/bitmap/bitmap_posting_iterator.h"
#include "indexlib/index/normal/inverted_index/builtin_index/bitmap/in_mem_bitmap_index_segment_reader.h" 
#include "indexlib/index/normal/inverted_index/builtin_index/bitmap/bitmap_posting_iterator.h"
#include "indexlib/index/normal/inverted_index/format/dictionary/tiered_dictionary_reader.h"
#include "indexlib/index/normal/inverted_index/format/short_list_optimize_util.h"
#include "indexlib/index/normal/inverted_index/format/dictionary/dictionary_reader.h"
#include "indexlib/index/normal/inverted_index/format/index_format_option.h"
#include "indexlib/index/normal/inverted_index/accessor/segment_posting.h"
#include "indexlib/index/normal/inverted_index/accessor/building_index_reader.h"
#include "indexlib/index_base/segment/segment_data.h"
#include "indexlib/index_base/segment/in_memory_segment.h"
#include "indexlib/index_base/segment/partition_segment_iterator.h"
#include "indexlib/index_base/partition_data.h"

using namespace std;
using namespace autil;
using namespace autil::mem_pool;

IE_NAMESPACE_USE(config);
IE_NAMESPACE_USE(index_base);
IE_NAMESPACE_USE(common);
IE_NAMESPACE_USE(misc);
IE_NAMESPACE_USE(storage);


IE_NAMESPACE_USE(util);
IE_NAMESPACE_BEGIN(index);

IE_LOG_SETUP(index, BitmapIndexReader);

BitmapIndexReader::BitmapIndexReader()
{
}

BitmapIndexReader::BitmapIndexReader(const BitmapIndexReader& other)
{
    mIndexConfig = other.mIndexConfig;
    mDictReaders = other.mDictReaders;
    mPostingReaders = other.mPostingReaders;
    mSegmentInfos = other.mSegmentInfos;
    mBaseDocIds = other.mBaseDocIds;
    mHasher = other.mHasher;
    mBuildingIndexReader = other.mBuildingIndexReader;
}

BitmapIndexReader::~BitmapIndexReader() 
{
}

bool BitmapIndexReader::Open(
        const IndexConfigPtr& indexConfig, const PartitionDataPtr& partitionData)
{
    assert(indexConfig);
    mIndexConfig = indexConfig;
    mHasher.reset(KeyHasherFactory::Create(indexConfig->GetIndexType()));
    return LoadSegments(partitionData);
}

bool BitmapIndexReader::LoadSegments(const PartitionDataPtr& partitionData)
{
    mDictReaders.clear();
    mPostingReaders.clear();
    mSegmentInfos.clear();
    mBaseDocIds.clear();

    PartitionSegmentIteratorPtr iter = partitionData->CreateSegmentIterator();
    assert(iter);
    while (iter->IsValid())
    {
        if (iter->GetType() == SIT_BUILT)
        {
            const SegmentData& segmentData = iter->GetSegmentData();
            if (segmentData.GetSegmentInfo().docCount == 0)
            {
                iter->MoveToNext();
                continue;
            }

            DictionaryReaderPtr dictReader;
            file_system::FileReaderPtr postingReader;
            try
            {
                LoadSegment(segmentData, dictReader, postingReader);
            }
            catch (const ExceptionBase& e)
            {
                const string& segmentPath = segmentData.GetDirectory()->GetPath();
                IE_LOG(ERROR, "Load segment [%s] FAILED, reason: [%s]", 
                       segmentPath.c_str(), e.what());
                throw;
            }
            mDictReaders.push_back(dictReader);
            mPostingReaders.push_back(postingReader);
            mSegmentInfos.push_back(segmentData.GetSegmentInfo());
            mBaseDocIds.push_back(iter->GetBaseDocId());
        }
        else
        {
            assert(iter->GetType() == SIT_BUILDING);
            AddInMemSegment(iter->GetBaseDocId(), iter->GetInMemSegment());
            IE_LOG(DEBUG, "Add In-Memory SegmentReader for segment [%d], by index [%s]",
                   iter->GetSegmentId(), mIndexConfig->GetIndexName().c_str());
        }
        iter->MoveToNext();
    }
    return true;
}

void BitmapIndexReader::LoadSegment(const SegmentData& segmentData,
                                    DictionaryReaderPtr& dictReader,
                                    file_system::FileReaderPtr& postingReader)
{
    dictReader.reset();
    postingReader.reset();

    file_system::DirectoryPtr directory = segmentData.GetIndexDirectory(
            mIndexConfig->GetIndexName(), true);
    if (directory->IsExist(BITMAP_DICTIONARY_FILE_NAME))
    {
        dictReader.reset(new TieredDictionaryReader());
        dictReader->Open(directory, BITMAP_DICTIONARY_FILE_NAME);

        postingReader = directory->CreateIntegratedFileReader(
                BITMAP_POSTING_FILE_NAME);
    }
}

BitmapIndexReader* BitmapIndexReader::Clone() const
{
    return new BitmapIndexReader(*this);
}

PostingIterator *BitmapIndexReader::Lookup(uint64_t key, uint32_t statePoolSize, 
        autil::mem_pool::Pool *sessionPool) const
{
    IndexFormatOption indexFormatOption;
    indexFormatOption.Init(mIndexConfig);
    PostingFormatOption bitmapPostingFormatOption =
        indexFormatOption.GetPostingFormatOption().GetBitmapPostingFormatOption();

    SegmentPostingVectorPtr segPostings(new SegmentPostingVector);
    size_t reserveCount = mDictReaders.size();
    if (mBuildingIndexReader)
    {
        reserveCount += mBuildingIndexReader->GetSegmentCount();
    }
    segPostings->reserve(reserveCount);
    for (uint32_t i = 0; i < mDictReaders.size(); i++)
    {
        SegmentPosting segPosting(bitmapPostingFormatOption);
        if (GetSegmentPosting(key, i, segPosting))
        {
            segPostings->push_back(std::move(segPosting));
        }
    }
    if (mBuildingIndexReader)
    {
        mBuildingIndexReader->GetSegmentPosting(key, *segPostings, sessionPool);
    }

    BitmapPostingIterator* bitmapIt = CreateBitmapPostingIterator(sessionPool);
    assert(bitmapIt);
    if (bitmapIt->Init(segPostings, statePoolSize))
    {
        return bitmapIt;
    }
    IE_POOL_COMPATIBLE_DELETE_CLASS(sessionPool, bitmapIt);
    return NULL;
}

bool BitmapIndexReader::GetSegmentPosting(
        dictkey_t key, uint32_t segmentIdx, 
        SegmentPosting &segPosting) const
{
    dictvalue_t dictValue;
    if (!mDictReaders[segmentIdx] || !mDictReaders[segmentIdx]->Lookup(key, dictValue))
    {
        return false;
    }

    int64_t postingOffset = 0;
    ShortListOptimizeUtil::GetOffset(dictValue, postingOffset);
            
    uint32_t postingLen;
    mPostingReaders[segmentIdx]->Read(&postingLen, sizeof(uint32_t), postingOffset);

    postingOffset += sizeof(uint32_t);
    ByteSliceList* sliceList = 
        mPostingReaders[segmentIdx]->Read(postingLen, postingOffset);

    segPosting.Init(util::ByteSliceListPtr(sliceList), mBaseDocIds[segmentIdx],
                    mSegmentInfos[segmentIdx], dictValue);
    return true;
}

void BitmapIndexReader::AddInMemSegment(
        docid_t baseDocId, const InMemorySegmentPtr& inMemSegment)
{
    if (!inMemSegment)
    {
        return;
    }

    InMemorySegmentReaderPtr reader = inMemSegment->GetSegmentReader();
    IndexSegmentReaderPtr segmentReader = 
        reader->GetSingleIndexSegmentReader(mIndexConfig->GetIndexName());
    AddInMemSegmentReader(baseDocId, segmentReader->GetBitmapSegmentReader());
}

void BitmapIndexReader::AddInMemSegmentReader(
        docid_t baseDocId, const InMemBitmapIndexSegmentReaderPtr& bitmapSegReader)
{
    if (!mBuildingIndexReader)
    {
        IndexFormatOption indexFormatOption;
        indexFormatOption.Init(mIndexConfig);
        PostingFormatOption bitmapPostingFormatOption =
            indexFormatOption.GetPostingFormatOption().GetBitmapPostingFormatOption();
        mBuildingIndexReader.reset(new BuildingIndexReader(bitmapPostingFormatOption));
    }
    mBuildingIndexReader->AddSegmentReader(baseDocId, bitmapSegReader);
}

BitmapPostingIterator* BitmapIndexReader::CreateBitmapPostingIterator(
        Pool *sessionPool) const
{
    return IE_POOL_COMPATIBLE_NEW_CLASS(sessionPool,
            BitmapPostingIterator, mIndexConfig->GetOptionFlag(), sessionPool);
}

IE_NAMESPACE_END(index);

