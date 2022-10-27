#include <queue>
#include <autil/LongHashValue.h>
#include "indexlib/index/normal/reclaim_map/reclaim_map.h"
#include "indexlib/index/normal/attribute/accessor/single_attribute_segment_reader_for_merge.h"
#include "indexlib/index/normal/attribute/accessor/single_value_attribute_segment_reader.h"
#include "indexlib/index/normal/inverted_index/truncate/attribute_evaluator.h"
#include "indexlib/index/normal/reclaim_map/weighted_doc_iterator.h"
#include "indexlib/index/normal/attribute/accessor/offline_attribute_segment_reader_container.h"
#include "indexlib/index/in_memory_segment_reader.h"
#include "indexlib/index/normal/deletionmap/deletion_map_reader.h"
#include "indexlib/index/segment_directory_base.h"
#include "indexlib/index/normal/inverted_index/truncate/doc_info_allocator.h"
#include "indexlib/index/normal/inverted_index/truncate/multi_comparator.h"
#include "indexlib/index/normal/inverted_index/truncate/multi_attribute_evaluator.h"
#include "indexlib/file_system/buffered_file_reader.h"
#include "indexlib/file_system/buffered_file_writer.h"
#include "indexlib/file_system/directory.h"
#include "indexlib/config/attribute_config.h"
#include "indexlib/config/configurator_define.h"
#include "indexlib/util/class_typed_factory.h"
#include "indexlib/misc/exception.h"
#include "indexlib/config/field_type_traits.h"

using namespace std;

IE_NAMESPACE_USE(util);
IE_NAMESPACE_USE(common);
IE_NAMESPACE_USE(file_system);
IE_NAMESPACE_USE(config);
IE_NAMESPACE_USE(misc);
IE_NAMESPACE_USE(index_base);

IE_NAMESPACE_BEGIN(index);
IE_LOG_SETUP(index, ReclaimMap);
    
const std::string ReclaimMap::RECLAIM_MAP_FILE_NAME = "reclaim_map";

ReclaimMap::ReclaimMap() 
    : mDeletedDocCount(0)
    , mNewDocId(0)
{
}

ReclaimMap::ReclaimMap(const ReclaimMap& other)
    : mDocIdArray(other.mDocIdArray)
    , mDeletedDocCount(other.mDeletedDocCount)
    , mNewDocId(other.mNewDocId)
{
    if (other.mReverseGlobalIdArray)
    {
        mReverseGlobalIdArray.reset(new GlobalIdVector(*(other.mReverseGlobalIdArray)));
    }
}

ReclaimMap::~ReclaimMap() 
{
}

void ReclaimMap::Init(const SegmentMergeInfos& segMergeInfos,
                      const DeletionMapReaderPtr& deletionMapReader,
                      const OfflineAttributeSegmentReaderContainerPtr& attrReaders,
                      bool needReverseMapping)
{
    mAttrReaderContainer = attrReaders;
    int64_t before = autil::TimeUtility::currentTimeInSeconds();
    uint32_t maxDocCount = segMergeInfos.rbegin()->baseDocId + 
                           segMergeInfos.rbegin()->segmentInfo.docCount;
    mDocIdArray.resize(maxDocCount);
    SegmentMapper segMapper;
    if (NeedSplitSegment())
    {
        segMapper.Init(maxDocCount);
    }
    for (size_t i = 0; i < segMergeInfos.size(); ++i)
    {
        const SegmentMergeInfo& segMergeInfo = segMergeInfos[i];
        docid_t baseDocId = segMergeInfo.baseDocId;
        uint32_t segDocCount = segMergeInfos[i].segmentInfo.docCount;
        ReclaimOneSegment(segMergeInfos[i].segmentId, baseDocId, 
                          segDocCount, deletionMapReader, segMapper);
    }
    if (NeedSplitSegment())
    {
        MakeTargetBaseDocIds(segMapper, segMapper.GetMaxSegmentIndex());
        RewriteDocIdArray(segMergeInfos, segMapper);
        segMapper.Clear();
    }

    if (needReverseMapping)
    {
        InitReverseDocIdArray(segMergeInfos);
    }
    int64_t after = autil::TimeUtility::currentTimeInSeconds();
    IE_LOG(INFO, "Init reclaim map use %ld seconds", (after - before));
}

void ReclaimMap::Init(const SegmentMergeInfos& segMergeInfos,
                      const DeletionMapReaderPtr& deletionMapReader,
                      const OfflineAttributeSegmentReaderContainerPtr& attrReaders,
                      const AttributeConfigVector& attributeConfigs,
                      const SortPatternVec& sortPatterns,
                      const SegmentDirectoryBasePtr& segDir,
                      bool needReverseMapping)
{
    mAttrReaderContainer = attrReaders;
    int64_t before = autil::TimeUtility::currentTimeInSeconds();
    SortByWeightInit(segMergeInfos, deletionMapReader,
            attributeConfigs, sortPatterns, segDir);
    if (needReverseMapping)
    {
        InitReverseDocIdArray(segMergeInfos);
    }
    int64_t after = autil::TimeUtility::currentTimeInSeconds();
    IE_LOG(INFO, "Init sort-by-weight reclaim map use %ld seconds", (after - before));
}

void ReclaimMap::Init(ReclaimMap* mainReclaimMap,
                      const SegmentDirectoryBasePtr& mainSegmentDirectory,
                      const AttributeConfigPtr& mainJoinAttrConfig,
                      const SegmentMergeInfos& mainSegMergeInfos,
                      const SegmentMergeInfos& subSegMergeInfos,
                      const DeletionMapReaderPtr& subDeletionMapReader,
                      bool needReverseMapping)
{
    assert(mainJoinAttrConfig->GetAttrName() == MAIN_DOCID_TO_SUB_DOCID_ATTR_NAME);
    CheckMainAndSubMergeInfos(mainSegMergeInfos, subSegMergeInfos);

    vector<int> segIdxMap;
    vector<AttributeSegmentReaderPtr> mainJoinAttrSegReaders;
    InitSegmentIdxs(segIdxMap, mainSegMergeInfos);
    InitJoinAttributeSegmentReaders(mainJoinAttrSegReaders, mainSegmentDirectory,
        mainJoinAttrConfig, mainSegMergeInfos, mainReclaimMap->GetTargetSegmentCount() > 1u);

    if (!mainReclaimMap->mReverseGlobalIdArray)
    {
        mainReclaimMap->InitReverseDocIdArray(mainSegMergeInfos);
    }

    ReclaimJoinedSubDocs(
        mainReclaimMap, subSegMergeInfos, segIdxMap, mainJoinAttrSegReaders, subDeletionMapReader);

    // If join attr reader is loaded in-mem, clear() will release its memory.
    mainJoinAttrSegReaders.clear();

    if (needReverseMapping)
    {
        InitReverseDocIdArray(subSegMergeInfos);
    }
}

void ReclaimMap::MakeTargetBaseDocIds(const SegmentMapper &segMapper,
                                      segmentindex_t maxSegIdx) {
    size_t currentDocs = 0;
    mTargetBaseDocIds.clear();
    for (segmentindex_t segIdx = 0; segIdx <= maxSegIdx; ++segIdx)
    {
        mTargetBaseDocIds.push_back(currentDocs);
        currentDocs += segMapper.GetSegmentDocCount(segIdx);
    }
}

void ReclaimMap::RewriteDocIdArray(
    const SegmentMergeInfos& segMergeInfos, const SegmentMapper& segMapper)
{
    mNewDocId = 0;
    for (const auto& segMergeInfo : segMergeInfos)
    {
        for (size_t localId = 0; localId < segMergeInfo.segmentInfo.docCount; ++localId)
        {
            auto oldId = segMergeInfo.baseDocId + localId;
            if (mDocIdArray[oldId] == INVALID_DOCID)
            {
                continue;
            }
            auto segIdx = segMapper.GetSegmentIndex(oldId);
            mDocIdArray[oldId] += mTargetBaseDocIds[segIdx];
            ++mNewDocId;            
        }
    }
}

void ReclaimMap::ReclaimJoinedSubDocs(
        ReclaimMap* mainReclaimMap, 
        const SegmentMergeInfos& subSegMergeInfos, 
        const vector<int>& segIdxMap, 
        const vector<AttributeSegmentReaderPtr>& mainJoinAttrSegReaders, 
        const DeletionMapReaderPtr& subDeletionMapReader)
{
    assert(mNewDocId == 0);
    uint32_t maxDocCount = subSegMergeInfos.rbegin()->baseDocId + 
                           subSegMergeInfos.rbegin()->segmentInfo.docCount;
    mDocIdArray.resize(maxDocCount, INVALID_DOCID);
    SegmentMapper segMapper;
    bool multiTargetSegment = mainReclaimMap->GetTargetSegmentCount() > 1u;
    if (multiTargetSegment)
    {
        segMapper.Init(maxDocCount, false);
    }

    uint32_t newMainDocCount = mainReclaimMap->GetNewDocCount();
    uint32_t mergedDocCount = 0;
    for (size_t i = 0; i < subSegMergeInfos.size(); ++i)
    {
        mergedDocCount += subSegMergeInfos[i].segmentInfo.docCount;
    }

    vector<docid_t> newMainJoinValues;
    vector<docid_t> newSubJoinValues;
    newMainJoinValues.resize(newMainDocCount);
    newSubJoinValues.reserve(mergedDocCount);

    for (docid_t newDocId = 0; newDocId < (docid_t)newMainDocCount; newDocId++)
    {
        segmentid_t segId;
        docid_t localMainDocId = mainReclaimMap->GetOldDocIdAndSegId(
                newDocId, segId);

        int32_t segIdx = segIdxMap[segId];
        const AttributeSegmentReaderPtr& localSegReader = mainJoinAttrSegReaders[segIdx];

        docid_t lastDocJoinValue = 0;
        docid_t curDocJoinValue = 0;
        GetMainJoinValues(localSegReader, localMainDocId, lastDocJoinValue, curDocJoinValue);
        
        for(docid_t subDocId = lastDocJoinValue; subDocId < curDocJoinValue; subDocId++)
        {
            if (subDeletionMapReader->IsDeleted(segId, subDocId))
            {
                continue;
            }
            docid_t baseDocId = subSegMergeInfos[segIdx].baseDocId;
            if (multiTargetSegment)
            {
                auto targetSegIdx = mainReclaimMap->GetTargetSegmentIndex(newDocId);
                segMapper.Collect(baseDocId + subDocId, targetSegIdx);
            }
            mDocIdArray[baseDocId + subDocId] = mNewDocId++;
            newSubJoinValues.push_back(newDocId);
        }
        newMainJoinValues[newDocId] = mNewDocId;
    }
    if (multiTargetSegment)
    {
        MakeTargetBaseDocIds(
            segMapper,
            (segmentindex_t)mainReclaimMap->GetTargetSegmentCount() - 1);
        segMapper.Clear();        
    }

    assert(mNewDocId == (docid_t)newSubJoinValues.size());
    mDeletedDocCount = mergedDocCount - mNewDocId;
    InitJoinValueArray(newSubJoinValues);
    mainReclaimMap->InitJoinValueArray(newMainJoinValues);
}

void ReclaimMap::GetMainJoinValues(const AttributeSegmentReaderPtr& segReader,
                                   docid_t localDocId, docid_t& lastDocJoinValue, 
                                   docid_t& curDocJoinValue)
{
    assert(segReader);
    assert(localDocId >= 0);

    if (localDocId == 0)
    {
        lastDocJoinValue = 0;
    }
    else
    {
        segReader->Read(localDocId - 1, (uint8_t*)&lastDocJoinValue, sizeof(docid_t));
    }
    segReader->Read(localDocId, (uint8_t*)&curDocJoinValue, sizeof(docid_t));
}

void ReclaimMap::InitSegmentIdxs(
        vector<int32_t> &segIdxs,
        const SegmentMergeInfos& mainSegMergeInfos)
{
    assert(segIdxs.empty());
    for (size_t i = 0; i < mainSegMergeInfos.size(); ++i)
    {
        segmentid_t segId = mainSegMergeInfos[i].segmentId;
        if ((size_t)segId >= segIdxs.size())
        {
            segIdxs.resize(segId + 1, -1);
        }
        segIdxs[segId] = (int32_t)i;
    }
}

void ReclaimMap::CheckMainAndSubMergeInfos(
        const SegmentMergeInfos& mainSegMergeInfos, 
        const SegmentMergeInfos& subSegMergeInfos)
{
    if (mainSegMergeInfos.size() != subSegMergeInfos.size())
    {
        INDEXLIB_FATAL_ERROR(InconsistentState,
                             "segment size of mainSegMergeInfo and subSegMergeInfo not match [%lu:%lu]",
                             mainSegMergeInfos.size(), subSegMergeInfos.size());
    }

    for (size_t i = 0; i < mainSegMergeInfos.size(); ++i)
    {
        if (mainSegMergeInfos[i].segmentId != subSegMergeInfos[i].segmentId)
        {
            INDEXLIB_FATAL_ERROR(InconsistentState,
                    "segmentId [idx:%lu] of mainSegMergeInfo and subSegMergeInfo not match [%d:%d]",
                    i, mainSegMergeInfos[i].segmentId, subSegMergeInfos[i].segmentId);
        }
    }
}

void ReclaimMap::InitJoinAttributeSegmentReaders(
        vector<AttributeSegmentReaderPtr>& attrSegReaders,
        const SegmentDirectoryBasePtr& mainSegmentDirectory,
        const AttributeConfigPtr& mainJoinAttrConfig,
        const SegmentMergeInfos& mainSegMergeInfos,
        bool multiTargetSegment)
{
    assert(attrSegReaders.empty());
    attrSegReaders.resize(mainSegMergeInfos.size(), AttributeSegmentReaderPtr());
    for(size_t i = 0; i < mainSegMergeInfos.size(); ++i)
    {
        FieldType fieldType = mainJoinAttrConfig->GetFieldType();
        const SegmentMergeInfo segMergeInfo = mainSegMergeInfos[i];
        if (segMergeInfo.segmentInfo.docCount == 0)
        {
            continue;
        }

        AttributeSegmentReaderPtr attrReaderPtr;
        //TODO: legacy for unitttest
        if (segMergeInfo.inMemorySegmentReader)
        {
            string attrName = mainJoinAttrConfig->GetAttrName();
            attrReaderPtr = segMergeInfo.inMemorySegmentReader->GetAttributeSegmentReader(
                    attrName);
        }
        else
        {
            if (multiTargetSegment)
            {
                unique_ptr<AttributeSegmentReader> attrReader(
                    ClassTypedFactory<AttributeSegmentReader, SingleValueAttributeSegmentReader,
                        const AttributeConfigPtr&>::GetInstance()
                        ->Create(fieldType, mainJoinAttrConfig));
                auto segData = mainSegmentDirectory->GetPartitionData()->GetSegmentData(
                    segMergeInfo.segmentId);
                auto attrDir
                    = segData.GetAttributeDirectory(mainJoinAttrConfig->GetAttrName(), true);
                AttributeSegmentPatchIteratorPtr emptyPatchIter;

#define OPEN_TYPED_READER(fieldType)                                                              \
    case fieldType:                                                                               \
    {                                                                                             \
        using ItemType = config::FieldTypeTraits<fieldType>::AttrItemType;                        \
        auto typedReader                                                                          \
            = static_cast<SingleValueAttributeSegmentReader<ItemType>*>(attrReader.get());        \
        typedReader->Open(segData, attrDir, emptyPatchIter);                                      \
        break;                                                                                    \
    }

                switch (fieldType)
                {
                    NUMBER_FIELD_MACRO_HELPER(OPEN_TYPED_READER);
                    OPEN_TYPED_READER(ft_hash_64);
                    OPEN_TYPED_READER(ft_hash_128);
                default:
                    assert(false);
                }
#undef OPEN_TYPED_READER
                attrReaderPtr.reset(attrReader.release());
            }
            else
            {
                unique_ptr<SingleAttributeSegmentReaderForMergeBase> attrReader(
                    ClassTypedFactory<SingleAttributeSegmentReaderForMergeBase,
                        SingleAttributeSegmentReaderForMerge,
                        const AttributeConfigPtr&>::GetInstance()
                        ->Create(fieldType, mainJoinAttrConfig));
                attrReader->Open(
                    mainSegmentDirectory, segMergeInfo.segmentInfo, segMergeInfo.segmentId);
                attrReaderPtr.reset(attrReader.release());
            }
        }
        attrSegReaders[i] = attrReaderPtr;
    }
}

void ReclaimMap::InitMultiComparator(
        const AttributeConfigVector& attributeConfigs, 
        const SortPatternVec& sortPatterns,
        MultiComparatorPtr multiComparator,
        DocInfoAllocatorPtr docInfoAllocator)
{
    assert(attributeConfigs.size() == sortPatterns.size());

    for(size_t i = 0; i < attributeConfigs.size(); ++i)
    {
        FieldType fieldType = attributeConfigs[i]->GetFieldType();
        Reference* refer = docInfoAllocator->DeclareReference(
                attributeConfigs[i]->GetAttrName(), fieldType);
        bool desc = (sortPatterns[i] != sp_asc);
        Comparator* comparator = ClassTypedFactory<
            Comparator, ComparatorTyped, Reference*, bool>::GetInstance()->Create(
                    fieldType, refer, desc);
        multiComparator->AddComparator(ComparatorPtr(comparator));
    }
}

void ReclaimMap::InitMultiAttrEvaluator(
        const SegmentMergeInfo& segMergeInfo,
        const AttributeConfigVector& attributeConfigs,
        const SegmentDirectoryBasePtr& segDir, 
        DocInfoAllocatorPtr docInfoAllocator,
        MultiAttributeEvaluatorPtr multiEvaluatorPtr)
{
    for(size_t i = 0; i < attributeConfigs.size(); ++i)
    {
        string attrName = attributeConfigs[i]->GetAttrName();
        FieldType fieldType = attributeConfigs[i]->GetFieldType();
        AttributeSegmentReaderPtr attrReaderPtr;

        //TODO: legacy for unitttest
        if (segMergeInfo.inMemorySegmentReader)
        {
            attrReaderPtr = segMergeInfo.inMemorySegmentReader->GetAttributeSegmentReader(
                    attrName);
        }
        else
        {
            attrReaderPtr = mAttrReaderContainer->GetAttributeSegmentReader(
                attrName, segMergeInfo.segmentId);
        }

        Reference* refer = docInfoAllocator->GetReference(attrName);
        Evaluator* evaluator = ClassTypedFactory<
            Evaluator, AttributeEvaluator, AttributeSegmentReaderPtr,
            Reference*>::GetInstance()->Create(fieldType, attrReaderPtr, refer);
        multiEvaluatorPtr->AddEvaluator(EvaluatorPtr(evaluator));        
    }
}

void ReclaimMap::SortByWeightInit(const SegmentMergeInfos& segMergeInfos, 
                                  const DeletionMapReaderPtr& deletionMapReader,
                                  const AttributeConfigVector& attributeConfigs,
                                  const SortPatternVec& sortPatterns,
                                  const SegmentDirectoryBasePtr& segDir)
{
    IE_LOG(INFO, "begin sortByWeight init");
    priority_queue<WeightedDocIterator*, vector<WeightedDocIterator*>,
                   WeightedDocIteratorComp> docHeap;

    MultiComparatorPtr comparator(new MultiComparator);
    DocInfoAllocatorPtr docInfoAllocator(new DocInfoAllocator);
    InitMultiComparator(attributeConfigs, sortPatterns, comparator, docInfoAllocator);

    IE_LOG(INFO, "init MultiComparator done");
    uint32_t maxDocCount = segMergeInfos.rbegin()->baseDocId + 
                           segMergeInfos.rbegin()->segmentInfo.docCount;
    mDocIdArray.resize(maxDocCount);
    SegmentMapper segMapper;
    if (NeedSplitSegment())
    {
        segMapper.Init(maxDocCount);
    }

    for (size_t i = 0; i < segMergeInfos.size(); i++)
    {
        if (segMergeInfos[i].segmentInfo.docCount == 0)
        {
            continue;
        }

        MultiAttributeEvaluatorPtr evaluatorPtr(new MultiAttributeEvaluator);
        InitMultiAttrEvaluator(segMergeInfos[i], attributeConfigs, segDir, 
                               docInfoAllocator, evaluatorPtr);

        WeightedDocIterator* docIter = new WeightedDocIterator(
                segMergeInfos[i], evaluatorPtr, comparator, docInfoAllocator);

        if (docIter->HasNext())
        {
            docIter->Next();
            docHeap.push(docIter);
        }
        else
        {
            delete docIter;
        }
    }
    
    IE_LOG(INFO, "begin heap sort");
    while (!docHeap.empty())
    {
        WeightedDocIterator* docIter = docHeap.top();
        docHeap.pop();
        docid_t baseId = docIter->GetBasedocId();
        docid_t localId = docIter->GetLocalDocId();
        segmentid_t segId = docIter->GetSegmentId();
        ReclaimOneDoc(segId, baseId, localId, deletionMapReader, segMapper);

        if (docIter->HasNext())
        {
            docIter->Next();
            docHeap.push(docIter);
        }
        else
        {
            delete docIter;
        }
    }
    if (NeedSplitSegment())
    {
        MakeTargetBaseDocIds(segMapper, segMapper.GetMaxSegmentIndex());
        RewriteDocIdArray(segMergeInfos, segMapper);
        segMapper.Clear();
    }
    IE_LOG(INFO, "end sortByWeight init");
}

void ReclaimMap::ReclaimOneDoc(
        segmentid_t segId, docid_t baseDocId, docid_t localId, 
        const DeletionMapReaderPtr& deletionMapReader,
        SegmentMapper& segMapper)
{
    auto oldGlobalId = baseDocId + localId;
    if (deletionMapReader->IsDeleted(segId, localId))
    {
        ++mDeletedDocCount;
        mDocIdArray[oldGlobalId] = INVALID_DOCID;
        return;
    }

    if (NeedSplitSegment())
    {
        auto segIndex = mSegmentSplitHandler(segId, localId);
        auto currentSegDocCount = segMapper.GetSegmentDocCount(segIndex);
        mDocIdArray[oldGlobalId] = static_cast<docid_t>(currentSegDocCount);
        segMapper.Collect(oldGlobalId, segIndex);
    }
    else
    {
        mDocIdArray[oldGlobalId] = mNewDocId++;
    }
}

void ReclaimMap::Init(uint32_t docCount)
{
    mDeletedDocCount = 0;
    mDocIdArray.resize(docCount);
}

void ReclaimMap::ReclaimOneSegment(
        segmentid_t segId, docid_t baseDocId, uint32_t docCount,
        const DeletionMapReaderPtr& deletionMapReader,
        SegmentMapper& segMapper)
{
    for (uint32_t j = 0; j < docCount; ++j)
    {
        ReclaimOneDoc(segId, baseDocId, j, deletionMapReader, segMapper);
    }
}

ReclaimMap* ReclaimMap::Clone() const
{
    return new ReclaimMap(*this);
}

void ReclaimMap::InitReverseDocIdArray(
        const SegmentMergeInfos& segMergeInfos)
{
    if (mNewDocId == 0)
    {
        return;
    }

    mReverseGlobalIdArray.reset(new GlobalIdVector);
    mReverseGlobalIdArray->Resize(mNewDocId);

    for (size_t i = 0; i < segMergeInfos.size(); i++)
    {
        uint32_t docCount = segMergeInfos[i].segmentInfo.docCount;
        docid_t baseDocId = segMergeInfos[i].baseDocId;
        for (uint32_t j = 0; j < docCount; j++)
        {
            docid_t newDocId = mDocIdArray[baseDocId + j];
            if (newDocId != INVALID_DOCID)
            {
                GlobalId gid = make_pair(segMergeInfos[i].segmentId, j);
                (*mReverseGlobalIdArray)[newDocId] = gid;
            }
        }
    }
}

template <typename T, typename SizeType>
bool ReclaimMap::LoadVector(FileReaderPtr& reader, vector<T>& vec)
{
    SizeType size = 0;
    if (reader->Read(&size, sizeof(size)) != sizeof(size))
    {
        IE_LOG(ERROR, "read vector size failed");
        return false;
    }
    if (size > 0)
    {
        vec.resize(size);
        int64_t expectedSize = sizeof(vec[0]) * size;
        if (reader->Read(vec.data(), expectedSize) != (size_t)expectedSize)
        {
            IE_LOG(ERROR, "read vector data failed");
            return false;
        }
    }
    return true;
}

template <typename T, typename SizeType>
void ReclaimMap::StoreVector(const FileWriterPtr& writer, const std::vector<T>& vec) const
{
    SizeType size = static_cast<SizeType>(vec.size());
    writer->Write(&size, sizeof(size));
    if (size > 0)
    {
        writer->Write(vec.data(), sizeof(vec[0]) * size);
    }
}

void ReclaimMap::StoreForMerge(const std::string &filePath,
                               const SegmentMergeInfos& segMergeInfos) const
{
    BufferedFileWriterPtr bufferedFileWriter(new BufferedFileWriter);
    bufferedFileWriter->Open(filePath);
    InnerStore(bufferedFileWriter, segMergeInfos);
}

void ReclaimMap::StoreForMerge(const DirectoryPtr &directory,
                               const string &fileName,
                               const SegmentMergeInfos& segMergeInfos) const
{
    FileWriterPtr writer = directory->CreateFileWriter(fileName);
    InnerStore(writer, segMergeInfos);
}

void ReclaimMap::InnerStore(const FileWriterPtr &writer,
                            const SegmentMergeInfos& segMergeInfos) const
{
    writer->Write(&mDeletedDocCount, sizeof(mDeletedDocCount));
    writer->Write(&mNewDocId, sizeof(mNewDocId));
    uint32_t docCount = mDocIdArray.size();
    writer->Write(&docCount, sizeof(docCount));
    for (size_t i = 0; i < segMergeInfos.size(); ++i) {
        writer->Write(mDocIdArray.data() + segMergeInfos[i].baseDocId,
                sizeof(mDocIdArray[0]) * segMergeInfos[i].segmentInfo.docCount);
    }
    uint32_t joinAttrCount = mJoinValueArray.size();
    assert(joinAttrCount == 0 || joinAttrCount == (uint32_t)mNewDocId);
    writer->Write(&joinAttrCount, sizeof(joinAttrCount));
    if (joinAttrCount > 0)
    {
        writer->Write(mJoinValueArray.data(), 
                sizeof(mJoinValueArray[0]) * joinAttrCount);
    }

    bool needReverseMapping = mReverseGlobalIdArray != NULL;
    writer->Write(&needReverseMapping, sizeof(needReverseMapping));

    StoreVector(writer, mTargetBaseDocIds);

    writer->Close();    
}

bool ReclaimMap::LoadDocIdArray(FileReaderPtr &reader,
                                const SegmentMergeInfos &segMergeInfos,
                                int64_t mergeMetaBinaryVersion)
{
    uint32_t docCount;
    uint32_t expectSize = sizeof(docCount);
    if (reader->Read(&docCount, expectSize) != expectSize)
    {
        IE_LOG(ERROR, "read doc count failed.");
        return false;
    }
    mDocIdArray.resize(docCount);

    if (mergeMetaBinaryVersion == -1)
    {
        expectSize = sizeof(mDocIdArray[0]) * docCount;
        if (reader->Read(&mDocIdArray[0], expectSize) != expectSize)
        {
            IE_LOG(ERROR, "read doc id array failed.");
            return false;
        }
    }
    else
    {
        for (size_t i = 0; i < segMergeInfos.size(); ++i) {
            expectSize = sizeof(mDocIdArray[0]) * segMergeInfos[i].segmentInfo.docCount;
            uint32_t actualSize = reader->Read(
                    &mDocIdArray[0] + segMergeInfos[i].baseDocId,expectSize);
            if (actualSize != expectSize)
            {
                IE_LOG(ERROR, "read docidarray[%u] failed. expect size [%u], "
                       "actual size [%u]", (uint32_t)i, expectSize, actualSize);
                return false;
            }
        }
    }

    return true;
}

bool ReclaimMap::InnerLoad(FileReaderPtr &reader,
                           const SegmentMergeInfos& segMergeInfos,
                           int64_t mergeMetaBinaryVersion)
{
    uint32_t expectSize = sizeof(mDeletedDocCount);
    if (reader->Read(&mDeletedDocCount, expectSize) != expectSize)
    {
        IE_LOG(ERROR, "read deleted doc count failed.");
        reader->Close();
        return false;
    }
    expectSize = sizeof(mNewDocId);
    if (reader->Read(&mNewDocId, expectSize) != expectSize)
    {
        IE_LOG(ERROR, "read new doc id failed.");
        reader->Close();
        return false;
    }
    if (!LoadDocIdArray(reader, segMergeInfos, mergeMetaBinaryVersion))
    {
        reader->Close();
        return false;
    }

    uint32_t joinAttrCount;
    expectSize = sizeof(joinAttrCount);
    if (reader->Read(&joinAttrCount, expectSize) != expectSize)
    {
        IE_LOG(ERROR, "read join attr count failed.");
        reader->Close();
        return false;
    }

    mJoinValueArray.resize(joinAttrCount);
    if (joinAttrCount > 0)
    {
        expectSize = sizeof(mJoinValueArray[0]) * joinAttrCount;
        if (reader->Read(&mJoinValueArray[0], expectSize) != expectSize)
        {
            IE_LOG(ERROR, "read join value array failed.");
            reader->Close();
            return false;
        }
    }

    bool needReverseMapping = false;
    expectSize = sizeof(needReverseMapping);
    if (reader->Read(&needReverseMapping, expectSize) != expectSize)
    {
        IE_LOG(ERROR, "read needReverseMapping failed");
        reader->Close();
        return false;
    }

    if (needReverseMapping)
    {
        InitReverseDocIdArray(segMergeInfos);
    }

    if (mergeMetaBinaryVersion >= 2)
    {
        if (!LoadVector(reader, mTargetBaseDocIds))
        {
            IE_LOG(ERROR, "load targetBaseDocIds failed");
            reader->Close();
            return false;
        }
    }
    else
    {
        mTargetBaseDocIds.clear();
    }
    reader->Close();
    return true;    
}

bool ReclaimMap::LoadForMerge(const string &filePath,
                              const SegmentMergeInfos &segMergeInfos,
                              int64_t mergeMetaBinaryVersion)
{
    BufferedFileReader* bufferedFileReader = new BufferedFileReader;
    bufferedFileReader->Open(filePath);
    FileReaderPtr reader(bufferedFileReader);
    return InnerLoad(reader, segMergeInfos, mergeMetaBinaryVersion);
}

bool ReclaimMap::LoadForMerge(const DirectoryPtr &directory,
                              const string &fileName,
                              const SegmentMergeInfos &segMergeInfos,
                              int64_t mergeMetaBinaryVersion)
{
    FileReaderPtr reader = directory->CreateFileReader(fileName, FSOpenType::FSOT_BUFFERED);
    reader->Open();
    return InnerLoad(reader, segMergeInfos, mergeMetaBinaryVersion);
}

int64_t ReclaimMap::EstimateMemoryUse() const
{
    int64_t memUse = 0;
    memUse += mDocIdArray.capacity() * sizeof(docid_t);
    memUse += mJoinValueArray.capacity() * sizeof(docid_t);
    memUse += mTargetBaseDocIds.capacity() & sizeof(mTargetBaseDocIds[0]);

    if (mReverseGlobalIdArray)
    {
        memUse += mReverseGlobalIdArray->Capacity() * sizeof(GlobalId);
    }
    return memUse;
}

IE_NAMESPACE_END(index);

