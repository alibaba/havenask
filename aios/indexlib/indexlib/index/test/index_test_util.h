#ifndef __INDEXLIB_INDEX_TEST_UTIL_H
#define __INDEXLIB_INDEX_TEST_UTIL_H

#include "indexlib/common_define.h"
#include "indexlib/indexlib.h"
#include "indexlib/index_base/index_meta/segment_info.h"
#include "indexlib/index/normal/reclaim_map/reclaim_map.h"
#include "indexlib/index/normal/deletionmap/deletion_map_reader.h"
#include "indexlib/merger/segment_directory.h"
#include "indexlib/index/test/posting_maker.h"
#include "indexlib/index/normal/inverted_index/format/index_format_option.h"
#include "indexlib/index_base/segment/segment_data.h"
#include "indexlib/file_system/directory.h"
#include <tr1/memory>

IE_NAMESPACE_BEGIN(index);

#define FOR_EACH(x, array) for (uint32_t x = 0; x < sizeof(array) / sizeof(array[0]); ++x)

class IndexTestUtil
{
public:
    typedef PostingMaker::DocMap AnswerMap;
public:
    typedef bool (*ToDelete) (docid_t);

    enum DeletionMode
    {
        DM_NODELETE = 0,
        DM_DELETE_EVEN, 
        DM_DELETE_SOME, 
        DM_DELETE_MANY, 
        DM_DELETE_ALL,
        DM_MAX_MODE
    };

public:
    static bool NoDelete(docid_t docId)
    {
        return false;
    }

    static bool DeleteEven(docid_t docId)
    {
        return docId % 2 == 0;
    }

    static bool DeleteSome(docid_t docId)
    {
        return (docId % 7 == 3) || (docId % 11 == 5)
            || (docId % 13 == 7);
    }

    static bool DeleteMany(docid_t docId)
    {
        docid_t tmpDocId = docId % 16;
        return (tmpDocId < 10);
    }

    static bool DeleteAll(docid_t docId)
    {
        return true;
    }

    static DeletionMapReaderPtr CreateDeletionMapReader(
            const std::vector<uint32_t>& docNums);

    static DeletionMapReaderPtr CreateDeletionMap(const index_base::Version& version,
                                                  const std::vector<uint32_t>& docCounts, 
                                                  IndexTestUtil::ToDelete toDel);

    static DeletionMapReaderPtr CreateDeletionMap(
        const merger::SegmentDirectoryPtr& segDir,
        const std::vector<uint32_t>& docCounts,             
        const std::vector<std::set<docid_t> >& delDocIdSets);

    static void CreateDeletionMap(segmentid_t segId, uint32_t docCount, 
                                  IndexTestUtil::ToDelete toDel, 
                                  DeletionMapReaderPtr& deletionMap);

    static uint32_t GetTotalDocCount(const std::vector<uint32_t> &docCounts,
                              const std::vector<std::set<docid_t> > &delDocIdSets,
                              const index_base::SegmentMergeInfos &segMergeInfos);

    static std::vector<docid_t>
    GetTargetBaseDocIds(const std::vector<uint32_t> &docCounts,
                        const std::vector<std::set<docid_t> > &delDocIdSets,
                        index_base::SegmentMergeInfos& segMergeInfos,
                        uint32_t targetSegmentCount);

    static ReclaimMapPtr
    CreateReclaimMap(const std::vector<uint32_t> &docCounts,
                     const std::vector<std::set<docid_t> > &delDocIdSets,
                     bool needReverse = false,
                     const std::vector<docid_t> &targetBaseDocIds = {});

    static ReclaimMapPtr
    CreateReclaimMap(const std::vector<uint32_t> &docCounts,
                     const std::vector<std::vector<docid_t> > &delDocIds,
                     const std::vector<docid_t> &targetBaseDocIds = {});

    static ReclaimMapPtr
    CreateReclaimMap(const std::vector<uint32_t> &docCounts,
                     const std::vector<std::set<docid_t> > &delDocIdSets,
                     const std::vector<segmentid_t> &mergeSegIdVect,
                     const std::vector<docid_t> &targetBaseDocIds = {});

    // format:
    //     docCount: delDocId, delDocId...;
    //     docCount: delDocId, delDocId...;
    static ReclaimMapPtr
    CreateReclaimMap(const std::string &segmentDocIdDesc,
                     const std::vector<docid_t> &targetBaseDocIds = {});

    static ReclaimMapPtr CreateSortMergingReclaimMap(
        const std::vector<uint32_t> &docCounts,
        const std::vector<std::vector<docid_t> > &delDocIds,
        const std::vector<docid_t> &targetBaseDocIds = {});

    static ReclaimMapPtr CreateSortMergingReclaimMap(
        const std::vector<uint32_t> &docCounts,
        const std::vector<std::set<docid_t> > &delDocIdSets,
        const std::vector<docid_t> &targetBaseDocIds = {});

    static void ResetDir(const std::string& dir);
    static void MkDir(const std::string& dir);
    static void CleanDir(const std::string& dir);

    static index_base::Version CreateVersion(uint32_t segCount, segmentid_t baseSegId);

    static merger::SegmentDirectoryPtr CreateSegmentDirectory(
            const std::string& dir, uint32_t segCount, 
            segmentid_t baseSegId = 0);

    static merger::SegmentDirectoryPtr CreateSegmentDirectory(
            const std::string& dir,
            size_t segCount,
            segmentid_t baseSegId,
            versionid_t vid);

    static void BuildMultiSegmentsFromDataStrings(
            const std::vector<std::string>& dataStr,
            const std::string& filePath, 
            const std::vector<docid_t>& baseDocIds,
            std::vector<AnswerMap>& answerMaps,
            std::vector<uint8_t>& compressModes,
            const index::IndexFormatOption& indexFormatOption);

    static uint8_t BuildOneSegmentFromDataString(const std::string& dataStr,
            const std::string& filePath, docid_t baseDocId, AnswerMap& answerMap,
            const index::IndexFormatOption& indexFormatOption);

    static index_base::PartitionDataPtr CreatePartitionData(
            const file_system::IndexlibFileSystemPtr& fileSystem,
            uint32_t segCount, segmentid_t baseSegId = 0);

    static index_base::PartitionDataPtr CreatePartitionData(
            const file_system::IndexlibFileSystemPtr& fileSystem,
            index_base::Version version);

    static index_base::SegmentData CreateSegmentData(
            const file_system::DirectoryPtr& directory,
            const index_base::SegmentInfo& segmentInfo,
            segmentid_t segId,
            docid_t baseDocId);

private:
    static DeletionMapReaderPtr InnerCreateDeletionMapReader(
            const index_base::Version& version,
            const std::vector<uint32_t>& docCounts);
public:
    static ToDelete deleteFuncs[DM_MAX_MODE];
};

////////////////////////////////////////////////////////

IE_NAMESPACE_END(index);

#endif //__INDEXLIB_INDEX_TEST_UTIL_H
