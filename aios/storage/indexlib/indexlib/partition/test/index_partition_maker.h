#pragma once

#include <memory>

#include "indexlib/common_define.h"
#include "indexlib/index/test/document_maker.h"
#include "indexlib/index/test/index_test_util.h"
#include "indexlib/indexlib.h"
#include "indexlib/partition/index_partition.h"

namespace indexlib { namespace partition {

class IndexPartitionMaker
{
public:
    typedef index::DocumentMaker::MockIndexPart MockIndexPart;
    typedef index::DocumentMaker::MockDeletionMap MockDeletionMap;
    typedef index::DocumentMaker::DocumentVect DocumentVect;

    typedef index::IndexTestUtil::DeletionMode DeletionMode;

public:
    IndexPartitionMaker();
    ~IndexPartitionMaker();

public:
    static void MakeOneSegmentData(uint32_t docCount, IndexPartition& indexPart4Write, MockIndexPart& mockIndexPart,
                                   int32_t pkStartSuffix, DeletionMode delMode = index::IndexTestUtil::DM_NODELETE,
                                   DocumentVect docsToUpdate = std::vector<document::NormalDocumentPtr>());

    static index::DocumentMaker::DocumentVect
    MakeOneSegmentDataWithoutDump(uint32_t docCount, IndexPartition& indexPart4Write, MockIndexPart& mockIndexPart,
                                  int32_t pkStartSuffix, DeletionMode delMode = index::IndexTestUtil::DM_NODELETE,
                                  DocumentVect docsToUpdate = std::vector<document::NormalDocumentPtr>());

    static void MakeMultiSegmentData(std::vector<uint32_t>& docCounts, IndexPartition& indexPart4Write,
                                     MockIndexPart& mockIndexPart, int32_t pkStartSuffix,
                                     DeletionMode delMode = index::IndexTestUtil::DM_NODELETE, bool isUpdate = false);

    static void MakeOneDelSegmentData(uint32_t docCount, IndexPartition& indexPart4Write, MockIndexPart& mockIndexPart,
                                      int32_t pkStartSuffix, DeletionMode delMode);

    static void MakeMultiSegmentsSortedData(std::vector<uint32_t>& docCounts, IndexPartitionPtr& indexPart4Write,
                                            MockIndexPart& mockIndexPart, int32_t pkStartSuffix,
                                            const std::string& sortFieldName,
                                            DeletionMode delMode = index::IndexTestUtil::DM_NODELETE,
                                            bool isUpdate = false);

    static void SortDocuments(std::vector<uint32_t>& docCounts, const config::AttributeConfigPtr& attrConfig,
                              DocumentVect& docVect);

    static void MultiSortDocuments(std::vector<uint32_t>& docCounts,
                                   std::vector<config::AttributeConfigPtr>& attrConfigs,
                                   std::vector<std::string>& sortPatterns, DocumentVect& docVect);

    static index::ReclaimMapPtr MakeSortedReclaimMap(const MockIndexPart& oldIndexPart,
                                                     const DocumentVect& sortedDocVec);

    static bool IsVersionFileExsit(const std::string& rootDir, versionid_t versionId);

private:
    static void InnerMakeMultiSegmentsSortedData(std::vector<uint32_t>& docCounts, IndexPartitionPtr& indexPart4Write,
                                                 MockIndexPart& mockIndexPart, DocumentVect& documentVect,
                                                 int32_t pkStartSuffix, const std::string& sortFieldName,
                                                 DeletionMode delMode, bool isUpdate = false);

    template <typename T>
    static void DoSortDocuments(std::vector<uint32_t>& srcDocCounts, fieldid_t fieldId, DocumentVect& docVect);

    static void MergeSortDocuments(std::vector<uint32_t>& srcDocCounts, const config::AttributeConfigPtr& attrConfig,
                                   DocumentVect& docVect);
    static void DoMultiSortDocuments(std::vector<uint32_t>& docCounts, std::vector<FieldType>& typeVec,
                                     std::vector<fieldid_t>& idVec, std::vector<std::string>& sortPatterns,
                                     DocumentVect& docVect);

    template <typename T>
    static void DoMergeSortDocuments(std::vector<uint32_t>& srcDocCounts, fieldid_t fieldId, DocumentVect& docVect);

    static void MakeDocuments(uint32_t docCount, const config::IndexPartitionSchemaPtr& schema, int32_t pkStartSuffix,
                              MockIndexPart& mockIndexPart, index::DocumentMaker::DocumentVect& segDocVect);

private:
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(IndexPartitionMaker);
}} // namespace indexlib::partition
