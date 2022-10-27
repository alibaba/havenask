#ifndef __INDEXLIB_ONLINEPARTITIONINTETEST_H
#define __INDEXLIB_ONLINEPARTITIONINTETEST_H

#include "indexlib/common_define.h"

#include "indexlib/test/test.h"
#include "indexlib/test/unittest.h"
#include "indexlib/config/index_partition_options.h"
#include "indexlib/test/partition_state_machine.h"
#include "indexlib/partition/online_partition.h"

DECLARE_REFERENCE_CLASS(file_system, Directory);
IE_NAMESPACE_BEGIN(partition);

class OnlinePartitionInteTest : 
    public INDEXLIB_TESTBASE_WITH_PARAM<std::tr1::tuple<bool, bool> >
{
public:
    OnlinePartitionInteTest();
    ~OnlinePartitionInteTest();

    DECLARE_CLASS_NAME(OnlinePartitionInteTest);
public:
    void CaseSetUp() override;
    void CaseTearDown() override;

public:
    void TestOpenFullIndex();
    void TestLoadIndexWithMultiThreadLoadPatch();
    void TestSubSchemaLoadIndexWithMultiThreadLoadPatch(); 
    void TestOpenFullIndexWithBitmapIndex();
    void TestOpenSpecificIncIndex();

    void TestRtAddDocWithBuildingSegment();
    void TestRtAddDocWithBuiltAndBuildingSegment();
    void TestRtAddDocHavingSamePkWithIncDoc();
    void TestRtAddDocWithSamePkInBuildingSegment();
    void TestRtAddDocWithSamePkInBuiltSegment();
    void TestRtAddDocWithDocByTsFiltered();
    void TestRtAddDocWithDocByTsFilteredByIncLocator();
    void TestAddIncDocTsBetweenTwoRtDocsWithSamePk(); // kelude #5395815

    void TestRtRemoveRtDoc();
    void TestRtRemoveIncDoc();
    void TestRtRemoveArrivingIncDoc();
    void TestRtRemoveCurrentAndArrivingIncDoc();
    void TestRtRemoveWithDocByTsFiltered();
    void TestRtRemoveRepeatedly();

    void TestRtUpdateDocument();
    void TestRtUpdateRtDoc();
    void TestRtUpdateIncDoc();
    void TestRtUpdateArrivingIncDoc();
    void TestRtUpdateCurrentAndArrivingIncDoc();
    void TestRtUpdateWithDocByTsFiltered();
    void TestRtUpdateWithUnupdatableFieldDoc();
    void TestRtUpdateDocWithoutAttributeSchema();

    void TestReopenIncIndex();
    void TestReopenIncIndexMultiTimes();
    void TestReopenSpecificIncIndex();
    void TestReopenIncIndexWithIOException();

    void TestReopenIncIndexWithRtNotReclaimed();
    void TestReopenIncIndexWithRtAllReclaimed();
    void TestReopenIncIndexWithRtPartlyReclaimed();
    
    //void TestReopenIncIndexWithRtBuildFilterByIncLocator();

    void TestReopenIncIndexWithSamePkRtDocsNotReclaimed();
    void TestReopenIncIndexWithSamePkRtDocsPartlyReclaimed();

    void TestReopenIncIndexWithRtReclaimedBySameDocAndSameTs();// kelude #5398149
    void TestReopenIncIndexWithRtReclaimedTwice();
    void TestReopenIncIndexWhenRtIndexTimestampEqualToInc();
    void TestReopenIncIndexWithRtUpdateInc();
    void TestReopenIncTwiceWithNoRtAfterFirstInc();

    void TestReopenIncWithAddAddDelCombo();
    void TestReopenIncWithAddDelAddCombo();

    void TestReopenIncIndexWithTrim();
    void TestReopenIncIndexWithMultiNewVersion();
    void TestReopenWithRollbackVersion();
    void TestReopenExecutorMemControl();

    void TestPartitionInfoWithOpenFullIndex();
    void TestPartitionInfoWithRtIndex();
    void TestPartitionInfoWithRtReopen();
    void TestPartitionInfoWithIncReopen();

    void TestPartitionMetrics();
    void TestReaderContainerMetrics(); 
    void TestPartitionMemoryMetrics();
    void TestAddVirtualAttributes();
    void TestCleanVirtualAttributes();
    void TestForceReOpenCleanVirtualAttributes();
    void TestRtBuildFromEmptyVersionAndIncReopen();

    void TestCleanReadersAndInMemoryIndex();
    void TestCleanOnDiskIndex();
    void TestCleanOnDiskIndexMultiPath();
    void TestCleanOnDiskIndexSecondaryPath();
    void TestCleanOnDiskIndexWithVersionHeldByReader();
    
    void TestRecycleUpdateVarAttributeMemory();
    void TestRecycleSliceWithReaderAccess();
    void TestAutoRecycleUpdateVarAttributeMem();

    void TestUpdateCompressValueLongCaseTest();

    void TestOptimizedReopenForIncNewerThanRtLongCaseTest();
    void TestOptimizedReopenForAllModifiedOnDiskDocUnchanged();
    void TestOptimizedReopenForAllModifiedOnDiskDocChanged();
    void TestOptimizedReopenForAllModifiedRtDocBeenCovered();
    void TestOptimizedReopenForAllModifiedRtDocNotBeenCovered();
    void TestOptimizedReopenForDeleteDocLongCaseTest();

    void TestGetDocIdAfterUpdateDocumentLongCaseTest();

    void TestBuildRtDocsFilteredByIncTimestampLongCaseTest();
    void TestIncCoverRtWithoutPk();
    void TestCombinedPkSegmentCleanSliceFile();

    void TestEnableOpenLoadSpeedLimit();

    void TestPreloadPatchFile();
    void TestForceReopenWithSpecificVersion();
    void TestReopenForCompressOperationBlocks();
    void TestReopenExceedMaxReopenMemoryUse();
    void TestReopenForCombineHashPk();
    void TestIncExandMaxReopenMemUse();
    void TestSegmentDirNameCompatibility();
    void TestReopenForNumberHashPk();
    void TestReopenForMurMurHashPk();
    void TestReopenForReaderHashId();

    void TestLatestNeedSkipIncTimestamp();
    void TestAsyncDumpWithRead();
    void TestAsyncDumpWithIncReopen();
    void TestBuildThreadReopenNewSegment();
    void TestRedoWithMultiDumpingSegment();
    void TestSharedRealtimeQuotaControl();
    void TestSyncRealtimeQuota();
    void TestIncCoverPartialRtWithVersionTsLTStopTs();
    void TestMultiThreadInitAttributeReader();
    void TestAttributeFloatCompress();
    void TestAttributeFloatPackCompress();
    void TestAttributeFloatCompressUpdateField();

private:
    void DoTestReopenIncWithMultiDocsCombo(
            const std::string& docs, const std::string& expectedResults,
            const std::string& query, const config::IndexPartitionOptions& options);

    void CheckMetricsValue(const test::PartitionStateMachine& psm,
                           const std::string& name,
                           const std::string& unit,
                           double value);
    void CheckMetricsMaxValue(const test::PartitionStateMachine& psm,
                              const std::string& name,
                              const std::string& unit,
                              kmonitor::MetricType type,
                              double value);

    void CheckVirtualReader(const OnlinePartitionPtr& partition, 
                            const std::string& attrName, 
                            int32_t docCount, const std::string& expectValues,
                            bool isSub=false);
    // void InnerTestAddVirtualAttributes(bool hasRepeated);
    std::string PrepareLongMultiValue(size_t numOfValue, size_t value);

    util::BytesAlignedSliceArrayPtr GetSliceFileArray(
            const partition::IndexPartitionPtr& indexPartition, const std::string& fileName);

    void TestGetDocIdWithCommand(std::string command);

    void DoTestOptimizedReopen(const std::string& fullDocSeq,
                               const std::string& rtDocSeq,
                               const std::string& incDocSeq,
                               bool isIncMerge);

    void DoTestReopenWithCompressOperationsForBuildingSegment(
            const std::string& fullDocSeq,
            const std::string& rtDocSeq,
            const std::string& incDocSeq,
            bool isIncMerge);

    void DoTestReopenWithCompressOperationsForBuiltSegment(
            const std::string& fullDocSeq,
            const std::string& rtDocSeq,
            const std::string& incDocSeq,
            bool isIncMerge);

    void DoTestOptimizedReopenForDelete(
            const std::string& fullDocSeq, const std::string& rtDocSeq, 
            const std::string& incDocSeq, bool isIncMerge,
            const std::string& expectDocids);

    std::string GetDoc(const std::vector<std::string>& docs,
                       const std::string& docSeq);

    std::string MakeDocs(const std::string& docsStr, int64_t timeStamp);
    void MakeSchema(bool needSub);

    void DoTestRtUpdateCurrentAndArrivingIncDoc(PrimaryKeyHashType hashType);
    void DoTestReopenIncIndexWithSamePkRtDocsNotReclaimed(PrimaryKeyHashType hashType);

protected:
    void DoTestReopenWithRollbackVersion(bool forceReopen);
    OnlinePartitionPtr CreatePartitionForRealtimeMemoryLimit(
            const config::IndexPartitionSchemaPtr& schema, const std::string& path,
            const partition::IndexPartitionResource& partitionResource);

protected:
    std::string mRootPath;
    file_system::DirectoryPtr mRootDirectory;
    config::IndexPartitionOptions mOptions;
    config::IndexPartitionSchemaPtr mSchema;
    test::PsmEvent mBuildIncEvent;
    bool mFlushRtIndex;
private:
    IE_LOG_DECLARE();
};

IE_NAMESPACE_END(partition);

#endif //__INDEXLIB_ONLINEPARTITIONINTETEST_H
