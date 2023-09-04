#include "indexlib/partition/test/doc_id_manager_test_util.h"

#include <algorithm>

#include "indexlib/config/index_partition_schema.h"
#include "indexlib/index/partition_info.h"
#include "indexlib/index/test/partition_info_creator.h"
#include "indexlib/partition/segment/single_segment_writer.h"

using namespace std;

namespace indexlib { namespace partition {
IE_LOG_SETUP(partition, DocIdManagerTestUtil);

std::unique_ptr<NormalDocIdManager>
DocIdManagerTestUtil::CreateNormalDocIdManager(const config::IndexPartitionSchemaPtr& schema, bool batchMode,
                                               const std::vector<docid_t>& existDocIds,
                                               docid_t buildingSegmentBaseDocId, const std::string& workRoot)
{
    assert(existDocIds.empty() || buildingSegmentBaseDocId > *std::max_element(existDocIds.begin(), existDocIds.end()));

    std::map<std::string, docid_t> pkToDocIdMap;
    for (docid_t docId : existDocIds) {
        pkToDocIdMap[std::to_string(docId)] = docId;
    }

    index::PartitionInfoPtr partitionInfo = index::PartitionInfoCreator::CreatePartitionInfo(
        /*incDocCounts=*/"",
        /*rtDocCounts=*/buildingSegmentBaseDocId == 0 ? "" : std::to_string(buildingSegmentBaseDocId), workRoot);

    return DocIdManagerTestUtil::CreateNormalDocIdManager(schema, batchMode, pkToDocIdMap, buildingSegmentBaseDocId,
                                                          partitionInfo);
}

std::unique_ptr<NormalDocIdManager> DocIdManagerTestUtil::CreateNormalDocIdManager(
    const config::IndexPartitionSchemaPtr& schema, bool batchMode, const std::map<std::string, docid_t>& pkToDocIdMap,
    docid_t buildingSegmentBaseDocId, const index::PartitionInfoPtr& partitionInfo)
{
    std::shared_ptr<index::MockPrimaryKeyIndexReader> mockReader(new index::MockPrimaryKeyIndexReader(pkToDocIdMap));

    MockPartitionModifierForDocIdManagerPtr mockModifier(new MockPartitionModifierForDocIdManager(mockReader));

    config::IndexPartitionOptions options;
    SingleSegmentWriterPtr segmentWriter(new SingleSegmentWriter(schema, options));

    segmentWriter->TEST_SetSegmentInfo(index_base::SegmentInfoPtr(new index_base::SegmentInfo()));
    auto manager = std::make_unique<NormalDocIdManager>(schema, /*enableAutoAdd2Update=*/false);
    manager->TEST_Init(mockReader, partitionInfo, buildingSegmentBaseDocId, mockModifier, segmentWriter,
                       batchMode ? PartitionWriter::BuildMode::BUILD_MODE_CONSISTENT_BATCH
                                 : PartitionWriter::BuildMode::BUILD_MODE_STREAM,
                       false);
    return manager;
}

std::unique_ptr<MainSubDocIdManager>
DocIdManagerTestUtil::CreateMainSubDocIdManager(const config::IndexPartitionSchemaPtr& schema, bool batchMode,
                                                const std::map<docid_t, std::vector<docid_t>>& mainDocIdToSubDocIds,
                                                docid_t mainBuildingSegmentBaseDocId,
                                                docid_t subBuildingSegmentBaseDocId, const std::string& workRoot)
{
    assert(schema && schema->GetSubIndexPartitionSchema());

    std::vector<docid_t> allMainDocIds;
    std::vector<docid_t> allSubDocIds;
    std::map<docid_t, docid_t> mainDocIdToSubDocIdsEnd;
    for (auto iter = mainDocIdToSubDocIds.begin(); iter != mainDocIdToSubDocIds.end(); ++iter) {
        docid_t mainDocId = iter->first;
        assert(allMainDocIds.empty() || *allMainDocIds.rbegin() < mainDocId); // input main docids should be increasing

        for (docid_t skipedDocId = allMainDocIds.empty() ? 0 : (*allMainDocIds.rbegin()) + 1; skipedDocId < mainDocId;
             ++skipedDocId) {
            mainDocIdToSubDocIdsEnd[skipedDocId] = (*allSubDocIds.rbegin()) + 1;
        }

        allMainDocIds.push_back(mainDocId);
        for (docid_t subDocId : iter->second) {
            assert(allSubDocIds.empty() || *allSubDocIds.rbegin() < subDocId); // input sub docids should be increasing
            allSubDocIds.push_back(subDocId);
        }
        mainDocIdToSubDocIdsEnd[mainDocId] = (allSubDocIds.size() == 0 ? 0 : (*allSubDocIds.rbegin()) + 1);
    }
    assert(mainBuildingSegmentBaseDocId > (allMainDocIds.empty() ? -1 : *allMainDocIds.rbegin()));
    assert(subBuildingSegmentBaseDocId > (allSubDocIds.empty() ? -1 : *allSubDocIds.rbegin()));
    for (docid_t mainDocId = (allMainDocIds.empty() ? 0 : *allMainDocIds.rbegin() + 1);
         mainDocId < mainBuildingSegmentBaseDocId; ++mainDocId) {
        mainDocIdToSubDocIdsEnd[mainDocId] = subBuildingSegmentBaseDocId;
    }

    auto manager = std::make_unique<MainSubDocIdManager>(schema);
    manager->TEST_Init(
        CreateNormalDocIdManager(schema, batchMode, allMainDocIds, mainBuildingSegmentBaseDocId, workRoot),
        CreateNormalDocIdManager(schema->GetSubIndexPartitionSchema(), batchMode, allSubDocIds,
                                 subBuildingSegmentBaseDocId, workRoot),
        index::JoinDocidAttributeReaderPtr(new MockJoinDocIdAttributeReader(mainDocIdToSubDocIdsEnd)));
    return manager;
}

bool DocIdManagerTestUtil::CheckDeletedDocIds(const DocIdManager* docIdManager,
                                              const std::set<docid_t> expectDeletedMainDocIds,
                                              const std::set<docid_t> expectDeletedSubDocIds)
{
    const MainSubDocIdManager* mainSubDocIdManager = dynamic_cast<const MainSubDocIdManager*>(docIdManager);
    if (mainSubDocIdManager) {
        return DocIdManagerTestUtil::CheckDeletedDocIds(mainSubDocIdManager->GetMainDocIdManager(),
                                                        expectDeletedMainDocIds, {}) &&
               DocIdManagerTestUtil::CheckDeletedDocIds(mainSubDocIdManager->GetSubDocIdManager(),
                                                        expectDeletedSubDocIds, {});
    }

    const NormalDocIdManager* normalDocIdManager = dynamic_cast<const NormalDocIdManager*>(docIdManager);
    assert(normalDocIdManager);

    if (!expectDeletedSubDocIds.empty()) {
        IE_LOG(ERROR, "it's a NormalDocIdManager but expectd delete sub doc ids [%s]",
               autil::StringUtil::toString(expectDeletedSubDocIds.begin(), expectDeletedSubDocIds.end()).c_str());
        return false;
    }
    PartitionModifierPtr modifier = normalDocIdManager->TEST_GetPartitionModifier();
    MockPartitionModifierForDocIdManagerPtr mockModifier =
        DYNAMIC_POINTER_CAST(MockPartitionModifierForDocIdManager, modifier);
    assert(mockModifier);
    const std::set<docid_t>& actuallyDeletedDocIds = mockModifier->deletedDocIds;

    bool isEqual = true;
    if (actuallyDeletedDocIds.size() == expectDeletedMainDocIds.size()) {
        for (docid_t docId : actuallyDeletedDocIds) {
            if (0 == expectDeletedMainDocIds.count(docId)) {
                isEqual = false;
                break;
            }
        }
    } else {
        isEqual = false;
    }
    if (!isEqual) {
        IE_LOG(ERROR, "DocIdManager check deleted doc ids failed, expectd [%s], actually [%s]",
               autil::StringUtil::toString(expectDeletedMainDocIds.begin(), expectDeletedMainDocIds.end(), ",").c_str(),
               autil::StringUtil::toString(actuallyDeletedDocIds.begin(), actuallyDeletedDocIds.end(), ",").c_str());
    }
    return isEqual;
}

}} // namespace indexlib::partition
