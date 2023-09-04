#include "indexlib/table/normal_table/index_task/test/ReclaimMapUtil.h"

#include "gtest/gtest.h"

#include "indexlib/index/IIndexMerger.h"
#include "indexlib/index/deletionmap/Common.h"
#include "indexlib/index/deletionmap/DeletionMapDiskIndexer.h"

namespace indexlibv2::table {

void ReclaimMapUtil::PrepareSegments(const std::shared_ptr<indexlib::file_system::Directory>& rootDir,
                                     const std::vector<uint32_t>& docCounts, const std::vector<segmentid_t>& segIds,
                                     index::IndexTestUtil::ToDelete toDel,
                                     std::vector<index::IIndexMerger::SourceSegment>& sourceSegments)
{
    docid_t baseDocId = 0;
    for (size_t i = 0; i < segIds.size(); ++i) {
        segmentid_t segmentId = segIds[i];
        index::IIndexMerger::SourceSegment sourceSegment;
        sourceSegment.baseDocid = baseDocId;
        framework::SegmentInfo segInfo = MakeOneSegment(rootDir, segmentId, docCounts[i]);
        sourceSegment.segment = std::make_shared<FakeSegment>(framework::Segment::SegmentStatus::ST_UNSPECIFY);
        auto segMeta = std::make_shared<framework::SegmentMeta>(segmentId);
        auto segmentDirectory = MakeSegmentDirectory(rootDir, segmentId);
        segMeta->segmentDir = segmentDirectory;
        segMeta->segmentInfo.reset(new framework::SegmentInfo(segInfo));
        sourceSegment.segment->TEST_SetSegmentMeta(*segMeta);

        // Init deletion map
        auto indexer = std::make_shared<index::DeletionMapDiskIndexer>(segMeta->segmentInfo->docCount, segmentId);
        indexer->TEST_InitWithoutOpen();
        for (docid_t i = 0; i < segMeta->segmentInfo->docCount; i++) {
            if (toDel(i)) {
                auto status = indexer->Delete(i);
                assert(status.IsOK());
            }
        }
        sourceSegment.segment->AddIndexer(index::DELETION_MAP_INDEX_TYPE_STR, index::DELETION_MAP_INDEX_NAME, indexer);

        baseDocId += segInfo.docCount;
        sourceSegments.push_back(sourceSegment);
    }
}

framework::SegmentInfo ReclaimMapUtil::MakeOneSegment(const std::shared_ptr<indexlib::file_system::Directory>& rootDir,
                                                      segmentid_t segmentId, uint32_t docCount)
{
    auto segmentDirectory = MakeSegmentDirectory(rootDir, segmentId);
    framework::SegmentInfo segInfo;
    segInfo.docCount = docCount;
    segmentDirectory->RemoveFile(SEGMENT_INFO_FILE_NAME, indexlib::file_system::RemoveOption::MayNonExist());
    [[maybe_unused]] auto status = segInfo.Store(segmentDirectory->GetIDirectory());
    assert(status.IsOK());
    rootDir->Sync(true);
    return segInfo;
}

std::shared_ptr<indexlib::file_system::Directory>
ReclaimMapUtil::MakeSegmentDirectory(const std::shared_ptr<indexlib::file_system::Directory>& rootDir,
                                     segmentid_t segId)
{
    return rootDir->MakeDirectory(std::string("segment_") + autil::StringUtil::toString(segId) + "_level_0",
                                  indexlib::file_system::DirectoryOption::Mem());
}

void ReclaimMapUtil::CheckAnswer(const ReclaimMap& reclaimMap, const std::string& expectedDocIdStr,
                                 bool needReverseMapping, const std::string& expectedOldDocIdStr,
                                 const std::string& expectedSegIdStr)
{
    std::vector<docid_t> expectedNewDocIds;
    autil::StringUtil::fromString(expectedDocIdStr, expectedNewDocIds, ",");
    // check new docids
    docid_t oldDocId = 0;
    for (size_t i = 0; i < expectedNewDocIds.size(); i++) {
        if (expectedNewDocIds[i] == -2) {
            continue;
        }
        docid_t newDocId = reclaimMap.GetNewId(oldDocId);
        ASSERT_EQ(expectedNewDocIds[i], newDocId) << i;
        oldDocId++;
    }
    // check old docids and segment ids
    if (needReverseMapping) {
        std::vector<docid_t> expectedOldDocIds;
        std::vector<segmentid_t> expectedSegIds;
        autil::StringUtil::fromString(expectedOldDocIdStr, expectedOldDocIds, ",");
        autil::StringUtil::fromString(expectedSegIdStr, expectedSegIds, ",");
        ASSERT_EQ(expectedSegIds.size(), expectedOldDocIds.size());
        ASSERT_EQ(expectedSegIds.size(), reclaimMap.GetNewDocCount());
        for (uint32_t i = 0; i < reclaimMap.GetNewDocCount(); i++) {
            segmentid_t segId = INVALID_SEGMENTID;
            docid_t docId = INVALID_DOCID;
            reclaimMap.GetOldDocIdAndSegId((docid_t)i, docId, segId);
            ASSERT_EQ(expectedOldDocIds[i], docId);
            ASSERT_EQ(expectedSegIds[i], segId);
        }
    }
}

void ReclaimMapUtil::CreateTargetSegment(const std::shared_ptr<indexlib::file_system::Directory>& rootDir,
                                         segmentid_t targetSegmentId,
                                         const std::shared_ptr<framework::SegmentInfo>& lastSegmentInfo,
                                         indexlibv2::index::IIndexMerger::SegmentMergeInfos& segMergeInfos)
{
    indexlib::file_system::DirectoryPtr targetSegmentDirectory = MakeSegmentDirectory(rootDir, targetSegmentId);
    std::shared_ptr<framework::SegmentMeta> segMeta(new framework::SegmentMeta(targetSegmentId));
    segMeta->segmentDir = targetSegmentDirectory;

    auto targetSegInfo = std::make_shared<framework::SegmentInfo>();
    size_t docCount = 0;
    for (size_t i = 0; i < segMergeInfos.srcSegments.size(); i++) {
        auto sourceSegment = segMergeInfos.srcSegments[i].segment;
        const auto& sourceSegInfo = sourceSegment->GetSegmentInfo();
        docCount += sourceSegInfo->docCount;
        targetSegInfo->maxTTL = std::max(targetSegInfo->maxTTL, sourceSegInfo->maxTTL);
    }

    targetSegInfo->docCount = docCount;
    targetSegInfo->SetLocator(lastSegmentInfo->GetLocator());
    targetSegInfo->timestamp = lastSegmentInfo->timestamp;
    targetSegInfo->mergedSegment = true;

    segMeta->segmentInfo = targetSegInfo;
    segMergeInfos.targetSegments.push_back(segMeta);
}

} // namespace indexlibv2::table
