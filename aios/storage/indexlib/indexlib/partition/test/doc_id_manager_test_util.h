#pragma once

#include <memory>

#include "indexlib/common_define.h"
#include "indexlib/index/normal/attribute/accessor/join_docid_attribute_reader.h"
#include "indexlib/index/primary_key/mock/MockPrimaryKeyIndexReader.h"
#include "indexlib/index_base/segment/segment_writer.h"
#include "indexlib/partition/main_sub_doc_id_manager.h"
#include "indexlib/partition/modifier/partition_modifier.h"
#include "indexlib/partition/normal_doc_id_manager.h"
#include "indexlib/partition/segment/single_segment_writer.h"
#include "indexlib/test/test.h"
#include "indexlib/test/unittest.h"

DECLARE_REFERENCE_CLASS(index_base, BuildingSegmentData);

namespace indexlib { namespace partition {

class MockJoinDocIdAttributeReader : public index::JoinDocidAttributeReader
{
public:
    MockJoinDocIdAttributeReader(std::map<docid_t, docid_t> joinedBaseDocIds) : _joinedBaseDocIds(joinedBaseDocIds) {}
    ~MockJoinDocIdAttributeReader() {}

public:
    docid_t GetJoinDocId(docid_t docId) const override
    {
        docid_t joinDocId = INVALID_DOCID;
        if (_joinedBaseDocIds.find(docId) != _joinedBaseDocIds.end()) {
            joinDocId = _joinedBaseDocIds.at(docId);
        }
        return joinDocId;
    }

private:
    std::map<docid_t, docid_t> _joinedBaseDocIds;
};

class MockPartitionModifierForDocIdManager : public PartitionModifier
{
public:
    MockPartitionModifierForDocIdManager(
        const std::shared_ptr<index::MockPrimaryKeyIndexReader>& mockPrimaryKeyIndexReader = nullptr)
        : PartitionModifier(nullptr)
    {
        _mockPrimaryKeyIndexReader = mockPrimaryKeyIndexReader;
    }
    MOCK_METHOD(bool, DedupDocument, (const document::DocumentPtr&), (override));
    MOCK_METHOD(bool, UpdateDocument, (const document::DocumentPtr&), (override));
    MOCK_METHOD(bool, RemoveDocument, (const document::DocumentPtr&), (override));
    MOCK_METHOD(bool, RedoIndex, (docid_t, const document::ModifiedTokens&), (override));
    bool RemoveDocument(docid_t docid) override
    {
        deletedDocIds.insert(docid);
        if (_mockPrimaryKeyIndexReader) {
            _mockPrimaryKeyIndexReader->DeleteIfExist(docid);
        }
        return true;
    }

    MOCK_METHOD(void, Dump, (const file_system::DirectoryPtr&, segmentid_t), (override));

    MOCK_METHOD(bool, UpdateField, (docid_t, fieldid_t, const autil::StringView&, bool), (override));
    MOCK_METHOD(bool, UpdatePackField, (docid_t, packattrid_t, const autil::StringView&), (override));
    MOCK_METHOD(bool, UpdateIndex, (docid_t, const document::ModifiedTokens&), ());
    MOCK_METHOD(bool, UpdateIndex, (index::IndexUpdateTermIterator*), (override));

    MOCK_METHOD(bool, IsDirty, (), (const, override));
    MOCK_METHOD(docid_t, GetBuildingSegmentBaseDocId, (), (const, override));
    MOCK_METHOD(const index::PrimaryKeyIndexReaderPtr&, GetPrimaryKeyIndexReader, (), (const, override));
    MOCK_METHOD(index::PartitionInfoPtr, GetPartitionInfo, (), (const, override));
    MOCK_METHOD(PartitionModifierDumpTaskItemPtr, GetDumpTaskItem, (const std::shared_ptr<PartitionModifier>&),
                (const, override));

    std::set<docid_t> deletedDocIds;

private:
    std::shared_ptr<index::MockPrimaryKeyIndexReader> _mockPrimaryKeyIndexReader;
};

DEFINE_SHARED_PTR(MockPartitionModifierForDocIdManager);

// class MockSegmentWriterForDocIdManager : public index_base::SegmentWriter
// {
// public:
//     MockSegmentWriterForDocIdManager(const index::MockPrimaryKeyIndexReaderPtr& mockPrimaryKeyIndexReader = nullptr)
//         : index_base::SegmentWriter(/*schema=*/nullptr, config::IndexPartitionOptions())
//     {
//         _mockPrimaryKeyIndexReader = mockPrimaryKeyIndexReader;
//     }

//     MOCK_METHOD(void, CollectSegmentMetrics, (), (override));
//     MOCK_METHOD(index_base::SegmentWriter*, CloneWithNewSegmentData, (index_base::BuildingSegmentData&, bool),
//     (const, override)); MOCK_METHOD(bool, AddDocument, (const document::DocumentPtr&), (override)); MOCK_METHOD(bool,
//     IsDirty, (), (const, override)); MOCK_METHOD(void, EndSegment, (), (override)); MOCK_METHOD(void,
//     CreateDumpItems, (const file_system::DirectoryPtr&, std::vector<common::DumpItem*>&), (override));
//     MOCK_METHOD(index_base::InMemorySegmentReaderPtr, CreateInMemSegmentReader, (), (override));
//     MOCK_METHOD(const index_base::SegmentInfoPtr&, GetSegmentInfo, (), (const, override));
//     MOCK_METHOD(partition::InMemorySegmentModifierPtr, GetInMemorySegmentModifier, (), (override));

//     std::set<docid_t> deletedDocIds;

// private:
//     index::MockPrimaryKeyIndexReaderPtr _mockPrimaryKeyIndexReader;
// };

// class MockSegmentWriterForDocIdManager : public partition::SingleSegmentWriter
// {
// public:
//     MockSegmentWriterForDocIdManager(const index::MockPrimaryKeyIndexReaderPtr& mockPrimaryKeyIndexReader = nullptr)
//         : partition::SingleSegmentWriter(/*schema=*/nullptr, config::IndexPartitionOptions())
//     {
//         _mockPrimaryKeyIndexReader = mockPrimaryKeyIndexReader;
//         mCurrentSegmentInfo.reset(new index_base::SegmentInfo());
//     }

//     // MOCK_METHOD(void, EndAddDocument, (const document::DocumentPtr&), (override));
//     std::set<docid_t> deletedDocIds;

// private:
//     index::MockPrimaryKeyIndexReaderPtr _mockPrimaryKeyIndexReader;
// };
// DEFINE_SHARED_PTR(MockSegmentWriterForDocIdManager);

class DocIdManagerTestUtil
{
public:
    DocIdManagerTestUtil() = delete;
    ~DocIdManagerTestUtil() = delete;

public:
    static std::unique_ptr<NormalDocIdManager> CreateNormalDocIdManager(const config::IndexPartitionSchemaPtr& schema,
                                                                        bool batchMode,
                                                                        const std::vector<docid_t>& existDocIds,
                                                                        docid_t buildingSegmentBaseDocId,
                                                                        const std::string& workRoot);

    static std::unique_ptr<NormalDocIdManager>
    CreateNormalDocIdManager(const config::IndexPartitionSchemaPtr& schema, bool batchMode,
                             const std::map<std::string, docid_t>& pkToDocIdMap, docid_t buildingSegmentBaseDocId,
                             const index::PartitionInfoPtr& partitionInfo);

    static std::unique_ptr<MainSubDocIdManager>
    CreateMainSubDocIdManager(const config::IndexPartitionSchemaPtr& schema, bool batchMode,
                              const std::map<docid_t, std::vector<docid_t>>& mainDocIdToSubDocIds,
                              docid_t mainBuildingSegmentBaseDocId, docid_t subBuildingSegmentBaseDocId,
                              const std::string& workRoot);

    static bool CheckDeletedDocIds(const DocIdManager* docIdManager, const std::set<docid_t> expectDeletedMainDocIds,
                                   const std::set<docid_t> expectDeletedSubDocIds = {});

private:
    IE_LOG_DECLARE();
};

}} // namespace indexlib::partition
