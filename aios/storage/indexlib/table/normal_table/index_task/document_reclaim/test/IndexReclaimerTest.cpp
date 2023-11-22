#include "indexlib/index/deletionmap/DeletionMapPatchWriter.h"
#include "indexlib/index/inverted_index/InvertedIndexReader.h"
#include "indexlib/index/inverted_index/MultiFieldIndexReader.h"
#include "indexlib/index/inverted_index/PostingIterator.h"
#include "indexlib/index/inverted_index/SectionAttributeReader.h"
#include "indexlib/index/inverted_index/config/SingleFieldIndexConfig.h"
#include "indexlib/index/inverted_index/format/TermMeta.h"
#include "indexlib/table/normal_table/index_task/document_reclaim/AndIndexReclaimer.h"
#include "indexlib/table/normal_table/index_task/document_reclaim/IndexFieldReclaimer.h"
#include "indexlib/util/PoolUtil.h"
#include "unittest/unittest.h"

namespace indexlibv2::table {
class FakePostingIterator : public indexlib::index::PostingIterator
{
public:
    FakePostingIterator(std::vector<docid_t> docIds) : _docIds(docIds), _termMeta(docIds.size(), docIds.size()) {}

public:
    indexlib::index::TermMeta* GetTermMeta() const override
    {
        return const_cast<indexlib::index::TermMeta*>(&_termMeta);
    }

    docid64_t SeekDoc(docid64_t docId) override
    {
        while (_cur < _docIds.size() && _docIds[_cur] < docId) {
            ++_cur;
        }
        return _cur < _docIds.size() ? _docIds[_cur] : INVALID_DOCID;
    }

    indexlib::index::ErrorCode SeekDocWithErrorCode(docid64_t, docid64_t&) override
    {
        return indexlib::index::ErrorCode::UnImplement;
    }

    bool HasPosition() const override { return false; }

    void Unpack(indexlib::index::TermMatchData& termMatchData) override { assert(false); }

    void Reset() override { _cur = 0; }

    PostingIterator* Clone() const override { return nullptr; }

protected:
    size_t _cur = 0;
    std::vector<docid_t> _docIds;
    indexlib::index::TermMeta _termMeta;
};

using Doc = std::vector<std::string>;
class FakeIndexReader : public indexlib::index::InvertedIndexReader
{
public:
    FakeIndexReader(const std::vector<Doc>& docs, fieldid_t fieldId) : _docs(docs), _fieldId(fieldId) {}

    Status Open(const std::shared_ptr<indexlibv2::config::IIndexConfig>& indexConfig,
                const indexlibv2::framework::TabletData* tabletData) override
    {
        assert(false);
        return Status::Unimplement();
    }

    indexlib::index::Result<indexlib::index::PostingIterator*> Lookup(const indexlib::index::Term&, uint32_t,
                                                                      PostingType, autil::mem_pool::Pool*) override
    {
        return nullptr;
    }

    future_lite::coro::Lazy<indexlib::index::Result<indexlib::index::PostingIterator*>>
    LookupAsync(const indexlib::index::Term* term, uint32_t statePoolSize, PostingType type,
                autil::mem_pool::Pool* pool, indexlib::file_system::ReadOption option) noexcept override
    {
        assert(false);
        co_return nullptr;
    }

    // ranges must be sorted and do not intersect
    indexlib::index::Result<indexlib::index::PostingIterator*> PartialLookup(const indexlib::index::Term& term,
                                                                             const DocIdRangeVector& ranges, uint32_t,
                                                                             PostingType,
                                                                             autil::mem_pool::Pool* pool) override
    {
        std::vector<docid_t> docIds;
        for (auto [baseDocId, endDocId] : ranges) {
            for (docid_t docId = baseDocId; docId < endDocId; ++docId) {
                if (term.GetWord() == _docs[docId][_fieldId]) {
                    docIds.push_back(docId);
                }
            }
        }
        return IE_POOL_COMPATIBLE_NEW_CLASS(pool, FakePostingIterator, docIds);
    }

    const indexlib::index::SectionAttributeReader* GetSectionReader(const std::string&) const override
    {
        return nullptr;
    };
    std::shared_ptr<indexlib::index::KeyIterator> CreateKeyIterator(const std::string&) override { return nullptr; }

private:
    bool GetSegmentPosting(const indexlib::index::DictKeyInfo& key, uint32_t segmentIdx,
                           indexlib::index::SegmentPosting& segPosting, indexlib::file_system::ReadOption readOption,
                           indexlib::index::InvertedIndexSearchTracer* tracer) override
    {
        assert(false);
        return false;
    }
    future_lite::coro::Lazy<indexlib::index::Result<bool>>
    GetSegmentPostingAsync(const indexlib::index::DictKeyInfo& key, uint32_t segmentIdx,
                           indexlib::index::SegmentPosting& segPosting, indexlib::file_system::ReadOption option,
                           indexlib::index::InvertedIndexSearchTracer* tracer) noexcept override
    {
        assert(false);
        co_return false;
    }
    void SetAccessoryReader(const std::shared_ptr<indexlib::index::IndexAccessoryReader>& accessorReader) override {}

private:
    using Doc = std::vector<std::string>;
    const std::vector<Doc>& _docs;
    fieldid_t _fieldId;
};

class IndexReclaimerTest : public TESTBASE
{
public:
    IndexReclaimerTest() = default;
    ~IndexReclaimerTest() = default;

public:
    void setUp() override;
    void tearDown() override;
};

void IndexReclaimerTest::setUp() {}

void IndexReclaimerTest::tearDown() {}

std::map<std::string, fieldid_t> INDEX_NAME_TO_FIELDID = {{"f0", 0}, {"f1", 1}, {"f2", 2}, {"f3", 3}, {"f4", 4},
                                                          {"f5", 5}, {"f6", 6}, {"f7", 7}, {"f8", 8}, {"f9", 9}};

std::map<segmentid_t, docid_t> GenerateSegment2BaseDocId(const std::map<segmentid_t, size_t>& segmentId2DocCount)
{
    std::map<segmentid_t, docid_t> segment2BaseDocId;
    docid_t baseDocId = 0;
    for (auto [segId, docCount] : segmentId2DocCount) {
        segment2BaseDocId[segId] = baseDocId;
        baseDocId += docCount;
    }
    return segment2BaseDocId;
}

std::vector<Doc> MakeDocs(const std::map<segmentid_t, size_t>& segmentId2DocCount, const std::vector<size_t>& maxValues)
{
    std::vector<Doc> docs;
    docid_t docId = 0;
    for (auto [_, docCount] : segmentId2DocCount) {
        for (size_t i = 0; i < docCount; ++i, ++docId) {
            Doc doc;
            for (auto maxValue : maxValues) {
                doc.push_back(std::to_string(docId % maxValue));
            }
            docs.push_back(doc);
        }
    }
    return docs;
}

void CheckDeleter(index::DeletionMapPatchWriter* docDeleter, const std::vector<Doc>& docs,
                  const IndexReclaimerParam& reclaimParam, const std::map<segmentid_t, docid_t>& segment2BaseDocId)
{
    std::vector<docid_t> docIds;
    for (auto& [segId, bitmap] : docDeleter->GetSegmentId2Bitmaps()) {
        docid_t baseDocId = segment2BaseDocId.at(segId);
        for (uint32_t docId = bitmap->Begin(); docId != indexlib::util::Bitmap::INVALID_INDEX;
             docId = bitmap->Next(docId)) {
            docIds.push_back(baseDocId + docId);
        }
    }

    std::vector<docid_t> expected;
    for (docid_t docId = 0; docId < docs.size(); ++docId) {
        if (reclaimParam.GetReclaimOperator() == IndexReclaimerParam::AND_RECLAIM_OPERATOR) {
            if (reclaimParam.GetReclaimOprands().empty()) {
                continue;
            }
            bool yes = true;
            for (auto oprand : reclaimParam.GetReclaimOprands()) {
                fieldid_t fieldId = INDEX_NAME_TO_FIELDID.at(oprand.indexName);
                yes &= docs[docId][fieldId] == oprand.term;
            }
            if (yes) {
                expected.push_back(docId);
            }
        } else {
            fieldid_t fieldId = INDEX_NAME_TO_FIELDID.at(reclaimParam.GetReclaimIndex());
            for (auto term : reclaimParam.GetReclaimTerms()) {
                if (term == docs[docId][fieldId]) {
                    expected.push_back(docId);
                    break;
                }
            }
        }
    }
    assert(!expected.empty());
    ASSERT_EQ(expected, docIds);
}

TEST_F(IndexReclaimerTest, testAndIndexReclaimer)
{
    std::string paramJson = R"(
    {
        "reclaim_operator": "AND",
        "reclaim_index_info" : [
            {"reclaim_index": "f0", "reclaim_term" : "2"},
            {"reclaim_index": "f1", "reclaim_term" : "8"}
        ]  
    })";
    IndexReclaimerParam reclaimParam;
    autil::legacy::FromJsonString(reclaimParam, paramJson);

    std::map<segmentid_t, size_t> segmentId2DocCount = {{0, 0}, {1, 10}, {2, 20}, {3, 30}};
    std::map<segmentid_t, docid_t> segment2BaseDocId = GenerateSegment2BaseDocId(segmentId2DocCount);
    AndIndexReclaimer reclaimer(nullptr, reclaimParam, segment2BaseDocId, segmentId2DocCount);
    std::vector<Doc> docs = MakeDocs(segmentId2DocCount, {4, 10});

    auto readers = std::make_shared<indexlib::index::MultiFieldIndexReader>(nullptr);
    auto indexConfig = std::make_shared<indexlibv2::config::SingleFieldIndexConfig>("f0", it_string);
    indexConfig->SetIndexId(0);
    ASSERT_TRUE(readers->AddIndexReader(indexConfig.get(), std::make_shared<FakeIndexReader>(docs, 0)).IsOK());

    indexConfig = std::make_shared<indexlibv2::config::SingleFieldIndexConfig>("f1", it_string);
    indexConfig->SetIndexId(1);
    ASSERT_TRUE(readers->AddIndexReader(indexConfig.get(), std::make_shared<FakeIndexReader>(docs, 1)).IsOK());

    ASSERT_TRUE(reclaimer.Init(readers));
    index::DeletionMapPatchWriter docDeleter(nullptr, segmentId2DocCount);
    ASSERT_TRUE(reclaimer.Reclaim(&docDeleter).IsOK());
    CheckDeleter(&docDeleter, docs, reclaimParam, segment2BaseDocId);
}

TEST_F(IndexReclaimerTest, testAndIndexReclaimer_2)
{
    std::string paramJson = R"(
    {
        "reclaim_operator": "AND",
        "reclaim_index_info" : [
            {"reclaim_index": "f0", "reclaim_term" : "3"},
            {"reclaim_index": "f1", "reclaim_term" : "7"}
        ]  
    })";
    IndexReclaimerParam reclaimParam;
    autil::legacy::FromJsonString(reclaimParam, paramJson);

    std::map<segmentid_t, size_t> segmentId2DocCount = {{0, 123}, {1, 1110}, {2, 20}, {3, 30123}, {4, 999}};
    std::map<segmentid_t, docid_t> segment2BaseDocId = GenerateSegment2BaseDocId(segmentId2DocCount);
    AndIndexReclaimer reclaimer(nullptr, reclaimParam, segment2BaseDocId, segmentId2DocCount);
    std::vector<Doc> docs = MakeDocs(segmentId2DocCount, {11, 29});

    auto readers = std::make_shared<indexlib::index::MultiFieldIndexReader>(nullptr);
    auto indexConfig = std::make_shared<indexlibv2::config::SingleFieldIndexConfig>("f0", it_string);
    indexConfig->SetIndexId(0);
    ASSERT_TRUE(readers->AddIndexReader(indexConfig.get(), std::make_shared<FakeIndexReader>(docs, 0)).IsOK());

    indexConfig = std::make_shared<indexlibv2::config::SingleFieldIndexConfig>("f1", it_string);
    indexConfig->SetIndexId(1);
    ASSERT_TRUE(readers->AddIndexReader(indexConfig.get(), std::make_shared<FakeIndexReader>(docs, 1)).IsOK());

    ASSERT_TRUE(reclaimer.Init(readers));
    index::DeletionMapPatchWriter docDeleter(nullptr, segmentId2DocCount);
    ASSERT_TRUE(reclaimer.Reclaim(&docDeleter).IsOK());
    CheckDeleter(&docDeleter, docs, reclaimParam, segment2BaseDocId);
}

TEST_F(IndexReclaimerTest, testAndIndexReclaimer_3)
{
    std::string paramJson = R"(
    {
        "reclaim_operator": "AND",
        "reclaim_index_info" : [
            {"reclaim_index": "f0", "reclaim_term" : "3"},
            {"reclaim_index": "f0", "reclaim_term" : "7"}
        ]  
    })";
    IndexReclaimerParam reclaimParam;
    autil::legacy::FromJsonString(reclaimParam, paramJson);

    std::map<segmentid_t, size_t> segmentId2DocCount = {{0, 123}};
    std::map<segmentid_t, docid_t> segment2BaseDocId = GenerateSegment2BaseDocId(segmentId2DocCount);
    AndIndexReclaimer reclaimer(nullptr, reclaimParam, segment2BaseDocId, segmentId2DocCount);
    std::vector<Doc> docs = MakeDocs(segmentId2DocCount, {123});

    auto readers = std::make_shared<indexlib::index::MultiFieldIndexReader>(nullptr);
    auto indexConfig = std::make_shared<indexlibv2::config::SingleFieldIndexConfig>("f0", it_string);
    indexConfig->SetIndexId(0);
    ASSERT_TRUE(readers->AddIndexReader(indexConfig.get(), std::make_shared<FakeIndexReader>(docs, 0)).IsOK());

    ASSERT_TRUE(reclaimer.Init(readers));
    index::DeletionMapPatchWriter docDeleter(nullptr, segmentId2DocCount);
    ASSERT_TRUE(reclaimer.Reclaim(&docDeleter).IsOK());

    auto bitmaps = docDeleter.GetSegmentId2Bitmaps();
    ASSERT_TRUE(bitmaps.empty());
}

TEST_F(IndexReclaimerTest, testAndIndexReclaimer_4)
{
    std::string paramJson = R"(
    {
        "reclaim_operator": "AND",
        "reclaim_index_info" : [
            {"reclaim_index": "f0", "reclaim_term" : "3"},
            {"reclaim_index": "f1", "reclaim_term" : "3"},
            {"reclaim_index": "f2", "reclaim_term" : "11"},
            {"reclaim_index": "f3", "reclaim_term" : "3"},
            {"reclaim_index": "f4", "reclaim_term" : "9"},
            {"reclaim_index": "f5", "reclaim_term" : "2"},
            {"reclaim_index": "f6", "reclaim_term" : "3"},
            {"reclaim_index": "f7", "reclaim_term" : "0"}
        ]  
    })";
    IndexReclaimerParam reclaimParam;
    autil::legacy::FromJsonString(reclaimParam, paramJson);

    std::map<segmentid_t, size_t> segmentId2DocCount = {{0, 212313}, {1, 1110}, {2, 0}, {3, 3012312}, {4, 1911231}};
    std::map<segmentid_t, docid_t> segment2BaseDocId = GenerateSegment2BaseDocId(segmentId2DocCount);
    AndIndexReclaimer reclaimer(nullptr, reclaimParam, segment2BaseDocId, segmentId2DocCount);
    std::vector<Doc> docs = MakeDocs(segmentId2DocCount, {5, 5, 13, 7, 11, 3, 5, 2});

    auto readers = std::make_shared<indexlib::index::MultiFieldIndexReader>(nullptr);

    indexid_t indexId = 0;
    for (const std::string& indexName : {"f0", "f1", "f2", "f3", "f4", "f5", "f6", "f7"}) {
        auto indexConfig = std::make_shared<indexlibv2::config::SingleFieldIndexConfig>(indexName, it_string);
        indexConfig->SetIndexId(indexId);
        ASSERT_TRUE(
            readers->AddIndexReader(indexConfig.get(), std::make_shared<FakeIndexReader>(docs, indexId++)).IsOK());
    }

    ASSERT_TRUE(reclaimer.Init(readers));
    index::DeletionMapPatchWriter docDeleter(nullptr, segmentId2DocCount);
    ASSERT_TRUE(reclaimer.Reclaim(&docDeleter).IsOK());
    CheckDeleter(&docDeleter, docs, reclaimParam, segment2BaseDocId);
}

TEST_F(IndexReclaimerTest, testIndexFieldReclaimer)
{
    std::string paramJson = R"(
    {
        "reclaim_index" : "f0",
        "reclaim_terms" : ["1", "2", "4"],
        "reclaim_operator" : "",
        "reclaim_index_info" : []
    })";
    IndexReclaimerParam reclaimParam;
    autil::legacy::FromJsonString(reclaimParam, paramJson);

    std::map<segmentid_t, size_t> segmentId2DocCount = {{0, 0}, {1, 10}, {2, 20}, {3, 30}};
    std::map<segmentid_t, docid_t> segment2BaseDocId = GenerateSegment2BaseDocId(segmentId2DocCount);
    IndexFieldReclaimer reclaimer(nullptr, reclaimParam, segment2BaseDocId, segmentId2DocCount);
    std::vector<Doc> docs = MakeDocs(segmentId2DocCount, {7});
    auto readers = std::make_shared<indexlib::index::MultiFieldIndexReader>(nullptr);
    auto indexConfig = std::make_shared<indexlibv2::config::SingleFieldIndexConfig>("f0", it_string);
    indexConfig->SetIndexId(0);
    ASSERT_TRUE(readers->AddIndexReader(indexConfig.get(), std::make_shared<FakeIndexReader>(docs, 0)).IsOK());
    ASSERT_TRUE(reclaimer.Init(readers));
    index::DeletionMapPatchWriter docDeleter(nullptr, segmentId2DocCount);
    ASSERT_TRUE(reclaimer.Reclaim(&docDeleter).IsOK());
    CheckDeleter(&docDeleter, docs, reclaimParam, segment2BaseDocId);
}

TEST_F(IndexReclaimerTest, testIndexFieldReclaimer_2)
{
    std::string paramJson = R"(
    {
        "reclaim_index" : "f0",
        "reclaim_terms" : ["2", "4"],
        "reclaim_operator" : "",
        "reclaim_index_info" : []
    })";
    IndexReclaimerParam reclaimParam;
    autil::legacy::FromJsonString(reclaimParam, paramJson);

    std::map<segmentid_t, size_t> segmentId2DocCount = {{0, 0}, {1, 12310}, {2, 20123}, {3, 31130}};
    std::map<segmentid_t, docid_t> segment2BaseDocId = GenerateSegment2BaseDocId(segmentId2DocCount);
    IndexFieldReclaimer reclaimer(nullptr, reclaimParam, segment2BaseDocId, segmentId2DocCount);
    std::vector<Doc> docs = MakeDocs(segmentId2DocCount, {723});
    auto readers = std::make_shared<indexlib::index::MultiFieldIndexReader>(nullptr);
    auto indexConfig = std::make_shared<indexlibv2::config::SingleFieldIndexConfig>("f0", it_string);
    indexConfig->SetIndexId(0);
    ASSERT_TRUE(readers->AddIndexReader(indexConfig.get(), std::make_shared<FakeIndexReader>(docs, 0)).IsOK());
    ASSERT_TRUE(reclaimer.Init(readers));
    index::DeletionMapPatchWriter docDeleter(nullptr, segmentId2DocCount);
    ASSERT_TRUE(reclaimer.Reclaim(&docDeleter).IsOK());
    CheckDeleter(&docDeleter, docs, reclaimParam, segment2BaseDocId);
}

TEST_F(IndexReclaimerTest, testIndexFieldReclaimer_3)
{
    std::string paramJson = R"(
    {
        "reclaim_index" : "f0",
        "reclaim_terms" : ["14", "18"],
        "reclaim_operator" : "",
        "reclaim_index_info" : []
    })";
    IndexReclaimerParam reclaimParam;
    autil::legacy::FromJsonString(reclaimParam, paramJson);

    std::map<segmentid_t, size_t> segmentId2DocCount = {{0, 3130}};
    std::map<segmentid_t, docid_t> segment2BaseDocId = GenerateSegment2BaseDocId(segmentId2DocCount);
    IndexFieldReclaimer reclaimer(nullptr, reclaimParam, segment2BaseDocId, segmentId2DocCount);
    std::vector<Doc> docs = MakeDocs(segmentId2DocCount, {13});
    auto readers = std::make_shared<indexlib::index::MultiFieldIndexReader>(nullptr);
    auto indexConfig = std::make_shared<indexlibv2::config::SingleFieldIndexConfig>("f0", it_string);
    indexConfig->SetIndexId(0);
    ASSERT_TRUE(readers->AddIndexReader(indexConfig.get(), std::make_shared<FakeIndexReader>(docs, 0)).IsOK());
    ASSERT_TRUE(reclaimer.Init(readers));
    index::DeletionMapPatchWriter docDeleter(nullptr, segmentId2DocCount);
    ASSERT_TRUE(reclaimer.Reclaim(&docDeleter).IsOK());
    auto bitmaps = docDeleter.GetSegmentId2Bitmaps();
    ASSERT_TRUE(bitmaps.empty());
}
} // namespace indexlibv2::table
