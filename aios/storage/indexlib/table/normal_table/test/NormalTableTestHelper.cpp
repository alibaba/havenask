#include "indexlib/table/normal_table/test/NormalTableTestHelper.h"

#include "autil/StringUtil.h"
#include "autil/ThreadPool.h"
#include "indexlib/document/DocumentBatch.h"
#include "indexlib/document/normal/rewriter/AddToUpdateDocumentRewriter.h"
#include "indexlib/document/test/NormalDocumentMaker.h"
#include "indexlib/framework/ITabletReader.h"
#include "indexlib/framework/Tablet.h"
#include "indexlib/framework/Version.h"
#include "indexlib/framework/VersionCommitter.h"
#include "indexlib/framework/index_task/ExtendResource.h"
#include "indexlib/index/ann/aitheta2/AithetaTerm.h"
#include "indexlib/index/primary_key/Common.h"
#include "indexlib/index/primary_key/PrimaryKeyReader.h"
#include "indexlib/table/normal_table/NormalTabletSessionReader.h"
#include "indexlib/table/normal_table/index_task/test/FakeNormalTabletDocIterator.h"
#include "indexlib/table/normal_table/test/NormalTableTestSearcher.h"
#include "indexlib/table/normal_table/test/NormalTabletSchemaMaker.h"
#include "indexlib/table/test/DocumentParser.h"
#include "indexlib/table/test/ResultChecker.h"

namespace indexlibv2::table {
AUTIL_LOG_SETUP(indexlib.table, NormalTableTestHelper);

NormalTableTestHelper::NormalTableTestHelper(bool autoCleanIndex, bool needDeploy,
                                             framework::OpenOptions::BuildMode buildMode)
    : TableTestHelper(autoCleanIndex, needDeploy)
    , _nextDocTimestamp(0)
    , _buildMode(buildMode)
{
}

NormalTableTestHelper::~NormalTableTestHelper() {};

void NormalTableTestHelper::AfterOpen()
{
    if (GetCurrentVersion().GetVersionId() != INVALID_VERSIONID) {
        _nextDocTimestamp = GetCurrentVersion().GetTimestamp() + 1;
    }
}

std::shared_ptr<TableTestHelper> NormalTableTestHelper::CreateMergeHelper()
{
    std::shared_ptr<TableTestHelper> ptr(new NormalTableTestHelper);
    return ptr;
}

Status NormalTableTestHelper::SpecifySegmentsMerge(const std::vector<segmentid_t>& srcSegIds)
{
    MergeOption option;
    option.mergeStrategy = "specific_segments";
    option.mergeStrategyParam = "merge_segments=" + autil::StringUtil::toString(srcSegIds, ",");
    return Merge(option);
}

std::pair<Status, framework::VersionCoord>
NormalTableTestHelper::OfflineSpecifySegmentsMerge(const framework::VersionCoord& versionCoord,
                                                   const std::vector<segmentid_t>& srcSegIds)
{
    MergeOption option;
    option.mergeStrategy = "specific_segments";
    option.mergeStrategyParam = "merge_segments=" + autil::StringUtil::toString(srcSegIds, ",");
    return OfflineMerge(versionCoord, option);
}

bool NormalTableTestHelper::Query(const std::string& queryStr, const std::string& expectResultStr,
                                  bool ignoreDeletionMap)
{
    return Query(GetTabletReader(), queryStr, expectResultStr, ignoreDeletionMap);
}

bool NormalTableTestHelper::Query(const std::shared_ptr<framework::ITabletReader>& tabletReader,
                                  const std::string& queryStr, const std::string& expectResultStr,
                                  bool ignoreDeletionMap)
{
    NormalTableTestSearcher searcher(tabletReader, tabletReader->GetSchema());
    auto result = searcher.Search(queryStr, ignoreDeletionMap);
    // 参照 partition_state_machine.cpp:L263 的行为，
    // 对于不存在的 term 返回 null postingIterator 时，返回空 Result 而非空指针，以兼容 ResultChecker 的逻辑
    if (not result) {
        result = std::make_shared<Result>();
    }
    auto expectResult = DocumentParser::ParseResult(expectResultStr);
    return ResultChecker::Check(result, /*expectedError*/ false, expectResult);
}

bool NormalTableTestHelper::QueryANN(const std::string& queryStr, const std::string& expectResultStr,
                                     bool ignoreDeletionMap,
                                     std::shared_ptr<index::ann::AithetaAuxSearchInfoBase> searchInfo)
{
    const std::shared_ptr<framework::ITabletReader>& tabletReader = GetTabletReader();
    NormalTableTestSearcher searcher(tabletReader, tabletReader->GetSchema());
    auto result = searcher.SearchANN(queryStr, ignoreDeletionMap, searchInfo);
    if (not result) {
        result = std::make_shared<Result>();
    }
    auto expectResult = DocumentParser::ParseResult(expectResultStr);
    return ResultChecker::Check(result, /*expectedError*/ false, expectResult);
}

bool NormalTableTestHelper::DoQuery(std::string indexType, std::string indexName, std::string queryStr,
                                    std::string expectValue)
{
    // docid_t docId = INVALID_DOCID;
    // if (indexType == INVERTED_INDEX_TYPE_STR) {
    //     docId = QueryInverted(indexName, queryStr);
    // }
    assert(false);
    return false;
}

Status NormalTableTestHelper::DoBuild(std::string docs, bool oneBatch)
{
    if (_buildMode != indexlibv2::framework::OpenOptions::STREAM) {
        indexlibv2::framework::OpenOptions openOptions(_buildMode, /*consistentModeBuildThreadCount=*/4,
                                                       /*consistentModeBuildThreadCount=*/4);
        openOptions.SetUpdateControlFlowOnly(true);
        indexlibv2::framework::ReopenOptions reopenOptions(openOptions);
        auto st = GetTablet()->Reopen(reopenOptions, CONTROL_FLOW_VERSIONID);
        RETURN_IF_STATUS_ERROR(st, "reopen failed: %s", st.ToString().c_str());
    }
    // just support oneBatch = true
    auto docBatch = CreateNormalDocumentBatch(docs);
    assert(docBatch);
    auto status = GetITablet()->Build(docBatch);
    if (status.IsUninitialize()) {
        status = GetITablet()->Build(docBatch);
    }
    return status;
}

Status NormalTableTestHelper::DoBuild(const std::string& docStr, const framework::Locator& locator)
{
    if (_buildMode != indexlibv2::framework::OpenOptions::STREAM) {
        indexlibv2::framework::OpenOptions openOptions(_buildMode, /*consistentModeBuildThreadCount=*/4,
                                                       /*consistentModeBuildThreadCount=*/4);
        openOptions.SetUpdateControlFlowOnly(true);
        indexlibv2::framework::ReopenOptions reopenOptions(openOptions);
        auto st = GetTablet()->Reopen(reopenOptions, CONTROL_FLOW_VERSIONID);
        RETURN_IF_STATUS_ERROR(st, "reopen failed: %s", st.ToString().c_str());
    }
    auto docBatch = CreateNormalDocumentBatchWithLocator(docStr, locator);
    auto status = GetITablet()->Build(docBatch);
    if (status.IsUninitialize()) {
        status = GetITablet()->Build(docBatch);
    }
    return status;
}

std::shared_ptr<document::IDocumentBatch> NormalTableTestHelper::CreateNormalDocumentBatch(const std::string& docs)
{
    if (docs.empty()) {
        return std::make_shared<document::DocumentBatch>();
    }
    auto docBatch = document::NormalDocumentMaker::MakeBatch(GetITablet()->GetTabletSchema(), docs);
    if (!docBatch) {
        return nullptr;
    }
    auto iter = indexlibv2::document::DocumentIterator<indexlibv2::document::IDocument>::Create(docBatch.get());
    while (iter->HasNext()) {
        auto doc = std::dynamic_pointer_cast<indexlibv2::document::NormalDocument>(iter->Next());
        assert(doc);
        framework::Locator::DocInfo docInfo = doc->GetDocInfo();
        if (docInfo.timestamp == -1) {
            docInfo.timestamp = _nextDocTimestamp;
            doc->SetDocInfo(docInfo);
        }
        if (!doc->GetLocatorV2().IsValid()) {
            indexlibv2::framework::Locator locator;
            locator.SetOffset({_nextDocTimestamp + 1, 0});
            doc->SetLocator(locator);
        }
        ++_nextDocTimestamp;
    }
    if (not RewriteDocumentBatch(GetITablet()->GetTabletSchema(), docBatch)) {
        return nullptr;
    }
    return docBatch;
}

std::shared_ptr<document::IDocumentBatch>
NormalTableTestHelper::CreateNormalDocumentBatchWithLocator(const std::string& docStr,
                                                            const framework::Locator& locator)
{
    // only support docs.size() == 1
    auto docBatch = CreateNormalDocumentBatch(docStr);
    if (!docBatch) {
        return nullptr;
    }
    assert(docBatch->GetBatchSize() == 1);
    auto doc = std::dynamic_pointer_cast<indexlibv2::document::NormalDocument>((*docBatch)[0]);
    assert(doc);
    doc->SetLocator(locator);
    return docBatch;
}

bool NormalTableTestHelper::RewriteDocumentBatch(const std::shared_ptr<config::ITabletSchema>& schema,
                                                 const std::shared_ptr<document::IDocumentBatch>& docBatch)
{
    // 之所以要用新Rewriter，是 NormalTabletSchemaMaker.cpp
    // 中，直接进行schema->Deserialize(jsonStr)，把legacySchema给生成好，
    // 外部ut在构造Schema之后，Build()之前，对TabletSchema的修改，无法对legacySchema生效
    document::AddToUpdateDocumentRewriter rewriter;
    auto status = rewriter.Init(schema, {}, {});
    if (not status.IsOK()) {
        AUTIL_LOG(ERROR, "init AddToUpdateDocumentRewriter failed")
        return false;
    }
    status = rewriter.Rewrite(docBatch.get());
    if (not status.IsOK()) {
        AUTIL_LOG(ERROR, "AddToUpdateDocumentRewriter rewrite failed")
        return false;
    }
    return true;
}

docid_t NormalTableTestHelper::QueryPK(const std::string& pk)
{
    auto reader = std::dynamic_pointer_cast<NormalTabletSessionReader>(GetITablet()->GetTabletReader());
    assert(reader);
    auto pkReader = reader->GetIndexReader<index::PrimaryKeyReader<uint64_t>>(index::PRIMARY_KEY_INDEX_TYPE_STR, "pk");
    return pkReader->Lookup(pk);
}

std::shared_ptr<config::TabletSchema> NormalTableTestHelper::MakeSchema(const std::string& fieldNames,
                                                                        const std::string& indexNames,
                                                                        const std::string& attributeNames,
                                                                        const std::string& summaryNames)
{
    return NormalTabletSchemaMaker::Make(fieldNames, indexNames, attributeNames, summaryNames);
}

void NormalTableTestHelper::AddAlterTabletResourceIfNeed(
    std::vector<std::shared_ptr<framework::IndexTaskResource>>& extendResources,
    const std::shared_ptr<config::ITabletSchema>& schema)
{
    for (auto resource : extendResources) {
        if (resource->GetName() == "DOC_READER_ITERATOR_CREATOR") {
            return;
        }
    }
    auto createIterFunc = [](const std::shared_ptr<indexlibv2::config::ITabletSchema>& schema)
        -> std::shared_ptr<indexlibv2::framework::ITabletDocIterator> {
        return std::make_shared<FakeNormalTabletDocIterator>(schema);
    };
    auto resource =
        std::make_shared<framework::ExtendResource<indexlibv2::framework::ITabletDocIterator::CreateDocIteratorFunc>>(
            "DOC_READER_ITERATOR_CREATOR",
            std::make_shared<indexlibv2::framework::ITabletDocIterator::CreateDocIteratorFunc>(createIterFunc));
    extendResources.push_back(resource);
}

Status NormalTableTestHelper::InitTypedTabletResource()
{
    if (_buildMode != framework::OpenOptions::STREAM) {
        size_t threadCount = 4;
        size_t queueSize = size_t(-1);
        auto threadPoolQueueFactory = std::make_shared<autil::ThreadPoolQueueFactory>();
        _buildThreadPool =
            std::make_shared<autil::ThreadPool>(threadCount, queueSize, threadPoolQueueFactory, "parallel-build");
        _buildThreadPool->start();
        // In unit test, we use the same thread pool for both consistent and inconsistent mode. In production, we can
        // use separate thread pools.
        GetTabletResource()->consistentModeBuildThreadPool = _buildThreadPool;
        GetTabletResource()->inconsistentModeBuildThreadPool = _buildThreadPool;
    }
    return Status::OK();
}

} // namespace indexlibv2::table
