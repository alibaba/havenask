/*
 * Copyright 2014-present Alibaba Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
#include "indexlib/test/partition_state_machine.h"

#include "autil/DataBuffer.h"
#include "autil/StringUtil.h"
#include "indexlib/document/index_document/normal_document/source_document.h"
#include "indexlib/document/kv_document/kv_index_document.h"
#include "indexlib/document/raw_document/default_raw_document.h"
#include "indexlib/file_system/FileBlockCache.h"
#include "indexlib/file_system/FileBlockCacheContainer.h"
#include "indexlib/file_system/LogicalFileSystem.h"
#include "indexlib/file_system/fslib/FslibWrapper.h"
#include "indexlib/index_base/branch_fs.h"
#include "indexlib/partition/builder_branch_hinter.h"
#include "indexlib/partition/index_partition.h"
#include "indexlib/partition/index_partition_creator.h"
#include "indexlib/partition/index_partition_reader.h"
#include "indexlib/partition/partition_validater.h"
#include "indexlib/plugin/plugin_manager.h"
#include "indexlib/test/document_convertor.h"
#include "indexlib/test/document_creator.h"
#include "indexlib/test/document_parser.h"
#include "indexlib/test/kkv_searcher.h"
#include "indexlib/test/kv_searcher.h"
#include "indexlib/test/query.h"
#include "indexlib/test/query_parser.h"
#include "indexlib/test/result_checker.h"
#include "indexlib/test/searcher.h"
#include "indexlib/test/simple_table_reader.h"
#include "indexlib/test/sub_document_extractor.h"
#include "indexlib/util/TaskScheduler.h"
#include "indexlib/util/memory_control/MemoryQuotaControllerCreator.h"

using namespace std;
using namespace autil;
using namespace indexlib::config;
using namespace indexlib::plugin;
using namespace indexlib::common;
using namespace indexlib::index;
using namespace indexlib::table;
using namespace indexlib::document;
using namespace indexlib::partition;
using namespace indexlib::util;
using namespace indexlib::index_base;
using namespace indexlib::file_system;

namespace indexlib { namespace test {
IE_LOG_SETUP(test, PartitionStateMachine);

string PartitionStateMachine::BUILD_CONTROL_SEPARATOR = "##";
namespace {
string LAST_VALUE_PREFIX = "__last_value__";
}

PartitionStateMachine::PartitionStateMachine(bool buildSerializedDoc,
                                             const partition::IndexPartitionResource& partitionResource,
                                             const partition::DumpSegmentContainerPtr& dumpSegmentContainer)
    : mPartitionResource(partitionResource)
    , mBuildSerializedDoc(buildSerializedDoc)
    , mIsSuspendRtBuild(false)
    , mCurrentTimestamp(0)
    , mDumpSegmentContainer(dumpSegmentContainer)
{
}

PartitionStateMachine::~PartitionStateMachine() {}

string PartitionStateMachine::SerializeDocuments(const vector<NormalDocumentPtr>& inDocs)
{
    vector<DocumentPtr> docs;
    for (auto doc : inDocs) {
        docs.push_back(DYNAMIC_POINTER_CAST(Document, doc));
    }
    return SerializeDocuments(docs);
}

string PartitionStateMachine::SerializeDocuments(const vector<DocumentPtr>& inDocs)
{
    DataBuffer dataBuffer;
    int docCount = inDocs.size();
    dataBuffer.write(docCount);
    string result;
    for (const auto& doc : inDocs) {
        DataBuffer temp;
        temp.write(doc);
        string docStr = string(temp.getData(), temp.getDataLen());
        dataBuffer.write(docStr);
    }
    return string(dataBuffer.getData(), dataBuffer.getDataLen());
}

vector<DocumentPtr> PartitionStateMachine::DeserializeDocuments(const string& docBinaryStr)
{
    vector<DocumentPtr> docs;
    DataBuffer dataBuffer(const_cast<char*>(docBinaryStr.c_str()), docBinaryStr.length());
    int docCount;
    dataBuffer.read(docCount);
    for (int i = 0; i < docCount; ++i) {
        string docStr;
        dataBuffer.read(docStr);
        DocumentPtr doc;
        if (mPsmOptions["multiMessage"] == "true") {
            doc = mDocParser->TEST_ParseMultiMessage(StringView(docStr));
        } else {
            doc = mDocParser->Parse(StringView(docStr));
        }
        docs.push_back(doc);
    }
    return docs;
}

bool PartitionStateMachine::Init(const config::IndexPartitionSchemaPtr& schema, config::IndexPartitionOptions options,
                                 string root, string partitionName, string secondRoot, uint32_t branchId,
                                 std::string globalCacheStr)
{
    setenv("TEST_QUICK_EXIT", "true", 0);
    if (schema->GetRegionCount() > 1) {
        SetPsmOption("defaultRegion", schema->GetRegionSchema(DEFAULT_REGIONID)->GetRegionName());
    }

    if (!mPartitionResource.memoryQuotaController) {
        mPartitionResource.memoryQuotaController =
            std::make_shared<util::MemoryQuotaController>(/*name*/ "", std::numeric_limits<int64_t>::max());
    }
    if (!mPartitionResource.taskScheduler) {
        mPartitionResource.taskScheduler.reset(new TaskScheduler);
    }
    if (!mPartitionResource.metricProvider) {
        mMetricProvider.reset(new util::MetricProvider(nullptr));
        mPartitionResource.metricProvider = mMetricProvider;
    }
    if (!mPartitionResource.fileBlockCacheContainer) {
        mPartitionResource.fileBlockCacheContainer.reset(new file_system::FileBlockCacheContainer());
        mPartitionResource.fileBlockCacheContainer->Init(globalCacheStr, mPartitionResource.memoryQuotaController,
                                                         mPartitionResource.taskScheduler,
                                                         mPartitionResource.metricProvider);
    }
    mPartitionResource.partitionName = partitionName;

    mFsBranchId = branchId;
    mSchema = schema;
    mOptions = options;
    mOptions.GetOnlineConfig().SetPrimaryNode(true);
    mRoot = root;
    auto ec = FslibWrapper::MkDirIfNotExist(root).Code();
    THROW_IF_FS_ERROR(ec, "mkdir [%s] failed", root.c_str());

    mSecondaryRootPath = secondRoot;

    mDocFactory.reset(new DocumentFactoryWrapper(schema));

    // init hash id document rewriter
    mHashIdDocumentRewriter.reset(DocumentRewriterCreator::CreateHashIdDocumentRewriter(schema));

    // customized doc factory
    auto docParserConfig = CustomizedConfigHelper::FindCustomizedConfig(schema->GetCustomizedDocumentConfigs(),
                                                                        CUSTOMIZED_DOCUMENT_CONFIG_PARSER);
    if (!docParserConfig) {
        if (!mDocFactory->Init()) {
            return false;
        }
        mDocParser.reset(mDocFactory->CreateDocumentParser());
        return mDocParser.get() != nullptr;
    }

    // init plugin
    string pluginName = docParserConfig->GetPluginName();
    if (pluginName.empty()) {
        return false;
    }
    config::ModuleInfo moduleInfo;
    moduleInfo.moduleName = pluginName;
    moduleInfo.modulePath = PluginManager::GetPluginFileName(pluginName);
    if (!mDocFactory->Init(mPartitionResource.indexPluginPath, moduleInfo)) {
        return false;
    }
    DocumentInitParamPtr initParam(new DocumentInitParam(docParserConfig->GetParameters()));
    mDocParser.reset(mDocFactory->CreateDocumentParser(initParam));
    return mDocParser.get() != nullptr;
}

bool PartitionStateMachine::Transfer(PsmEvent event, const string& data, const string& queryString,
                                     const string& resultString)
{
    AUTIL_LOG(INFO, "psm begin event");
    if (!ExecuteEvent(event, data)) {
        return false;
    }
    AUTIL_LOG(INFO, "psm begin query");
    if (NeedCheckMetrics(event)) {
        PartitionMetricsPtr metrics = GetMetrics();
        return metrics->CheckMetrics(resultString);
    }

    if (queryString.empty()) {
        return true;
    }

    TableSearchCacheType cacheType = GetSearchCacheType(event);
    ResultPtr expectResult = DocumentParser::ParseResult(resultString);
    ResultPtr result = Search(queryString, cacheType);

    if (NeedUnorderCheck()) {
        return ResultChecker::UnorderCheck(result, expectResult);
    }
    return ResultChecker::Check(result, expectResult);
}

bool PartitionStateMachine::ExecuteEvent(PsmEvent event, const string& data)
{
    try {
        if (NeedBuild(event)) {
            if (!DoBuildEvent(event, data)) {
                return false;
            }
        }

        if (NeedReOpen(event)) {
            if (!RefreshOnlinePartition(event, data)) {
                return false;
            }
        }
    } catch (const ExceptionBase& e) {
        IE_LOG(ERROR, "ExecuteEvent [%d] FAILED, reason: [%s]", (int32_t)event, e.what());
        return false;
    }
    return true;
}

PartitionMetricsPtr PartitionStateMachine::GetMetrics()
{
    PartitionMetricsPtr partitionMetrics(new PartitionMetrics());
    if (!mOnlinePartition) {
        return PartitionMetricsPtr();
    }
    partitionMetrics->Init(mOnlinePartition);
    return partitionMetrics;
}

ResultPtr PartitionStateMachine::Search(const string& queryString, TableSearchCacheType cacheType)
{
    if (!mOnlinePartition) {
        CreateOnlinePartition(INVALID_VERSION);
    }

    if (!mOnlinePartition) {
        IE_LOG(ERROR, "failed to create index partition");
        return ResultPtr(new test::Result);
    }

    if (mSchema->GetTableType() == tt_kkv) {
        return SearchKKV(queryString, cacheType);
    }
    if (mSchema->GetTableType() == tt_kv) {
        return SearchKV(queryString, cacheType);
    }
    if (mSchema->GetTableType() == tt_customized) {
        return SearchCustomTable(queryString);
    }

    IndexPartitionReaderPtr reader = mOnlinePartition->GetReader();
    QueryPtr query = QueryParser::Parse(queryString, reader, &mPool);
    if (!query) {
        IE_LOG(ERROR, "failed to lookup term [%s]", queryString.c_str());
        return ResultPtr(new test::Result);
    }

    IndexPartitionSchema* schema = mSchema.get();
    if (query->IsSubQuery()) {
        reader = reader->GetSubPartitionReader();
        schema = mSchema->GetSubIndexPartitionSchema().get();
    }

    Searcher searcher;
    searcher.Init(reader, schema);
    ResultPtr result = searcher.Search(query, cacheType);
    result->SetIsSubResult(query->IsSubQuery());

    return result;
}

bool PartitionStateMachine::DoBuildEvent(PsmEvent event, const string& data)
{
    if ((event & PE_BUILD_RT) && !mOnlinePartition) {
        AUTIL_LOG(INFO, "refresh online part begin");
        RefreshOnlinePartition(BUILD_FULL_NO_MERGE, "");
        AUTIL_LOG(INFO, "refresh online part end");
    }
    if (event == BUILD_RT_MEM_LIMIT) {
        UpdateRtBuildStatus();
        if (mIsSuspendRtBuild) {
            return false;
        }
    }

    Build(event, data);
    return true;
}

void PartitionStateMachine::UpdateRtBuildStatus()
{
    if (mOnlinePartition->CheckMemoryStatus() == indexlib::partition::IndexPartition::MS_REACH_MAX_RT_INDEX_SIZE) {
        IE_LOG(ERROR, "failed to build rt doc, reach MS_REACH_MAX_RT_INDEX_SIZE");
        mIsSuspendRtBuild = true;
        return;
    }
    mIsSuspendRtBuild = false;
}

bool PartitionStateMachine::RefreshOnlinePartition(PsmEvent event, const string& data)
{
    versionid_t targetVersionId = INVALID_VERSION;
    if (!data.empty() && (event == PE_REOPEN || event == PE_REOPEN_FORCE)) {
        if (!StringUtil::fromString(data, targetVersionId)) {
            IE_LOG(ERROR, "failed to get target vesion[%s]", data.c_str());
            targetVersionId = INVALID_VERSION;
        }
    }

    if (!mOnlinePartition) {
        AUTIL_LOG(INFO, "create online partition begin");
        if (!CreateOnlinePartition(targetVersionId)) {
            return false;
        }
        AUTIL_LOG(INFO, "create online partition end");
    } else {
        bool force = event & PE_REOPEN_FORCE;
        IndexPartition::OpenStatus rs = mOnlinePartition->ReOpen(force, targetVersionId);
        if (rs != IndexPartition::OS_OK) {
            IE_LOG(ERROR, "failed to reopen");
            return false;
        }
    }
    return true;
}

void PartitionStateMachine::Build(PsmEvent event, const string& data)
{
    IndexBuilderPtr indexBuilder = CreateIndexBuilder(event);
    int64_t versionTs = INVALID_TIMESTAMP;
    if (event & PE_BUILD_MASK) {
        versionTs = BatchBuild(indexBuilder, data, true, event & PE_DUMP_SEGMENT);

        auto branchFs = indexBuilder->GetBranchFs();
        if (branchFs && versionTs == INVALID_TIMESTAMP) {
            BuilderBranchHinter hinter(BuilderBranchHinter::Option::Test());
            branchFs->CommitToDefaultBranch(&hinter);
        }
    }

    if (event & PE_MERGE_MASK) {
        indexBuilder->Merge(mOptions, event & PE_BUILD_FULL, mCurrentTimestamp);
        ValidateLatestIndexVersion();
    }
    if (!(event & PE_BUILD_RT)) {
        try {
            indexBuilder->TEST_GetRootDirectory()->GetFileSystem()->TEST_MountLastVersion();
        } catch (...) {
            // pass
        }
    }

    if (versionTs == INVALID_TIMESTAMP && mFsBranchId) {
        versionTs = 0;
    }

    indexBuilder->EndIndex(versionTs);
    if (mFsBranchId == 0) {
        indexBuilder->TEST_BranchFSMoveToMainRoad();
    }
}

IndexBuilderPtr PartitionStateMachine::CreateIndexBuilder(PsmEvent event)
{
    IndexBuilderPtr builder;
    if (event & PE_BUILD_RT) {
        assert(mOnlinePartition);
        uint64_t memUse = mOptions.GetOnlineConfig().buildConfig.buildTotalMemory;
        // std::cout << "mem use:" << memUse << std::endl;
        QuotaControlPtr quotaControl(new QuotaControl(memUse * 1024 * 1024));
        builder.reset(new IndexBuilder(mOnlinePartition, quotaControl, GetMetricsProvider()));
    } else {
        uint64_t memUse = mOptions.GetOfflineConfig().buildConfig.buildTotalMemory;
        // std::cout << "mem use:" << memUse << std::endl;
        QuotaControlPtr quotaControl(new QuotaControl(memUse * 1024 * 1024));
        string buildRoot = mSecondaryRootPath.empty() ? mRoot : mSecondaryRootPath;
        builder.reset(new IndexBuilder(buildRoot, mOptions, mSchema, quotaControl,
                                       BuilderBranchHinter::Option::Test(mFsBranchId), GetMetricsProvider(),
                                       mPartitionResource.indexPluginPath, mPartitionResource.range));
    }
    if (!builder->Init()) {
        INDEXLIB_FATAL_ERROR(Runtime, "builder init fail");
    }
    return builder;
}

int64_t PartitionStateMachine::BatchBuild(const IndexBuilderPtr& builder, const string& data, bool needDump,
                                          bool needDumpSegment)
{
    string defaultRegion = mPsmOptions["defaultRegion"];
    int64_t endTimestamp = INVALID_TIMESTAMP;
    int64_t docTimeStamp = INVALID_TIMESTAMP;
    vector<DocumentPtr> docs;
    if (mPsmOptions["documentFormat"] == "binary") {
        docs = DeserializeDocuments(data);
    } else {
        size_t ctrlPos = data.rfind(BUILD_CONTROL_SEPARATOR);
        string ctrlStr;
        if (ctrlPos != string::npos) {
            ctrlStr = string(data, ctrlPos + BUILD_CONTROL_SEPARATOR.size());
        }
        vector<vector<string>> ctrlCmds;
        StringUtil::fromString(ctrlStr, ctrlCmds, "=", ";");

        for (const auto& kv : ctrlCmds) {
            if (kv.size() == 2 && kv[0] == "stopTs") {
                int64_t stopTs;
                if (StringUtil::numberFromString(kv[1], stopTs)) {
                    endTimestamp = stopTs;
                }
            }
        }
        string docStr(data, 0, ctrlPos);
        TableType tableType = mSchema->GetTableType();
        if (tableType == tt_kv || tableType == tt_kkv) {
            vector<document::KVDocumentPtr> kvDocs = DocumentCreator::CreateKVDocuments(mSchema, docStr, defaultRegion);
            for (auto kvDoc : kvDocs) {
                docs.push_back(DYNAMIC_POINTER_CAST(Document, kvDoc));
            }
        } else {
            docs = CreateDocuments(docStr);
        }
    }

    for (size_t i = 0; i < docs.size(); ++i) {
        DocumentPtr doc = docs[i];
        assert(doc);
        Locator locator = doc->GetLocator();
        int64_t docTs = doc->GetTimestamp();
        if (!locator.IsValid()) {
            IndexLocator indexLocator(0, docTs + 1, /*userData=*/"");
            doc->SetLocator(indexLocator.toString());
        }
        if (docTimeStamp < docTs) {
            docTimeStamp = docTs;
        }

        NormalDocumentPtr normalDoc = DYNAMIC_POINTER_CAST(NormalDocument, docs[i]);
        if (normalDoc) {
            IndexDocumentPtr indexDoc = normalDoc->GetIndexDocument();
            if (indexDoc) {
                // set term payload
                for (auto it = mTermPayloadMap.begin(); it != mTermPayloadMap.end(); ++it) {
                    indexDoc->SetTermPayload(it->first, it->second);
                }
            }
        }

        if (mBuildSerializedDoc) {
            DataBuffer dataBuffer;
            dataBuffer.write(docs[i]);
            StringView serializedStr(dataBuffer.getData(), dataBuffer.getDataLen());
            document::DocumentPtr doc;
            if (mPsmOptions["multiMessage"] == "true") {
                doc = mDocParser->TEST_ParseMultiMessage(serializedStr);
            } else {
                doc = mDocParser->Parse(serializedStr);
            }
        }
        if (!builder->Build(doc)) {
            IE_LOG(ERROR, "failed to build the [%lu] doc", i);
        }
    }

    if (needDumpSegment) {
        builder->DumpSegment();
    }
    if (needDump) {
        builder->EndIndex();
    }
    return endTimestamp;
}

bool PartitionStateMachine::CreateOnlinePartition(versionid_t targetVersionId)
{
    IndexPartitionPtr onlinePartition;
    IndexPartitionOptions onlineOptions = mOptions;
    onlineOptions.SetIsOnline(true);
    if (mSchema->GetTableType() == tt_customized) {
        onlinePartition = IndexPartitionCreator(mPartitionResource).CreateCustom(onlineOptions);
    } else {
        partition::OnlinePartition* pOnlinePartition = new partition::OnlinePartition(mPartitionResource);
        pOnlinePartition->TEST_SetDumpSegmentContainer(mDumpSegmentContainer);
        onlinePartition.reset(pOnlinePartition);
    }

    IndexPartition::OpenStatus rs =
        onlinePartition->Open(mRoot, mSecondaryRootPath, mSchema, onlineOptions, targetVersionId);
    if (rs != IndexPartition::OS_OK) {
        IE_LOG(ERROR, "failed to open");
        return false;
    }
    mOnlinePartition = onlinePartition;
    mSchema = mOnlinePartition->GetSchema();
    return true;
}

ResultPtr PartitionStateMachine::SearchCustomTable(const string& queryString)
{
    ResultPtr result(new test::Result);
    IndexPartitionReaderPtr reader = mOnlinePartition->GetReader();
    TableReaderPtr tableReader = reader->GetTableReader();
    if (!tableReader) {
        IE_LOG(ERROR, "TableReader is null in partition[%s]", mPartitionResource.partitionName.c_str());
        return result;
    }
    SimpleTableReaderPtr simpleTableReader = DYNAMIC_POINTER_CAST(SimpleTableReader, tableReader);

    if (!simpleTableReader) {
        IE_LOG(ERROR, "PartitionStataMachine only supports TableReader"
                      " derived from SimpleTableReader");
        return result;
    }
    return simpleTableReader->Search(queryString);
}

ResultPtr PartitionStateMachine::SearchKV(const string& queryString, TableSearchCacheType cacheType)
{
    vector<string> regionInfos = StringUtil::split(queryString, "@");
    assert(regionInfos.size() <= 2);

    string defaultRegion = mPsmOptions["defaultRegion"];
    string regionName = defaultRegion.empty() ? DEFAULT_REGIONNAME : defaultRegion;
    string regionQuery = queryString;
    if (regionInfos.size() == 2) {
        regionName = regionInfos[0];
        regionQuery = regionInfos[1];
    }

    IndexPartitionReaderPtr reader = mOnlinePartition->GetReader();
    KVSearcher searcher;
    searcher.Init(reader, mSchema.get(), regionName);
    vector<string> queryInfos = StringUtil::split(regionQuery, "#");
    uint64_t timestamp = 0;
    if (queryInfos.size() > 2 || queryInfos.size() <= 0) {
        assert(false);
    } else if (queryInfos.size() == 2) {
        timestamp = SecondToMicrosecond(StringUtil::fromString<uint64_t>(queryInfos[1]));
    }
    return searcher.Search(queryInfos[0], timestamp, cacheType);
}

ResultPtr PartitionStateMachine::SearchKKV(const string& queryString, TableSearchCacheType cacheType)
{
    vector<string> regionInfos = StringUtil::split(queryString, "@");
    assert(regionInfos.size() <= 2);

    string defaultRegion = mPsmOptions["defaultRegion"];
    string regionName = defaultRegion.empty() ? DEFAULT_REGIONNAME : defaultRegion;
    string regionQuery = queryString;
    if (regionInfos.size() == 2) {
        regionName = regionInfos[0];
        regionQuery = regionInfos[1];
    }

    IndexPartitionReaderPtr reader = mOnlinePartition->GetReader();
    KKVSearcher searcher;
    searcher.Init(reader, mSchema.get(), regionName);
    vector<string> queryInfos = StringUtil::split(regionQuery, "#");
    uint64_t timestamp = 0;
    if (queryInfos.size() > 2) {
        assert(false);
    } else if (queryInfos.size() == 2) {
        timestamp = SecondToMicrosecond(StringUtil::fromString<uint64_t>(queryInfos[1]));
    }
    vector<string> keyInfos = StringUtil::split(queryInfos[0], ":");
    if (keyInfos.size() == 1) {
        return searcher.Search(keyInfos[0], timestamp, cacheType);
    }
    if (keyInfos.size() == 2) {
        vector<string> skeys = StringUtil::split(keyInfos[1], ",");
        return searcher.Search(keyInfos[0], skeys, timestamp, cacheType);
    }
    assert(false);
    return ResultPtr(new test::Result);
}

bool PartitionStateMachine::NeedUnorderCheck() const
{
    auto iter = mPsmOptions.find("resultCheckType");
    if (iter != mPsmOptions.end()) {
        if (iter->second == "ORDERED") {
            return false;
        }
        if (iter->second == "UNORDERED") {
            return true;
        }
        if (iter->second != "") {
            INDEXLIB_FATAL_ERROR(BadParameter, "unsupported resultCheckType [%s]", iter->second.c_str());
        }
    }
    assert(mSchema);
    return mSchema->GetTableType() == tt_kkv;
}

TableSearchCacheType PartitionStateMachine::GetSearchCacheType(PsmEvent event) const
{
    if (event == QUERY_NO_CACHE) {
        return tsc_no_cache;
    }
    if (event == QUERY_ONLY_CACHE) {
        return tsc_only_cache;
    }
    return tsc_default;
}

vector<DocumentPtr> PartitionStateMachine::CreateDocuments(const string& docString)
{
    vector<string> docStrings = StringUtil::split(docString, DocumentParser::DP_CMD_SEPARATOR);
    vector<DocumentPtr> docs;
    for (size_t i = 0; i < docStrings.size(); ++i) {
        DocumentPtr doc = CreateDocument(docStrings[i]);
        // assert(doc);
        if (!doc) {
            continue;
        }
        docs.push_back(doc);
    }
    return docs;
}

void PartitionStateMachine::CleanResources()
{
    while (((OnlinePartition*)(mOnlinePartition.get()))->ExecuteTask(OnlinePartitionTaskItem::TT_CLEAN_RESOURCE))
        ;
    mOnlinePartition->GetFileSystem()->Sync(true).GetOrThrow();
}

DocumentPtr PartitionStateMachine::CreateDocument(const string& docString)
{
    RawDocumentPtr rawDoc = DocumentParser::Parse(docString);
    string hashIdFieldValue = rawDoc->GetField(DocumentParser::DP_TAG_HASH_ID_FIELD);
    assert(rawDoc);

    IndexPartitionSchemaPtr subSchema = mSchema->GetSubIndexPartitionSchema();
    IndexlibExtendDocumentPtr extDoc = test::DocumentConvertor::CreateExtendDocFromRawDoc(mSchema, rawDoc);
    vector<RawDocumentPtr> subDocs;
    if (subSchema) {
        SubDocumentExtractor extractor(mSchema);
        extractor.extractSubDocuments(rawDoc, subDocs);
        for (size_t i = 0; i < subDocs.size(); ++i) {
            IndexlibExtendDocumentPtr subExtDoc =
                test::DocumentConvertor::CreateExtendDocFromRawDoc(subSchema, subDocs[i]);
            extDoc->addSubDocument(subExtDoc);
        }
    }
    // support source
    SourceSchemaPtr sourceSchema = mSchema->GetSourceSchema();
    if (sourceSchema) {
        const ClassifiedDocumentPtr& classifiedDoc = extDoc->getClassifiedDocument();
        SourceSchema::FieldGroups fieldGroups;
        for (auto iter = sourceSchema->Begin(); iter != sourceSchema->End(); iter++) {
            assert((*iter)->GetFieldMode() == SourceGroupConfig::SourceFieldMode::SPECIFIED_FIELD);
            fieldGroups.push_back((*iter)->GetSpecifiedFields());
        }
        SourceDocumentPtr srcDoc(new SourceDocument(&mPool));
        for (size_t i = 0; i < fieldGroups.size(); i++) {
            for (auto& field : fieldGroups[i]) {
                const string& value = rawDoc->GetField(field);
                srcDoc->Append(i, field, StringView(value), true);
            }
        }
        classifiedDoc->setSourceDocument(srcDoc);
    }

    document::DocumentPtr doc = mDocParser->Parse(extDoc);
    const document::RawDocumentPtr& extRawDoc = extDoc->getRawDocument();
    if (extRawDoc && extRawDoc->getLocator().IsValid()) {
        doc->SetLocator(extRawDoc->getLocator());
    }

    // support hashId if enable hash id
    if (mHashIdDocumentRewriter) {
        doc->AddTag(DOCUMENT_HASHID_TAG_KEY, hashIdFieldValue);
        mHashIdDocumentRewriter->Rewrite(doc);
    }
    return doc;
}

void PartitionStateMachine::ValidateLatestIndexVersion()
{
    PartitionValidater validater(mOptions);
    string buildRoot = mSecondaryRootPath.empty() ? mRoot : mSecondaryRootPath;
    if (!validater.Init(buildRoot)) {
        INDEXLIB_FATAL_ERROR(IndexCollapsed, "init partition validater in path [%s] fail.", buildRoot.c_str());
    }
    validater.Check();
}

}} // namespace indexlib::test
