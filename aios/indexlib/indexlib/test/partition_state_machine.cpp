#include <autil/DataBuffer.h>
#include <autil/StringUtil.h>
#include "indexlib/test/partition_state_machine.h"
#include "indexlib/test/document_creator.h"
#include "indexlib/test/document_parser.h"
#include "indexlib/test/result_checker.h"
#include "indexlib/test/searcher.h"
#include "indexlib/test/query.h"
#include "indexlib/test/query_parser.h"
#include "indexlib/partition/online_partition.h"
#include "indexlib/util/memory_control/memory_quota_controller_creator.h"
#include "indexlib/util/task_scheduler.h"
#include "indexlib/file_system/file_block_cache.h"
#include "indexlib/document/index_document/kv_document/kv_index_document.h"
#include "indexlib/document/raw_document/default_raw_document.h"
#include "indexlib/testlib/sub_document_extractor.h"
#include "indexlib/plugin/plugin_manager.h"
#include "indexlib/partition/index_partition_creator.h"
#include "indexlib/test/simple_table_reader.h"

using namespace std;
using namespace autil;
IE_NAMESPACE_USE(misc);
IE_NAMESPACE_USE(config);
IE_NAMESPACE_USE(plugin);
IE_NAMESPACE_USE(common);
IE_NAMESPACE_USE(misc);
IE_NAMESPACE_USE(index);
IE_NAMESPACE_USE(table);
IE_NAMESPACE_USE(document);
IE_NAMESPACE_USE(testlib);
IE_NAMESPACE_USE(partition);

IE_NAMESPACE_USE(util);
IE_NAMESPACE_BEGIN(test);
IE_LOG_SETUP(test, PartitionStateMachine);

string PartitionStateMachine::BUILD_CONTROL_SEPARATOR = "##";

PartitionStateMachine::PartitionStateMachine(
    int64_t memUseLimit, bool buildSerializedDoc,
    const partition::IndexPartitionResource& partitionResource,
    const partition::DumpSegmentContainerPtr& dumpSegmentContainer)
    : mPartitionResource(partitionResource)
    , mMemUseLimit(memUseLimit)
    , mBuildSerializedDoc(buildSerializedDoc)
    , mCurrentTimestamp(0)
    , mDumpSegmentContainer(dumpSegmentContainer)
{
}

PartitionStateMachine::~PartitionStateMachine()
{
}

string PartitionStateMachine::SerializeDocuments(
        const vector<NormalDocumentPtr>& inDocs)
{
    vector<DocumentPtr> docs;
    for (auto doc : inDocs)
    {
        docs.push_back(DYNAMIC_POINTER_CAST(Document, doc));
    }
    return SerializeDocuments(docs);
}

string PartitionStateMachine::SerializeDocuments(
        const vector<DocumentPtr>& inDocs)
{
    DataBuffer dataBuffer;
    int docCount = inDocs.size();
    dataBuffer.write(docCount);
    string result;
    for (const auto& doc : inDocs)
    {
        DataBuffer temp;
        temp.write(doc);
        string docStr = string(temp.getData(), temp.getDataLen());
        dataBuffer.write(docStr);
    }
    return string(dataBuffer.getData(), dataBuffer.getDataLen());
}

vector<DocumentPtr> PartitionStateMachine::DeserializeDocuments(
    const string& docBinaryStr)
{
    vector<DocumentPtr> docs;
    DataBuffer dataBuffer(const_cast<char*>(docBinaryStr.c_str()), docBinaryStr.length());
    int docCount;
    dataBuffer.read(docCount);
    for (int i = 0; i < docCount; ++i)
    {
        string docStr;
        dataBuffer.read(docStr);
        DocumentPtr doc;
        if (mPsmOptions["multiMessage"] == "true")
        {
            doc = mDocParser->TEST_ParseMultiMessage(ConstString(docStr));
        }
        else
        {
            doc = mDocParser->Parse(ConstString(docStr));
        }
        docs.push_back(doc);
    }
    return docs;
}

bool PartitionStateMachine::Init(const config::IndexPartitionSchemaPtr& schema,
                                 config::IndexPartitionOptions options,
                                 string root,
                                 string partitionName,
                                 string secondRoot)
{
    if (schema->GetRegionCount() > 1)
    {
        SetPsmOption("defaultRegion", schema->GetRegionSchema(DEFAULT_REGIONID)->GetRegionName());
    }
    mPartitionName = partitionName;
    if (!mPartitionResource.memoryQuotaController)
    {
        mPartitionResource.memoryQuotaController =
            MemoryQuotaControllerCreator::CreateMemoryQuotaController(mMemUseLimit * 1024 * 1024);
    }

    if (!mPartitionResource.taskScheduler)
    {
        mPartitionResource.taskScheduler.reset(new TaskScheduler);
    }

    if (!mPartitionResource.metricProvider)
    {
        mMetricProvider.reset(new misc::MetricProvider(nullptr));
        mPartitionResource.metricProvider = mMetricProvider;
    }
    if (!mPartitionResource.fileBlockCache)
    {
        mPartitionResource.fileBlockCache.reset(new file_system::FileBlockCache());
        mPartitionResource.fileBlockCache->TEST_mQuickExit = true;
        mPartitionResource.fileBlockCache->Init("", mPartitionResource.memoryQuotaController,
                mPartitionResource.taskScheduler, mPartitionResource.metricProvider);
    }

    mSchema = schema;
    mOptions = options;
    mOptions.TEST_mQuickExit = true;
    mRoot = root;
    mSecondaryRootPath = secondRoot;

    mDocFactory.reset(new DocumentFactoryWrapper(schema));
    // customized doc factory
    auto docParserConfig = CustomizedConfigHelper::FindCustomizedConfig(
        schema->GetCustomizedDocumentConfigs(),  CUSTOMIZED_DOCUMENT_CONFIG_PARSER);
    if (!docParserConfig)
    {
        if (!mDocFactory->Init())
        {
            return false;
        }
        mDocParser.reset(mDocFactory->CreateDocumentParser());
        return mDocParser.get() != nullptr;
    }

    string pluginName = docParserConfig->GetPluginName();
    if (pluginName.empty())
    {
        return false;
    }
    config::ModuleInfo moduleInfo;
    moduleInfo.moduleName = pluginName;
    moduleInfo.modulePath = PluginManager::GetPluginFileName(pluginName);
    if (!mDocFactory->Init(mPartitionResource.indexPluginPath, moduleInfo))
    {
        return false;
    }
    DocumentInitParamPtr initParam(
        new DocumentInitParam(docParserConfig->GetParameters()));
    mDocParser.reset(mDocFactory->CreateDocumentParser(initParam));
    return mDocParser.get() != nullptr;
}

bool PartitionStateMachine::Transfer(PsmEvent event, const string& data,
                                     const string& queryString,
                                     const string& resultString)
{
    if (!ExecuteEvent(event, data))
    {
        IE_LOG(WARN, "execute event failed.");
        return false;
    }

    if (NeedCheckMetrics(event))
    {
        PartitionMetricsPtr metrics = GetMetrics();
        return metrics->CheckMetrics(resultString);
    }

    if (queryString.empty())
    {
        return true;
    }

    TableSearchCacheType cacheType = GetSearchCacheType(event);
    ResultPtr expectResult = DocumentParser::ParseResult(resultString);
    ResultPtr result = Search(queryString, cacheType);

    if (NeedUnorderCheck())
    {
        IE_LOG(WARN, "result check unorder.");
        return ResultChecker::UnorderCheck(result, expectResult);
    }
    return ResultChecker::Check(result, expectResult);
}

bool PartitionStateMachine::ExecuteEvent(PsmEvent event, const string& data)
{
    try
    {
        if (NeedBuild(event))
        {
            DoBuildEvent(event, data);
        }

        if (NeedReOpen(event))
        {
            if (!RefreshIndexPartition(event, data))
            {
                return false;
            }
        }
    }
    catch (const ExceptionBase& e)
    {
        IE_LOG(ERROR, "ExecuteEvent [%d] FAILED, reason: [%s]",
               (int32_t)event, e.what());
        return false;
    }
    return true;
}

PartitionMetricsPtr PartitionStateMachine::GetMetrics()
{
    PartitionMetricsPtr partitionMetrics(new PartitionMetrics());
    if(!mIndexPartition)
    {
        return PartitionMetricsPtr();
    }
    partitionMetrics->Init(mIndexPartition);
    return partitionMetrics;
}

ResultPtr PartitionStateMachine::Search(
        const string& queryString, TableSearchCacheType cacheType)
{
    if (!mIndexPartition)
    {
        CreateIndexPartition(INVALID_VERSION);
    }

    if (!mIndexPartition)
    {
        IE_LOG(ERROR, "failed to create index partition");
        return ResultPtr(new Result);
    }

    if (mSchema->GetTableType() == tt_customized)
    {
        return SearchCustomTable(queryString);
    }

    IndexPartitionReaderPtr reader = mIndexPartition->GetReader();
    QueryPtr query = QueryParser::Parse(queryString, reader);
    if (!query)
    {
        IE_LOG(ERROR, "failed to lookup term [%s]", queryString.c_str());
        return ResultPtr(new Result);
    }

    IndexPartitionSchema* schema = mSchema.get();
    if (query->IsSubQuery())
    {
        reader = reader->GetSubPartitionReader();
        schema = mSchema->GetSubIndexPartitionSchema().get();
    }

    Searcher searcher;
    searcher.Init(reader, schema);
    ResultPtr result = searcher.Search(query, cacheType);
    result->SetIsSubResult(query->IsSubQuery());

    return result;
}

void PartitionStateMachine::DoBuildEvent(PsmEvent event, const string& data)
{
    if ((event & PE_BUILD_RT) && !mIndexPartition)
    {
        //first build rt, we'll build empty full index
        Build(BUILD_FULL_NO_MERGE, "");
        RefreshIndexPartition(BUILD_FULL_NO_MERGE, "");
    }

    Build(event, data);
}

bool PartitionStateMachine::RefreshIndexPartition(PsmEvent event, const string& data)
{
    versionid_t targetVersionId = INVALID_VERSION;
    if (!data.empty() && (event == PE_REOPEN || event == PE_REOPEN_FORCE))
    {
        if (!StringUtil::fromString(data, targetVersionId))
        {
            IE_LOG(ERROR, "failed to get target vesion[%s]", data.c_str());
            targetVersionId = INVALID_VERSION;
        }
    }

    if (!mIndexPartition)
    {
        if (!CreateIndexPartition(targetVersionId))
        {
            return false;
        }
    }
    else
    {
        bool force = event & PE_REOPEN_FORCE;
        IndexPartition::OpenStatus rs =
            mIndexPartition->ReOpen(force, targetVersionId);
        if (rs != IndexPartition::OS_OK)
        {
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
    if (event & PE_BUILD_MASK)
    {
        versionTs = BatchBuild(indexBuilder, data,
                               event & PE_BUILD_RT,
                               event & PE_DUMP_SEGMENT);
    }

    if (event & PE_MERGE_MASK)
    {
        indexBuilder->Merge(mOptions, event & PE_BUILD_FULL, mCurrentTimestamp);
    }
    indexBuilder->EndIndex(versionTs);
}

IndexBuilderPtr PartitionStateMachine::CreateIndexBuilder(PsmEvent event)
{
    IndexBuilderPtr builder;
    if (event & PE_BUILD_RT)
    {
        assert(mIndexPartition);
        uint64_t memUse = mOptions.GetOnlineConfig().buildConfig.buildTotalMemory;
        QuotaControlPtr quotaControl(new QuotaControl(memUse * 1024 * 1024));
        builder.reset(new IndexBuilder(mIndexPartition, quotaControl,
                        GetMetricsProvider()));
    }
    else
    {
        uint64_t memUse = mOptions.GetOfflineConfig().buildConfig.buildTotalMemory;
        QuotaControlPtr quotaControl(new QuotaControl(memUse * 1024 * 1024));
        string buildRoot = mSecondaryRootPath.empty() ? mRoot : mSecondaryRootPath;
        builder.reset(new IndexBuilder(buildRoot, mOptions, mSchema, quotaControl,
                                       GetMetricsProvider(),  mPartitionResource.indexPluginPath, mPartitionResource.range));

    }
    if (!builder->Init())
    {
        INDEXLIB_FATAL_ERROR(Runtime, "builder init fail");
    }
    return builder;
}

int64_t PartitionStateMachine::BatchBuild(
    const IndexBuilderPtr& builder, const string& data,
    bool needDump, bool needDumpSegment)
{

    string defaultRegion = mPsmOptions["defaultRegion"];

    int64_t endTimestamp = INVALID_TIMESTAMP;

    vector<DocumentPtr> docs;
    if (mPsmOptions["documentFormat"] == "binary")
    {
        docs = DeserializeDocuments(data);
    }
    else
    {
        size_t ctrlPos = data.rfind(BUILD_CONTROL_SEPARATOR);
        string ctrlStr;
        if (ctrlPos != string::npos)
        {
            ctrlStr = string(data, ctrlPos + BUILD_CONTROL_SEPARATOR.size());
        }
        vector<vector<string>> ctrlCmds;
        StringUtil::fromString(ctrlStr, ctrlCmds, "=", ";");

        for (const auto& kv : ctrlCmds)
        {
            if (kv.size() == 2 && kv[0] == "stopTs")
            {
                int64_t stopTs;
                if (StringUtil::numberFromString(kv[1], stopTs))
                {
                    endTimestamp = stopTs;
                }
            }
        }
        string docStr(data, 0, ctrlPos);
        docs = CreateDocuments(docStr);
    }

    for (size_t i = 0; i < docs.size(); ++i)
    {
        DocumentPtr doc = docs[i];
        assert(doc);
        Locator locator = doc->GetLocator();
        if (!locator.IsValid())
        {
            int64_t docTs = doc->GetTimestamp();
            IndexLocator indexLocator(0, docTs+1);
            doc->SetLocator(indexLocator.toString());
        }

        NormalDocumentPtr normalDoc = DYNAMIC_POINTER_CAST(NormalDocument, docs[i]);
        if (normalDoc)
        {
            IndexDocumentPtr indexDoc = normalDoc->GetIndexDocument();
            if (indexDoc)
            {
                // set term payload
                for (auto it = mTermPayloadMap.begin(); it != mTermPayloadMap.end(); ++it)
                {
                    indexDoc->SetTermPayload(it->first, it->second);
                }
            }
        }

        if (mBuildSerializedDoc)
        {
            DataBuffer dataBuffer;
            dataBuffer.write(docs[i]);
            ConstString serializedStr(dataBuffer.getData(), dataBuffer.getDataLen());
            document::DocumentPtr doc;
            if (mPsmOptions["multiMessage"] == "true")
            {
                doc = mDocParser->TEST_ParseMultiMessage(serializedStr);
            }
            else
            {
                doc = mDocParser->Parse(serializedStr);
            }
        }
        if (!builder->Build(doc))
        {
            IE_LOG(ERROR, "failed to build the [%lu] doc", i);
        }
    }

    if (needDumpSegment)
    {
        builder->DumpSegment();
    }
    if (needDump)
    {
        builder->EndIndex();
    }
    return endTimestamp;
}

bool PartitionStateMachine::CreateIndexPartition(versionid_t targetVersionId)
{
    IndexPartitionPtr indexPartition;
    IndexPartitionOptions onlineOptions = mOptions;
    onlineOptions.SetIsOnline(true);
    if (mSchema->GetTableType() == tt_customized)
    {
        indexPartition = IndexPartitionCreator::CreateCustomIndexPartition(
            mPartitionName, onlineOptions, mPartitionResource);
    }
    else
    {
        partition::OnlinePartition* onlinePartition =
            new partition::OnlinePartition(mPartitionName, mPartitionResource);
        onlinePartition->TEST_SetDumpSegmentContainer(mDumpSegmentContainer);
        indexPartition.reset(onlinePartition);
    }
    IndexPartition::OpenStatus rs =
        indexPartition->Open(mRoot, mSecondaryRootPath, mSchema, onlineOptions, targetVersionId);
    if (rs != IndexPartition::OS_OK)
    {
        IE_LOG(ERROR, "failed to open");
        return false;
    }
    mIndexPartition = indexPartition;
    mSchema = mIndexPartition->GetSchema();
    return true;
}

ResultPtr PartitionStateMachine::SearchCustomTable(const string& queryString)
{
    ResultPtr result(new Result);
    IndexPartitionReaderPtr reader = mIndexPartition->GetReader();
    TableReaderPtr tableReader = reader->GetTableReader();
    if (!tableReader)
    {
        IE_LOG(ERROR, "TableReader is null in partition[%s]", mPartitionName.c_str());
        return result;
    }
    SimpleTableReaderPtr simpleTableReader =
        DYNAMIC_POINTER_CAST(SimpleTableReader, tableReader);

    if (!simpleTableReader)
    {
        IE_LOG(ERROR, "PartitionStataMachine only supports TableReader"
               " derived from SimpleTableReader");
        return result;
    }
    return simpleTableReader->Search(queryString);
}

bool PartitionStateMachine::NeedUnorderCheck() const
{
    auto iter = mPsmOptions.find("resultCheckType");
    if (iter != mPsmOptions.end())
    {
        if (iter->second == "ORDERED")
        {
            return false;
        }
        if (iter->second == "UNORDERED")
        {
            return true;
        }
        if (iter->second != "")
        {
            INDEXLIB_FATAL_ERROR(BadParameter, "unsupported resultCheckType [%s]",
                    iter->second.c_str());
        }
    }
    assert(mSchema);
    return mSchema->GetTableType() == tt_kkv;
}

TableSearchCacheType PartitionStateMachine::GetSearchCacheType(PsmEvent event) const
{
    if (event == QUERY_NO_CACHE)
    {
        return tsc_no_cache;
    }
    if (event == QUERY_ONLY_CACHE)
    {
        return tsc_only_cache;
    }
    return tsc_default;
}


IndexlibExtendDocumentPtr PartitionStateMachine::CreateExtendDocFromRawDoc(
    const IndexPartitionSchemaPtr& schema, const RawDocumentPtr& rawDoc)
{
    document::IndexlibExtendDocumentPtr extDoc(new document::IndexlibExtendDocument);
    document::RawDocumentPtr newRawDoc(new document::DefaultRawDocument);
    newRawDoc->SetDocOperateType(rawDoc->GetDocOperateType());
    newRawDoc->SetTimestamp(rawDoc->GetTimestamp());
    newRawDoc->SetLocator(rawDoc->GetLocator());

    FieldSchemaPtr fieldSchema = schema->GetFieldSchema();
    RawDocument::Iterator iter = rawDoc->Begin();
    for (; iter != rawDoc->End(); iter++)
    {
        string fieldName = iter->first;
        string value = iter->second;
        if (value.empty())
        {
            continue;
        }

        FieldConfigPtr fieldConfig = fieldSchema->GetFieldConfig(fieldName);
        if (!fieldConfig)
        {
            newRawDoc->setField(fieldName, value);
            continue;
        }

        if (fieldConfig->IsMultiValue())
        {
            FieldType ft = fieldConfig->GetFieldType();
            bool replaceSep = (ft != ft_location &&
                               ft != ft_line &&
                               ft != ft_polygon);
            string parsedFieldValue =
                DocumentParser::ParseMultiValueField(value, replaceSep);
            newRawDoc->setField(fieldName, parsedFieldValue);
        }
        else
        {
            newRawDoc->setField(fieldName, value);
        }
    }

    ConvertModifyFields(schema, extDoc, rawDoc);
    extDoc->setRawDocument(newRawDoc);
    // prepare tokenized doc
    PrepareTokenizedDoc(schema, extDoc, rawDoc);
    return extDoc;
}

void PartitionStateMachine::PrepareTokenizedDoc(
    const IndexPartitionSchemaPtr& schema,
    const IndexlibExtendDocumentPtr &extDoc,
    const RawDocumentPtr& rawDoc)
{
    TokenizeDocumentPtr tokenDoc = extDoc->getTokenizeDocument();
    const FieldSchemaPtr& fieldSchema = schema->GetFieldSchema();
    const IndexSchemaPtr& indexSchema = schema->GetIndexSchema();
    for (FieldSchema::Iterator it = fieldSchema->Begin();
         it != fieldSchema->End(); ++it)
    {
        fieldid_t fieldId = (*it)->GetFieldId();
        FieldType fieldType = (*it)->GetFieldType();
        string fieldName = (*it)->GetFieldName();
        string fieldValue = rawDoc->GetField(fieldName);
        if (fieldValue.empty())
        {
            continue;
        }

        if (fieldType == ft_raw) {
            continue;
        }
        if (!indexSchema || !indexSchema->IsInIndex(fieldId))
        {
            continue;
        }


        const document::TokenizeFieldPtr &field = tokenDoc->createField(fieldId);
        bool ret = false;
        if (ft_text == fieldType) {
            ret = TokenizeValue(field, fieldValue);
        }
        else if (ft_location == fieldType ||
                 ft_line == fieldType ||
                 ft_polygon == fieldType)
        {
            ret = TokenizeSingleValueField(field, fieldValue);
        }
        else if ((*it)->IsMultiValue()
                 && indexSchema->IsInIndex(fieldId))
        {
            ret = TokenizeValue(field, fieldValue);
        }
        else
        {
            ret = TokenizeSingleValueField(field, fieldValue);
        }

        if (!ret)
        {
            string errorMsg = "Failed to Tokenize field:[" + fieldName + "]";
            IE_LOG(WARN, "%s", errorMsg.c_str());
        }
    }
}

bool PartitionStateMachine::TokenizeValue(
    const document::TokenizeFieldPtr &field,
    const string &fieldValue, const string& sep)
{
    vector<string> tokenVec = StringUtil::split(fieldValue, sep);
    document::TokenizeSection *section = field->getNewSection();
    if (NULL == section) {
        stringstream ss;
        ss << "Create section failed, fieldId = [" << field->getFieldId() << "]";
        string errorMsg = ss.str();
        IE_LOG(ERROR, "%s", errorMsg.c_str());
        return false;
    }
    document::TokenizeSection::Iterator iterator = section->createIterator();
    for (size_t i = 0; i < tokenVec.size(); i++)
    {
        document::AnalyzerToken token(tokenVec[i], i, tokenVec[i]);
        document::AnalyzerToken::TokenProperty property;
        property._isStopWord = 0;
        property._isSpace = 0;
        property._isBasicRetrieve = 1;
        property._isDelimiter = 0;
        property._isRetrieve = 1;
        property._needSetText = 1;
        property._unused = 0;

        token._tokenProperty = property;
        section->insertBasicToken(token, iterator);
    }
    return true;

}

bool PartitionStateMachine::TokenizeSingleValueField(
    const document::TokenizeFieldPtr &field,
    const string &fieldValue)
{
    document::TokenizeSection *section = field->getNewSection();
    if(NULL == section) {
        stringstream ss;
        ss << "Create section failed, fieldId = [" << field->getFieldId() << "]";
        string errorMsg = ss.str();
        IE_LOG(ERROR, "%s", errorMsg.c_str());
        return false;
    }
    document::TokenizeSection::Iterator iterator = section->createIterator();

    document::AnalyzerToken token;
    token.setText(fieldValue);
    token.setNormalizedText(fieldValue);
    section->insertBasicToken(token, iterator);
    return true;
}


void PartitionStateMachine::ConvertModifyFields(
    const IndexPartitionSchemaPtr& schema,
    const IndexlibExtendDocumentPtr &extDoc,
    const RawDocumentPtr& rawDoc)
{
    string modifyFields = rawDoc->GetField(RESERVED_MODIFY_FIELDS);
    if (modifyFields.empty())
    {
        return;
    }
    vector<string> fieldNames;
    StringUtil::fromString(modifyFields, fieldNames, MODIFY_FIELDS_SEP);
    const IndexPartitionSchemaPtr& subSchema =
        schema->GetSubIndexPartitionSchema();
    FieldSchemaPtr subFieldSchema;
    if (subSchema)
    {
        subFieldSchema = subSchema->GetFieldSchema();
    }
    FieldSchemaPtr fieldSchema = schema->GetFieldSchema();
    for (size_t i = 0; i < fieldNames.size(); i++)
    {
        const string& fieldName = fieldNames[i];
        if (fieldSchema->IsFieldNameInSchema(fieldName))
        {
            fieldid_t fid = fieldSchema->GetFieldId(fieldName);
            assert(fid != INVALID_FIELDID);
            extDoc->addModifiedField(fid);
        }
        else if (subFieldSchema)
        {
            fieldid_t fid = subFieldSchema->GetFieldId(fieldName);
            assert(fid != INVALID_FIELDID);
            extDoc->addSubModifiedField(fid);
        }
    }
}

vector<DocumentPtr> PartitionStateMachine::CreateDocuments(
        const string& docString)
{
    vector<string> docStrings = StringUtil::split(
        docString,  DocumentParser::DP_CMD_SEPARATOR);
    vector<DocumentPtr> docs;
    for (size_t i = 0; i < docStrings.size(); ++i)
    {
        DocumentPtr doc = CreateDocument(docStrings[i]);
        //assert(doc);
        if (!doc)
        {
            continue;
        }
        docs.push_back(doc);
    }
    return docs;
}

DocumentPtr PartitionStateMachine::CreateDocument(const string& docString)
{
    RawDocumentPtr rawDoc = DocumentParser::Parse(docString);
    assert(rawDoc);

    IndexPartitionSchemaPtr subSchema = mSchema->GetSubIndexPartitionSchema();
    vector<RawDocumentPtr> subDocs;
    if (subSchema)
    {
        SubDocumentExtractor extractor(mSchema);
        extractor.extractSubDocuments(rawDoc, subDocs);
    }

    IndexlibExtendDocumentPtr extDoc = CreateExtendDocFromRawDoc(mSchema, rawDoc);
    if (subSchema)
    {
        for (size_t i = 0; i < subDocs.size(); ++i)
        {
            IndexlibExtendDocumentPtr subExtDoc =
                CreateExtendDocFromRawDoc(subSchema, subDocs[i]);
            extDoc->addSubDocument(subExtDoc);
        }
    }
    document::DocumentPtr doc = mDocParser->Parse(extDoc);
    const document::RawDocumentPtr& extRawDoc = extDoc->getRawDocument();
    if (extRawDoc && extRawDoc->GetLocator() != NULL)
    {
        doc->SetLocator(extRawDoc->GetLocator());
    }
    return doc;
}


IE_NAMESPACE_END(test);
