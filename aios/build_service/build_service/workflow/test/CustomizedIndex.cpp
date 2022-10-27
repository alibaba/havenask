#include "CustomizedIndex.h"
#include <sstream>
#include <autil/StringUtil.h>
#include <autil/StringTokenizer.h>
#include <indexlib/index_base/segment/segment_data_base.h>
#include <indexlib/file_system/directory.h>
#include <indexlib/plugin/plugin_manager.h>
#include <indexlib/table/table_resource.h>
#include <indexlib/storage/file_system_wrapper.h>
#include <indexlib/file_system/file_reader.h>
#include "build_service/document/DocumentDefine.h"

using namespace std;
using namespace autil;
using namespace fslib;
using namespace fslib::fs;

IE_NAMESPACE_USE(document);
IE_NAMESPACE_USE(plugin);
IE_NAMESPACE_USE(util);
IE_NAMESPACE_USE(common);
IE_NAMESPACE_USE(config);
IE_NAMESPACE_USE(table);
IE_NAMESPACE_USE(file_system);
IE_NAMESPACE_USE(storage);

namespace build_service {
namespace workflow {

/**
 * build process:
 * 1. [Reader] generate RawDocument from source file
 *    document reader read 20 bytes each time from source file, and transfer it to MyRawDocument
 * 2. [Processor] transfer RawDocument to Document
 *    since we defined use_raw_doc_as_document=true, so BS framework will use MyRawDocument as Document and write it to swift.
 * 3. [Builder] read Document from swift topic
 *    [Builder] will read swift msg and tranfser it to MyRawDocument, then MyTableWriter will build index from these document.
 **/


/**
 * Customized Document and RawDocument
 **/
void MyRawDocument::DoSerialize(autil::DataBuffer &dataBuffer,
                             uint32_t serializedVersion) const
{
    dataBuffer.write(mDocId);
    for (size_t i = 0; i < MyTableWriter::DEFAULT_VALUE_COUNT; i++)
    {
        dataBuffer.write(mData[i]);
    }
}

void MyRawDocument::DoDeserialize(autil::DataBuffer &dataBuffer,
                               uint32_t serializedVersion)
{
    dataBuffer.read(mDocId);
    mDocIdStr.reset((char*)&mDocId, sizeof(docid_t));
    for (size_t i = 0; i < MyTableWriter::DEFAULT_VALUE_COUNT; i++)
    {
        dataBuffer.read(mData[i]);
    }
}

void MyRawDocument::SetDocId(docid_t docId)
{
    mDocId = docId;
    mDocIdStr.reset((char*)&mDocId, sizeof(docid_t));
}

MyRawDocument::MyRawDocument()
{
    for (size_t i = 0; i < MyTableWriter::DEFAULT_VALUE_COUNT; i++)
    {
        mData[i] = 0.0;
    }
    mDocId = 0;
}

MyRawDocument::~MyRawDocument()
{}

void MyRawDocument::setField(const ConstString &fieldName,
                             const ConstString &fieldValue)
{
    // this method is useless since we have defined RawDocumentPaser
    assert(false);
}

ConstString MyRawDocument::SOURCE = ConstString("default_source");

const ConstString &MyRawDocument::getField(
        const ConstString &fieldName) const
{
    if (fieldName == "id") {
        return mDocIdStr;
    } else if (fieldName == "ha_reserved_source") {
        return MyRawDocument::SOURCE;
    }
    return ConstString::EMPTY_STRING;
}

RawDocument *MyRawDocument::clone() const
{
    MyRawDocument* doc = new MyRawDocument();
    for (size_t i = 0; i < MyTableWriter::DEFAULT_VALUE_COUNT; i++)
    {
        doc->mData[i] = mData[i];
    }
    return doc;
}

void MyRawDocument::setData(const float* data)
{
    for (size_t i = 0; i < MyTableWriter::DEFAULT_VALUE_COUNT; i++)
    {
        mData[i] = data[i];
    }
}

string MyRawDocument::toString() const
{
    stringstream ss;
    for (size_t i = 0; i < MyTableWriter::DEFAULT_VALUE_COUNT; i++)
    {
        ss << mData[i] << " ";
    }
    return ss.str();
}

bool MyRawDocumentParser::parse(const string &docString,
                                RawDocument &rawDoc)
{
    assert(docString.size() == 20);
    docid_t* docId = (docid_t*)docString.c_str();
    float* data = (float*)(docString.c_str() + sizeof(docid_t));
    MyRawDocument* doc = (MyRawDocument*)&rawDoc;
    doc->setData(data);
    doc->SetDocId(*docId);
    doc->SetDocOperateType(DocOperateType::ADD_DOC);
    return true;
}


MyDocumentParserForDemo::MyDocumentParserForDemo(const IndexPartitionSchemaPtr& schema)
    : DocumentParser(schema)
{
    cout << "construct MyDocumentParserForDemo" << endl;
}

MyDocumentParserForDemo::~MyDocumentParserForDemo()
{
    cout << "deconstruct MyDocumentParserForDemo" << endl;
}

bool MyDocumentParserForDemo::DoInit()
{
    return true;
}

DocumentPtr MyDocumentParserForDemo::Parse(const IndexlibExtendDocumentPtr& extendDoc)
{
    // since we defined "use_raw_doc_as_doc=true" in ProcessorChainConfig,
    // processor will not invoke this method anymore.
    assert(false);
    return DocumentPtr();
}

DocumentPtr MyDocumentParserForDemo::Parse(const ConstString& serializedData)
{
    // builder will use this method to generate Document from swift
    DataBuffer dataBuffer(const_cast<char*>(serializedData.data()),
                          serializedData.length());
    MyRawDocumentPtr document;
    dataBuffer.read(document);
    return document;
}


/**
 * Customized TableWriter
 **/

IE_LOG_SETUP(table, MyTableWriter);
IE_LOG_SETUP(table, MyTableFactory);
IE_LOG_SETUP(table, MyTableResource);
IE_LOG_SETUP(table, MyTableMerger);
IE_LOG_SETUP(table, MyMergePolicy);

// static string member in class will cause double free .....
// global static string member will not, #?!*
/*
std::string MyTableWriter::DATA_FILE_NAME = "demo_data_file";
std::string MyTableWriter::META_FILE_NAME = "demo_meta_file";
std::string MyTableWriter::MILESTONE_FILE_NAME = "demo_milestone_file";
*/
static const std::string DATA_FILE_NAME = "demo_data_file";
static const std::string META_FILE_NAME = "demo_meta_file";
static const std::string MILESTONE_FILE_NAME = "demo_milestone_file";

MyTableWriter::MyTableWriter(
    const KeyValueMap& parameters)
    : TableWriter()
    , mData(NULL)
    , mIsDirty(false)
    , mMinDocId(std::numeric_limits<docid_t>::max())
    , mMaxDocId(std::numeric_limits<docid_t>::min())
    , mLastConsumedMessageCount(0)
{
    // this is defined in schema.json
    auto iter = parameters.find("max_doc_count");
    if (iter == parameters.end()) {
        mMaxDocCount = DEFAULT_MAX_DOC_COUNT;
    } else if (!StringUtil::fromString(iter->second, mMaxDocCount)) {
        mMaxDocCount = DEFAULT_MAX_DOC_COUNT;
    }
    IE_LOG(INFO, "set max_doc_count = %zu", mMaxDocCount);
}

MyTableWriter::~MyTableWriter()
{
    if (mData)
    {
        // release memory
        for (size_t i = 0; i < mMaxDocCount; i++) {
            delete[] mData[i];
        }
        delete[] mData;
    }
}

bool MyTableWriter::DoInit()
{
    mIsDirty = false;
    // make sure build total memory > init mem * 2
    // const OfflineConfig offlineConfig = mOptions->GetOfflineConfig();
    // if (offlineConfig.buildConfig.buildTotalMemory <
    //     mMaxDocCount * sizeof(float) * DEFAULT_VALUE_COUNT * 2 / 1024) {
    //     return false;
    // }
    // init in memory segment
    if (mData != NULL)
    {
        IE_LOG(ERROR, "MyTableWriter has already been initailized");
        assert(false);
        return false;
    }

    mData = new float*[mMaxDocCount];
    for (size_t i = 0; i < mMaxDocCount; i++) {
        mData[i] = new float[DEFAULT_VALUE_COUNT];
        for (size_t j = 0; j < DEFAULT_VALUE_COUNT; j++) {
            mData[i][j] = 0.0;
        }
    }
    // read ondisk segment
    if (!RecoverFromSegment()) {
        return false;
    }
    return true;
}

bool MyTableWriter::RecoverFromOneSegment(const SegmentMetaPtr& segMeta,
                                          float** data, size_t maxDocCount)
{
    // read data
    DirectoryPtr segDir = segMeta->segmentDataDirectory;
    size_t fileLen = segDir->GetFileLength(DATA_FILE_NAME);
    FileReaderPtr fileReader = segDir->CreateFileReader(
        DATA_FILE_NAME, IE_NAMESPACE(file_system)::FSOT_IN_MEM);
    char buf[fileLen];
    size_t readLen = fileReader->Read(buf, fileLen);
    if (fileLen != readLen) {
        IE_LOG(ERROR, "read DATA_FILE[%s/%s] failed, readLen[%zu], fileLen[%zu]",
               segDir->GetPath().c_str(), DATA_FILE_NAME.c_str(),
               readLen, fileLen);
        return false;
    }
    // parse data
    string dataStr(buf, fileLen);
    string delimiter = "\n";
    size_t pos = 0;
    std::string line;
    while ((pos = dataStr.find(delimiter)) != std::string::npos) {
        line = dataStr.substr(0, pos);
        // parse each record
        vector<string> tokens = StringTokenizer::tokenize(ConstString(line), ",");
        if (tokens.size() != 5) {
            IE_LOG(WARN, "fail to parse segment data[%s]", line.c_str());
            continue;
        }
        docid_t docId;
        if (!StringUtil::fromString(tokens[0], docId)) {
            IE_LOG(WARN, "fail to parse segment data[%s], docId is invalid",
                   line.c_str());
            continue;
        }
        if (docId >= (docid_t)maxDocCount) {
            IE_LOG(WARN, "docId[%d] is greater then MaxDocCount[%zu]",
                   docId, maxDocCount);
            continue;
        }
        float tmp[DEFAULT_VALUE_COUNT];
        bool isDefaultValue = true;
        for (size_t i = 0; i < DEFAULT_VALUE_COUNT; i++) {
            float value = 0.0;
            StringUtil::fromString(tokens[i + 1], value);
            tmp[i] = value;
            if (value != 0.0) {
                isDefaultValue = false;
            }
        }
        if (!isDefaultValue) {
            std::memcpy(data[docId], &tmp, DEFAULT_VALUE_COUNT * sizeof(float));
        }
        dataStr.erase(0, pos + delimiter.length());
    }
    return true;
}

bool MyTableWriter::RecoverFromSegment()
{
    MyTableResourcePtr tbResource =
        DYNAMIC_POINTER_CAST(MyTableResource, mTableResource);
    vector<SegmentMetaPtr> segMetas = tbResource->GetSegmentMetas();
    for (auto segMeta : segMetas) {
        if (!RecoverFromOneSegment(segMeta, mData, mMaxDocCount)) {
            return false;
        }
    }
    return true;
}

size_t MyTableWriter::GetLastConsumedMessageCount() const { return mLastConsumedMessageCount; }

TableWriter::BuildResult MyTableWriter::Build(docid_t docId, const DocumentPtr& doc)
{
    mLastConsumedMessageCount = 0;
    MyRawDocumentPtr normDoc = DYNAMIC_POINTER_CAST(MyRawDocument, doc);
    // get real doc id from document
    docId = normDoc->GetDocId();
    if (docId >= (docid_t)mMaxDocCount) {
        // IE_LOG(ERROR, "docId[%d] is invalid", docId);
        return TableWriter::BuildResult::BR_SKIP;
    }
    if (!normDoc) {
        IE_LOG(ERROR, "cast to Document failed for docId[%d]", docId);
        return TableWriter::BuildResult::BR_FAIL;
    }
    // update meta
    if (docId > mMaxDocId) {
        mMaxDocId = docId;
    }
    if (docId < mMinDocId) {
        mMinDocId = docId;
    }
    // update in inmemory segment
    std::memcpy(mData[docId], normDoc->getData(), DEFAULT_VALUE_COUNT * sizeof(float));
    mIsDirty = true;
    mLastConsumedMessageCount = 1;    
    return TableWriter::BuildResult::BR_OK;
}

bool MyTableWriter::IsDirty() const
{
    return mIsDirty;
}

bool MyTableWriter::DumpSegment(BuildSegmentDescription& segmentDescription)
{
    try
    {
        // dump data
        stringstream dataStr;
        assert(mData);
        for (size_t i = 0; i < mMaxDocCount; i++) {
            // doc id
            dataStr << i << ",";
            // value
            for (size_t j = 0; j < DEFAULT_VALUE_COUNT; j++) {
                dataStr << mData[i][j];
                if (j == DEFAULT_VALUE_COUNT - 1) {
                    dataStr << "\n";
                } else {
                    dataStr << ",";
                }
            }
        }
        mSegmentDataDirectory->Store(DATA_FILE_NAME, dataStr.str());

        // dump meta
        stringstream metaStr;
        metaStr << mMinDocId << "," << mMaxDocId;
        mSegmentDataDirectory->Store(META_FILE_NAME, metaStr.str());
        segmentDescription.isEntireDataSet = true;
        mSegmentDataDirectory->Store(MILESTONE_FILE_NAME, "full data collected!");
        segmentDescription.useSpecifiedDeployFileList = true;
        segmentDescription.deployFileList.push_back(DATA_FILE_NAME);
        segmentDescription.deployFileList.push_back(META_FILE_NAME);
    }
    catch(const autil::legacy::ExceptionBase& e)
    {
        IE_LOG(ERROR, "fail to store index file [%s] in segment[%s], error: [%s]",
               DATA_FILE_NAME.c_str(), mSegmentData->GetSegmentDirName().c_str(), e.what());
        return false;
    }
    return true;
}

size_t MyTableWriter::EstimateInitMemoryUse(
    const TableWriterInitParamPtr& initParam) const
{
    return mMaxDocCount * sizeof(float) * DEFAULT_VALUE_COUNT;
}

size_t MyTableWriter::GetCurrentMemoryUse() const
{
    return mMaxDocCount * sizeof(float) * DEFAULT_VALUE_COUNT;
}

size_t MyTableWriter::EstimateDumpMemoryUse() const
{
    return mMaxDocCount * sizeof(float) * DEFAULT_VALUE_COUNT;
}

size_t MyTableWriter::EstimateDumpFileSize() const
{
    return mMaxDocCount * sizeof(float) * DEFAULT_VALUE_COUNT;
}

vector<TableMergePlanPtr> MyMergePolicy::CreateMergePlansForFullMerge(
    const string& mergeStrategyStr,
    const MergeStrategyParameter& mergeStrategyParameter,
    const vector<SegmentMetaPtr>& segmentMetas,
    const IE_NAMESPACE(PartitionRange)& targetRange) const
{
    // merge all segments as one segment
    TableMergePlanPtr newPlan(new TableMergePlan());
    for (const auto& segMeta : segmentMetas)
    {
        newPlan->AddSegment(segMeta->segmentDataBase.GetSegmentId());
    }
    vector<TableMergePlanPtr> newPlans;
    newPlans.push_back(newPlan);
    return newPlans;
}

vector<TableMergePlanPtr> MyMergePolicy::CreateMergePlansForIncrementalMerge(
    const string& mergeStrategyStr,
    const MergeStrategyParameter& mergeStrategyParameter,
    const vector<SegmentMetaPtr>& segmentMetas,
    const IE_NAMESPACE(PartitionRange)& targetRange) const
{
    vector<TableMergePlanPtr> plans;
    return plans;
}

bool MyMergePolicy::ReduceMergeTasks(const TableMergePlanPtr& mergePlan,
                                     const vector<MergeTaskDescription>& taskDescriptions,
                                     const vector<DirectoryPtr>& inputDirectorys,
                                     const DirectoryPtr& outputDirectory,
                                     bool isFailOver) const
{
    assert(inputDirectorys.size() == 1);
    string sourceDataFile = FileSystemWrapper::JoinPath(inputDirectorys[0]->GetPath(), DATA_FILE_NAME);
    string targetDataFile = FileSystemWrapper::JoinPath(outputDirectory->GetPath(), DATA_FILE_NAME);
    try
    {
        if (!FileSystemWrapper::IsExist(sourceDataFile))
        {
            IE_LOG(ERROR, "data file[%s] is not exist, nothing to do in END_MERGE",
                   sourceDataFile.c_str());
            return true;
        }
        if (EC_OK != FileSystem::move(sourceDataFile, targetDataFile))
        {
            IE_LOG(ERROR, "failed to move source[%s] to target[%s]",
                   sourceDataFile.c_str(), targetDataFile.c_str());
            return true;
        }
    }
    catch(const autil::legacy::ExceptionBase& e)
    {
        return false;
    }
    return true;
}

vector<MergeTaskDescription> MyMergePolicy::CreateMergeTaskDescriptions(
    const TableMergePlanPtr& mergePlan, const TableMergePlanResourcePtr& planResource,
    const vector<SegmentMetaPtr>& segmentMetas, MergeSegmentDescription& segmentDescription) const
{
    MergeTaskDescription simpleMergeTask;
    simpleMergeTask.name = DEFAULT_MERGE_TASK_NAME;
    simpleMergeTask.cost = 100;
    vector<MergeTaskDescription> taskDescriptions;
    taskDescriptions.push_back(simpleMergeTask);

    segmentDescription.docCount = mMaxDocCount;
    return taskDescriptions;
}

bool MyMergePolicy::DoInit()
{
    const CustomizedTableConfigPtr tableConfig = mSchema->GetCustomizedTableConfig();
    assert(tableConfig);
    map<string, string> parameters = tableConfig->GetParameters();
    auto iter = parameters.find("max_doc_count");
    if (iter == parameters.end()) {
        mMaxDocCount = MyTableWriter::DEFAULT_MAX_DOC_COUNT;
    } else if (!StringUtil::fromString(iter->second, mMaxDocCount)) {
        mMaxDocCount = MyTableWriter::DEFAULT_MAX_DOC_COUNT;
    }
    IE_LOG(INFO, "set max_doc_count = %d", mMaxDocCount);
    return true;
}

TableWriter* MyTableFactory::CreateTableWriter(const KeyValueMap& parameters)
{
    TableWriter* tableWriter = new MyTableWriter(parameters);
    return tableWriter;
}

TableResource* MyTableFactory::CreateTableResource(const KeyValueMap& parameters)
{
    TableResource* tableResource = new MyTableResource(parameters);
    return tableResource;
}

TableMerger* MyTableFactory::CreateTableMerger(const KeyValueMap& parameters)
{
    return new MyTableMerger;
}

MergePolicy* MyTableFactory::CreateMergePolicy(const KeyValueMap& parameters)
{
    return new MyMergePolicy;
}

TableReader* MyTableFactory::CreateTableReader(const KeyValueMap& parameters)
{
    return NULL;
}

MyTableResource::MyTableResource(const KeyValueMap& parameters)
{
}

MyTableResource::~MyTableResource()
{
}

bool MyTableResource::Init(const vector<SegmentMetaPtr>& segmentMetas,
                           const IndexPartitionSchemaPtr& schema,
                           const IndexPartitionOptions& options)
{
    mSegmentMetas.reserve(segmentMetas.size());
    for (auto segMeta : segmentMetas) {
        mSegmentMetas.push_back(segMeta);
    }
    return true;
}

bool MyTableResource::ReInit(const vector<SegmentMetaPtr>& segmentMetas)
{
    assert(false);
    return true;
}

size_t MyTableResource::GetCurrentMemoryUse() const
{
    return 0u;
}

MyTableMerger::MyTableMerger()
    : mData(NULL)
    , mMaxDocCount(0)
{
}

MyTableMerger::~MyTableMerger()
{
    if (mData)
    {
        // release memory
        for (size_t i = 0; i < mMaxDocCount; i++) {
            delete[] mData[i];
        }
        delete[] mData;
    }
}

bool MyTableMerger::Init(
        const IndexPartitionSchemaPtr& schema,
        const IndexPartitionOptions& options,
        const TableMergePlanResourcePtr& mergePlanResources,
        const TableMergePlanMetaPtr& mergePlanMeta)
{
    const CustomizedTableConfigPtr tableConfig = schema->GetCustomizedTableConfig();
    assert(tableConfig);
    map<string, string> parameters = tableConfig->GetParameters();
    auto iter = parameters.find("max_doc_count");
    if (iter == parameters.end()) {
        mMaxDocCount = MyTableWriter::DEFAULT_MAX_DOC_COUNT;
    } else if (!StringUtil::fromString(iter->second, mMaxDocCount)) {
        mMaxDocCount = MyTableWriter::DEFAULT_MAX_DOC_COUNT;
    }
    IE_LOG(INFO, "set max_doc_count = %zu", mMaxDocCount);

    if (mData != NULL)
    {
        IE_LOG(ERROR, "MyTableMerger has already been initailized");
        assert(false);
    }
    // TODO: check memory use
    mData = new float*[mMaxDocCount];
    for (size_t i = 0; i < mMaxDocCount; i++) {
        mData[i] = new float[MyTableWriter::DEFAULT_VALUE_COUNT];
        for (size_t j = 0; j < MyTableWriter::DEFAULT_VALUE_COUNT; j++) {
            mData[i][j] = 0.0;
        }
    }
    return true;
}

size_t MyTableMerger::EstimateMemoryUse(
        const vector<SegmentMetaPtr>& inPlanSegMetas,
        const MergeTaskDescription& taskDescription) const
{
    return mMaxDocCount * sizeof(float) * MyTableWriter::DEFAULT_VALUE_COUNT;
}

bool MyTableMerger::Merge(
        const vector<SegmentMetaPtr>& inPlanSegMetas,
        const MergeTaskDescription& taskDescription,
        const DirectoryPtr& outputDirectory)
{
    string dumpPath = outputDirectory->GetPath();
    // merge segment
    for (auto segMeta : inPlanSegMetas) {
        if (!MyTableWriter::RecoverFromOneSegment(segMeta, mData, mMaxDocCount)) {
            return false;
        }
    }

    try
    {
        // dump segment
        string dataPath = FileSystemWrapper::JoinPath(dumpPath, DATA_FILE_NAME);
        File* file = FileSystem::openFile(dataPath, WRITE);
        stringstream dataStr;
        for (size_t i = 0; i < mMaxDocCount; i++)
        {
            // doc id
            dataStr << i << ",";
            // value
            for (size_t j = 0; j < MyTableWriter::DEFAULT_VALUE_COUNT; j++) {
                dataStr << mData[i][j];
                if (j == MyTableWriter::DEFAULT_VALUE_COUNT - 1) {
                    dataStr << "\n";
                } else {
                    dataStr << ",";
                }
            }
            file->write((void*)(dataStr.str().c_str()), dataStr.str().size());
            dataStr.str(string());
            dataStr.clear();
        }
        file->close();
        delete file;
    }
    catch(const autil::legacy::ExceptionBase& e)
    {
        IE_LOG(ERROR, "fail to store index file [%s] in segment[%s], error: [%s]",
               DATA_FILE_NAME.c_str(), dumpPath.c_str(), e.what());
        return false;
    }
    return true;
}

/////////////////////////////////

extern "C"
ModuleFactory* createTableFactory() {
    return new MyTableFactory();
}

extern "C"
void destroyTableFactory(ModuleFactory *factory) {
    factory->destroy();
}

extern "C"
ModuleFactory* createDocumentFactory() {
    return new build_service::workflow::MyDocumentFactoryForDemo;
}

extern "C"
void destroyDocumentFactory(ModuleFactory *factory) {
    factory->destroy();
}

}
}
