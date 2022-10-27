#include "indexlib/table/demo/demo_table_writer.h"
#include "indexlib/index_base/segment/segment_data_base.h"
#include "indexlib/table/table_resource.h"
#include "indexlib/plugin/plugin_manager.h"
#include "indexlib/file_system/directory.h"
#include "indexlib/document/index_document/normal_document/normal_document.h"
#include "indexlib/table/demo/demo_building_segment_reader.h"
#include "indexlib/index/online_join_policy.h"
#include <autil/ConstString.h>
#include <autil/StringUtil.h>

using namespace std;
using namespace autil;

IE_NAMESPACE_USE(plugin);
IE_NAMESPACE_USE(file_system);
IE_NAMESPACE_USE(config);
IE_NAMESPACE_USE(document);
IE_NAMESPACE_USE(common);

IE_NAMESPACE_BEGIN(table);
IE_LOG_SETUP(table, DemoTableWriter);

std::string DemoTableWriter::DATA_FILE_NAME = "demo_data_file";
std::string DemoTableWriter::MILESTONE_FILE_NAME = "demo_milestone_file";

DemoTableWriter::DemoTableWriter(
    const util::KeyValueMap& parameters)
    : TableWriter()
    , mIsDirty(false)
    , mIsOnline(false)
    , mRtFilterTimestamp(-1)
    , mThreshold(5u)
{
    
    auto it = parameters.find("create_new_version_threshold");
    if (it != parameters.end())
    {
        if (!StringUtil::numberFromString(it->second, mThreshold))
        {
            IE_LOG(ERROR, "create_new_version_threshold[%s] cannot be converted to interger type",
                   it->second.c_str());
        }
        else
        {
            IE_LOG(INFO, "create_new_version_threshold set to [%zu]", mThreshold); 
        }
    }
}

DemoTableWriter::~DemoTableWriter() 
{
}

bool DemoTableWriter::DoInit()
{
    mIsDirty = false;
    mIsOnline = mOptions.IsOnline();
    mDataLock.reset(new autil::ThreadMutex());
    mData.reset(new std::map<std::string, std::string>());
    index::OnlineJoinPolicy joinPolicy(mOnDiskVersion, mSchema->GetTableType());
    mRtFilterTimestamp = joinPolicy.GetRtFilterTimestamp(); 
    return true;
}

TableWriter::BuildResult DemoTableWriter::Build(docid_t docId, const DocumentPtr& doc)
{
    if (mIsOnline && mRtFilterTimestamp != -1)
    {
        int64_t timestamp = doc->GetTimestamp();
        if (timestamp < mRtFilterTimestamp)
        {
            IE_LOG(ERROR, "rt doc with ts[%ld], incTs[%ld], drop rt doc",
                   timestamp, mRtFilterTimestamp);
            return TableWriter::BuildResult::BR_FAIL;
        }
    }
    NormalDocumentPtr normDoc = DYNAMIC_POINTER_CAST(NormalDocument, doc);
    if (!normDoc) {
        IE_LOG(ERROR, "cast to NormalDocument failed for docId[%d]", docId);
        return TableWriter::BuildResult::BR_FAIL;
    }

    const string& pkStr = normDoc->GetPrimaryKey();
    IndexDocumentPtr indexDoc = normDoc->GetIndexDocument();

    if (!indexDoc)
    {
        IE_LOG(ERROR, "no indexDoc in NormalDocument for docId[%d]", docId);
        return TableWriter::BuildResult::BR_FAIL;
    }

    auto fieldSchema = mSchema->GetFieldSchema();
    auto field1Config = fieldSchema->GetFieldConfig("cfield1");
    auto field2Config = fieldSchema->GetFieldConfig("cfield2");

    Field* field1 = indexDoc->GetField(field1Config->GetFieldId());
    Field* field2 = indexDoc->GetField(field2Config->GetFieldId());    

    if (!field1 || !field2)
    {
        IE_LOG(ERROR, "missing fields in docId[%d]", docId);
        return TableWriter::BuildResult::BR_FAIL;
    }

    const ConstString& field1Val = dynamic_cast<IndexRawField*>(field1)->GetData();
    const ConstString& field2Val = dynamic_cast<IndexRawField*>(field2)->GetData();    

    string val = field1Val.toString() + ":" + field2Val.toString();

    {
        ScopedLock lock(*mDataLock);
        auto it = mData->find(pkStr);
        if (it == mData->end())
        {
            mData->insert(make_pair(pkStr, val));
        }
        else
        {
            it->second = val;
        }
    }
    mIsDirty = true;
    return TableWriter::BuildResult::BR_OK;
}

bool DemoTableWriter::IsDirty() const
{
    return mIsDirty;
}

bool DemoTableWriter::DumpSegment(BuildSegmentDescription& segmentDescription)
{
    IE_LOG(INFO, "DemoTableWriter dump to [%s] begin", mSegmentDataDirectory->GetPath().c_str());
    try
    {
        string fileJsonStr = autil::legacy::ToJsonString(*mData);
        mSegmentDataDirectory->Store(DATA_FILE_NAME, fileJsonStr);
        
        if (mData->size() >= mThreshold) {
            segmentDescription.isEntireDataSet = true;
            mSegmentDataDirectory->Store(MILESTONE_FILE_NAME, "full data collected!");
            segmentDescription.useSpecifiedDeployFileList = true;
            segmentDescription.deployFileList.push_back(DATA_FILE_NAME);
        }
        else { 
            segmentDescription.isEntireDataSet = false;
            segmentDescription.useSpecifiedDeployFileList = false;
        }
    }
    catch(const autil::legacy::ExceptionBase& e)
    {
        IE_LOG(ERROR, "fail to store index file [%s] in segment[%s], error: [%s]",
               DATA_FILE_NAME.c_str(), mSegmentData->GetSegmentDirName().c_str(), e.what());
        return false;
    }
    IE_LOG(INFO, "DemoTableWriter dump end"); 
    return true; 
}

BuildingSegmentReaderPtr DemoTableWriter::CreateBuildingSegmentReader(
    const util::UnsafeSimpleMemoryQuotaControllerPtr&)
{
    return DemoBuildingSegmentReaderPtr(
        new DemoBuildingSegmentReader(mDataLock, mData));
}

size_t DemoTableWriter::EstimateInitMemoryUse(
    const TableWriterInitParamPtr& initParam) const
{
    return 0u;
}

size_t DemoTableWriter::GetCurrentMemoryUse() const
{
    return 0u;
}

size_t DemoTableWriter::EstimateDumpMemoryUse() const
{
    return 0u;
}

size_t DemoTableWriter::EstimateDumpFileSize() const
{
    return 0u;
}

IE_NAMESPACE_END(table);

