#include "indexlib/table/demo/demo_table_merger.h"
#include "indexlib/table/demo/demo_table_writer.h"
#include "indexlib/storage/file_system_wrapper.h"
#include "autil/legacy/jsonizable.h"

using namespace std;

IE_NAMESPACE_USE(config);
IE_NAMESPACE_USE(file_system);

IE_NAMESPACE_BEGIN(table);
IE_LOG_SETUP(table, DemoTableMerger);

DemoTableMerger::DemoTableMerger(const util::KeyValueMap& parameters) 
{
}

DemoTableMerger::~DemoTableMerger() 
{
}
bool DemoTableMerger::Init(
    const IndexPartitionSchemaPtr& schema,
    const IndexPartitionOptions& options,
    const TableMergePlanResourcePtr& mergePlanResources,
    const TableMergePlanMetaPtr& mergePlanMeta)
{
    return true;
}

size_t DemoTableMerger::EstimateMemoryUse(
    const vector<SegmentMetaPtr>& inPlanSegMetas,
    const table::MergeTaskDescription& taskDescription) const
{
    return 0;
}

bool DemoTableMerger::Merge(
    const vector<SegmentMetaPtr>& inPlanSegMetas,
    const table::MergeTaskDescription& taskDescription,
    const DirectoryPtr& outputDirectory)
{
    std::map<std::string, std::string> mergedData;
    try
    {
        for (const auto& segMeta : inPlanSegMetas)
        {
            DirectoryPtr dataDir = segMeta->segmentDataDirectory;
            if (!dataDir->IsExist(DemoTableWriter::DATA_FILE_NAME))
            {
                IE_LOG(ERROR, "data file is missing in segment[%d]",
                       segMeta->segmentDataBase.GetSegmentId());
            }
            string fileContent;
            dataDir->Load(DemoTableWriter::DATA_FILE_NAME, fileContent);
            std::map<std::string, std::string> segData;
            autil::legacy::FromJsonString(segData, fileContent);

            for (const auto& kv : segData)
            {
                mergedData.insert(kv);
            }
        }        
    }
    catch(const misc::ExceptionBase& e)
    {
        return false;
    }
    try
    {
        // TODO: implement merge
        outputDirectory->Store(DemoTableWriter::DATA_FILE_NAME, autil::legacy::ToJsonString(mergedData));
    }
    catch(const misc::ExceptionBase& e)
    {
        return false;
    }    
    return true;
}

IE_NAMESPACE_END(table);

