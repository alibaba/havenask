#include "indexlib/index/normal/inverted_index/customized_index/index_reducer.h"
#include "indexlib/storage/file_system_wrapper.h"
#include "indexlib/misc/exception.h"

using namespace std;
using namespace autil::legacy;

IE_NAMESPACE_USE(index_base);
IE_NAMESPACE_USE(file_system);
IE_NAMESPACE_USE(storage);
IE_NAMESPACE_USE(misc);

IE_NAMESPACE_BEGIN(index);
IE_LOG_SETUP(index, IndexReducer);

void ParallelReduceMeta::Store(const string& dir) const
{
    string filePath = FileSystemWrapper::JoinPath(dir, GetFileName());
    FileSystemWrapper::DeleteIfExist(filePath);
    FileSystemWrapper::AtomicStore(filePath, ToJsonString(*this));
}

void ParallelReduceMeta::Store(const DirectoryPtr& directory) const
{
    string fileName = GetFileName();
    directory->RemoveFile(fileName, true);
    directory->Store(fileName, ToJsonString(*this));
}

bool ParallelReduceMeta::Load(const DirectoryPtr& directory)
{
    string fileName = GetFileName();
    string content;
    try
    {
        if (!directory->IsExist(fileName))
        {
            return false;
        }
        directory->Load(fileName, content);
    }
    catch (...)
    {
        throw;
    }
    FromJsonString(*this, content);
    return true;
}
    
bool ParallelReduceMeta::Load(const std::string& path)
{
    string filePath = FileSystemWrapper::JoinPath(path, GetFileName());
    string content;
    try
    {
        if (!FileSystemWrapper::AtomicLoad(filePath, content, true))
        {
            return false;
        }
    }
    catch (...)
    {
        throw;
    }
    FromJsonString(*this, content);
    return true;
}

string ParallelReduceMeta::GetInsDirName(uint32_t id)
{
    return ParallelMergeItem::GetParallelInstanceDirName(parallelCount, id);
}

ReduceResource::ReduceResource(const SegmentMergeInfos& _segMergeInfos,
                               const ReclaimMapPtr& _reclaimMap,
                               bool _isEntireDataSet)
    : segMergeInfos(_segMergeInfos)
    , reclaimMap(_reclaimMap)
    , isEntireDataSet(_isEntireDataSet)
{}

ReduceResource::~ReduceResource() {}

bool IndexReducer::Init(const IndexerResourcePtr& resource,
                        const MergeItemHint& mergeHint,
                        const MergeTaskResourceVector& taskResources)
{
    if (!resource->schema)
    {
        IE_LOG(ERROR, "indexer init failed: schema is NULL");
        return false;
    }
    mMergeHint = mergeHint;
    mTaskResources = taskResources;
    return DoInit(resource);
}

vector<ReduceTask> IndexReducer::CreateReduceTasks(
    const vector<DirectoryPtr>& indexDirs,
    const SegmentMergeInfos& segmentInfos, uint32_t instanceCount,
    bool isEntireDataSet, MergeTaskResourceManagerPtr& resMgr)
{
    vector<ReduceTask> emptyReduceTask;
    return emptyReduceTask;
}

void IndexReducer::EndParallelReduce(const OutputSegmentMergeInfos& outputSegMergeInfos,
    int32_t totalParallelCount,
    const std::vector<index_base::MergeTaskResourceVector>& instResourceVec)
{
    for (const auto& info : outputSegMergeInfos)
    {
        ParallelReduceMeta meta(totalParallelCount);
        meta.Store(info.directory);
    }
}

IE_NAMESPACE_END(index);

