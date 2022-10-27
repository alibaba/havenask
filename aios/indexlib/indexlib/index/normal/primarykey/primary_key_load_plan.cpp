#include "indexlib/index/normal/primarykey/primary_key_load_plan.h"
#include "indexlib/index/normal/primarykey/primary_key_loader.h"

using namespace std;
IE_NAMESPACE_USE(file_system);
IE_NAMESPACE_USE(index_base);
IE_NAMESPACE_USE(config);

IE_NAMESPACE_BEGIN(index);
IE_LOG_SETUP(index, PrimaryKeyLoadPlan);

PrimaryKeyLoadPlan::PrimaryKeyLoadPlan() 
    : mBaseDocid(INVALID_DOCID)
    , mDocCount(0)
    , mDeleteDocCount(0)
{
}

PrimaryKeyLoadPlan::~PrimaryKeyLoadPlan() 
{
}

void PrimaryKeyLoadPlan::Init(docid_t baseDocid, 
                              const PrimaryKeyIndexConfigPtr& indexConfig)
{
    mBaseDocid = baseDocid;
    mIndexConfig = indexConfig;
    mDocCount = 0;
}

bool PrimaryKeyLoadPlan::CanDirectLoad() const
{
    if (mSegmentDatas.size() > 1)
    {
        return false;
    }
    PrimaryKeyLoadStrategyParam::PrimaryKeyLoadMode loadMode
        = mIndexConfig->GetPKLoadStrategyParam()->GetPrimaryKeyLoadMode();
    PrimaryKeyIndexType pkIndexType = mIndexConfig->GetPrimaryKeyIndexType();
    if (loadMode == PrimaryKeyLoadStrategyParam::HASH_TABLE
        && pkIndexType == pk_hash_table)
    {
        return true;
    }

    if (loadMode == PrimaryKeyLoadStrategyParam::SORTED_VECTOR
        && pkIndexType == pk_sort_array)
    {
        return true;
    }
    return false;
}

string PrimaryKeyLoadPlan::GetTargetFileName() const
{
    if (CanDirectLoad())
    {
        return PRIMARY_KEY_DATA_FILE_NAME;
    }
    stringstream ss;
    //TODO
    ss << PRIMARY_KEY_DATA_SLICE_FILE_NAME;
    for (size_t i = 0; i < mSegmentDatas.size(); i++)
    {
        ss << "_" << mSegmentDatas[i].GetSegmentId();
    }
    return ss.str();
}

file_system::DirectoryPtr PrimaryKeyLoadPlan::GetTargetFileDirectory() const
{
    assert(mSegmentDatas.size() > 0);
    SegmentData maxSegData = mSegmentDatas[mSegmentDatas.size() - 1];
    return PrimaryKeyLoader<uint64_t>::GetPrimaryKeyDirectory(
            maxSegData.GetDirectory(), mIndexConfig);
}

IE_NAMESPACE_END(index);

