#include <autil/TimeUtility.h>
#include <autil/StringUtil.h>
#include "indexlib/merger/merge_work_item.h"
#include "indexlib/storage/file_system_wrapper.h"

using namespace std;
using namespace autil;
IE_NAMESPACE_USE(storage);

IE_NAMESPACE_BEGIN(merger);
IE_LOG_SETUP(merger, MergeWorkItem);

void MergeWorkItem::Process()
{
    assert(mMergeFileSystem);
    if (mMergeFileSystem->HasCheckpoint(mCheckPointName))
    {
        IE_LOG(INFO, "mergeWorkItem [%s] has already been done!", mIdentifier.c_str());
        ReportMetrics();
        return;
    }
    
    DoProcess();
    
    if (!mCheckPointName.empty())
    {
        mMergeFileSystem->MakeCheckpoint(mCheckPointName);
    }
}

IE_NAMESPACE_END(merger);

