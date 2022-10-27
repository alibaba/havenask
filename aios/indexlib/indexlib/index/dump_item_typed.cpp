#include "indexlib/index/dump_item_typed.h"
#include "indexlib/index/normal/attribute/accessor/attribute_updater.h"

using namespace std;
using namespace autil::mem_pool;

IE_NAMESPACE_BEGIN(index);

void AttributeUpdaterDumpItem::process(PoolBase* pool)
{
    mUpdater->Dump(mDirectory, mSrcSegId);
}

IE_NAMESPACE_END(index);

