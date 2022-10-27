#include "indexlib/index/normal/attribute/adaptive_attribute_offset_dumper.h"
#include "indexlib/index/normal/attribute/accessor/attribute_compress_info.h"
#include "indexlib/config/attribute_config.h"

using namespace std;
IE_NAMESPACE_USE(config);
IE_NAMESPACE_USE(file_system);

IE_NAMESPACE_BEGIN(index);
IE_LOG_SETUP(index, AdaptiveAttributeOffsetDumper);

AdaptiveAttributeOffsetDumper::AdaptiveAttributeOffsetDumper(
        autil::mem_pool::PoolBase* pool)
    : mPool(pool)
    , mEnableCompress(false)
    , mOffsetThreshold(VAR_NUM_ATTRIBUTE_OFFSET_THRESHOLD)
    , mReserveCount(0)
    , mU32Allocator(pool)
    , mU64Allocator(pool)
    , mU32Offsets(mU32Allocator)
{
}

AdaptiveAttributeOffsetDumper::~AdaptiveAttributeOffsetDumper() 
{
}

void AdaptiveAttributeOffsetDumper::Init(const AttributeConfigPtr& attrConf)
{
    mOffsetThreshold =             
        attrConf->GetFieldConfig()->GetU32OffsetThreshold();
    mEnableCompress =         
        AttributeCompressInfo::NeedCompressOffset(attrConf);
    if (mEnableCompress && attrConf->IsAttributeUpdatable())
    {
        // compress & updatable offset use u64
        mU64Offsets.reset(new U64Vector(mU64Allocator));
    }
}

void AdaptiveAttributeOffsetDumper::Clear()
{
    U32Vector(mU32Allocator).swap(mU32Offsets);

    if (mU64Offsets)
    {
        mU64Offsets.reset(new U64Vector(mU64Allocator));
    }

    if (mReserveCount > 0)
    {
        Reserve(mReserveCount);
    }
}

void AdaptiveAttributeOffsetDumper::Dump(const FileWriterPtr& offsetFile)
{
    if (!mEnableCompress)
    {
        size_t offsetUnitSize = IsU64Offset() ? sizeof(uint64_t) : sizeof(uint32_t);
        offsetFile->ReserveFileNode(offsetUnitSize * Size());
    }

    if (mU64Offsets)
    {
        WriteOffsetData<uint64_t, FileWriterPtr>(*mU64Offsets, offsetFile);
    }
    else
    {
        WriteOffsetData<uint32_t, FileWriterPtr>(mU32Offsets, offsetFile);
    }
}

IE_NAMESPACE_END(index);

