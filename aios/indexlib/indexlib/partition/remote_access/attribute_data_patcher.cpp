#include "indexlib/partition/remote_access/attribute_data_patcher.h"
#include "indexlib/index_define.h"
#include "indexlib/common/field_format/attribute/attribute_convertor_factory.h"
#include "indexlib/storage/file_system_wrapper.h"
#include "indexlib/config/attribute_config.h"
#include "indexlib/config/merge_io_config.h"
#include "indexlib/util/path_util.h"

using namespace std;
using namespace autil;

IE_NAMESPACE_USE(config);
IE_NAMESPACE_USE(index);
IE_NAMESPACE_USE(util);
IE_NAMESPACE_USE(common);
IE_NAMESPACE_USE(storage);

IE_NAMESPACE_BEGIN(partition);
IE_LOG_SETUP(partition, AttributeDataPatcher);

AttributeDataPatcher::AttributeDataPatcher()
    : mTotalDocCount(0)
    , mPatchedDocCount(0)
    , mPool(POOL_CHUNK_SIZE)
{
}

AttributeDataPatcher::~AttributeDataPatcher() 
{
    assert(!mPatchDataWriter);
    if (mPatchDataWriter)
    {
        IE_LOG(ERROR, "patcher for [%s] is not closed!",
               mAttrConf->GetAttrName().c_str());
    }
}

bool AttributeDataPatcher::Init(const AttributeConfigPtr& attrConfig,
                                const config::MergeIOConfig& mergeIOConfig,
                                const string& segDirPath, uint32_t docCount)
{
    mAttrConf = attrConfig;
    mTotalDocCount = docCount;
    mPatchedDocCount = 0;
    mAttrConvertor.reset(AttributeConvertorFactory::GetInstance()->CreateAttrConvertor(
                    mAttrConf->GetFieldConfig()));
    if (!mAttrConvertor)
    {
        IE_LOG(ERROR, "create attribute convertor fail!");
        return false;
    }
    mAttrConvertor->SetEncodeEmpty(true);
    if (!FileSystemWrapper::IsExist(segDirPath))
    {
        IE_LOG(ERROR, "[%s] not exist!", segDirPath.c_str());
        return false;
    }
    
    if (mTotalDocCount == 0)
    {
        IE_LOG(INFO, "Init AttributeDataPatcher [%s] for empty segment [%s]",
               attrConfig->GetAttrName().c_str(), segDirPath.c_str());
        return true;
    }
    string attrDirPath = PathUtil::JoinPath(segDirPath, ATTRIBUTE_DIR_NAME);
    FileSystemWrapper::MkDirIfNotExist(attrDirPath);
    mDirPath = PathUtil::JoinPath(attrDirPath, attrConfig->GetAttrName());
    if (FileSystemWrapper::IsExist(mDirPath))
    {
        IE_LOG(ERROR, "target path [%s] already exist, will be cleaned!",
               mDirPath.c_str());
        FileSystemWrapper::DeleteIfExist(mDirPath);
    }
    FileSystemWrapper::MkDir(mDirPath);
    return DoInit(mergeIOConfig);
}

void AttributeDataPatcher::AppendFieldValue(const string& valueStr)
{
    ConstString inputStr = ConstString(valueStr);
    AppendFieldValue(inputStr);
}

void AttributeDataPatcher::AppendAllDocByDefaultValue()
{
    assert(mPatchedDocCount == 0);
    IE_LOG(INFO, "append all doc by default value");
    const string& defaultValue =
        mAttrConf->GetFieldConfig()->GetDefaultValue();
    ConstString inputStr = ConstString(defaultValue);
    assert(mAttrConvertor);
    ConstString encodeValue = mAttrConvertor->Encode(inputStr, &mPool);
    for (uint32_t i = mPatchedDocCount; i < mTotalDocCount; i++)
    {
        AppendEncodedValue(encodeValue);
    }
}

IE_NAMESPACE_END(partition);

