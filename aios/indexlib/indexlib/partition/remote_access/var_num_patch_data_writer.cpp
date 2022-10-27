#include "indexlib/partition/remote_access/var_num_patch_data_writer.h"
#include "indexlib/index/normal/attribute/accessor/attribute_data_info.h"
#include "indexlib/index/normal/attribute/adaptive_attribute_offset_dumper.h"
#include "indexlib/common/field_format/attribute/attribute_convertor_factory.h"
#include "indexlib/config/merge_io_config.h"
#include "indexlib/file_system/file_writer.h"
#include "indexlib/util/path_util.h"

using namespace std;
using namespace autil;

IE_NAMESPACE_USE(config);
IE_NAMESPACE_USE(file_system);
IE_NAMESPACE_USE(index);
IE_NAMESPACE_USE(util);
IE_NAMESPACE_USE(common);

IE_NAMESPACE_BEGIN(partition);
IE_LOG_SETUP(partition, VarNumPatchDataWriter);

VarNumPatchDataWriter::VarNumPatchDataWriter()
    : mFileOffset(0)
    , mDataItemCount(0)
    , mMaxItemLen(0)
{
}

VarNumPatchDataWriter::~VarNumPatchDataWriter()
{
    mOffsetDumper.reset();
}

bool VarNumPatchDataWriter::Init(const AttributeConfigPtr& attrConfig,
                                 const MergeIOConfig& mergeIOConfig,
                                 const string& dirPath)
{
    mFileOffset = 0;
    mDataItemCount = 0;
    mMaxItemLen = 0;

    string dataFilePath = PathUtil::JoinPath(dirPath, ATTRIBUTE_DATA_FILE_NAME);
    mDataFile.reset(CreateBufferedFileWriter(mergeIOConfig.writeBufferSize,
                    mergeIOConfig.enableAsyncWrite));
    mDataFile->Open(dataFilePath);
    
    string offsetFilePath = PathUtil::JoinPath(dirPath, ATTRIBUTE_OFFSET_FILE_NAME);
    mOffsetFile.reset(
            CreateBufferedFileWriter(mergeIOConfig.writeBufferSize,
                    mergeIOConfig.enableAsyncWrite));
    mOffsetFile->Open(offsetFilePath);

    string dataInfoFilePath = PathUtil::JoinPath(dirPath, ATTRIBUTE_DATA_INFO_FILE_NAME);
    // small file no need async write
    mDataInfoFile.reset(CreateBufferedFileWriter(mergeIOConfig.writeBufferSize, false));
    mDataInfoFile->Open(dataInfoFilePath);

    mOffsetDumper.reset(new AdaptiveAttributeOffsetDumper(&mPool));
    mOffsetDumper->Init(attrConfig);

    if (attrConfig->IsUniqEncode())
    {
        mEncodeMap.reset(new EncodeMap);
    }

    mAttrConvertor.reset(
            AttributeConvertorFactory::GetInstance()->CreateAttrConvertor(
                    attrConfig->GetFieldConfig()));
    if (!mAttrConvertor)
    {
        IE_LOG(ERROR, "create attribute convertor fail!");
        return false;
    }
    return true;
}

void VarNumPatchDataWriter::AppendValue(const ConstString& encodeValue)
{
    assert(mAttrConvertor);
    common::AttrValueMeta meta = mAttrConvertor->Decode(encodeValue);
    const autil::ConstString& value = meta.data;

    uint64_t offset = mFileOffset;
    if (mEncodeMap)
    {
        EncodeMap::const_iterator iter = mEncodeMap->find(meta.hashKey);
        if (iter != mEncodeMap->end())
        {
            offset = iter->second;
            mOffsetDumper->PushBack(offset);
            return;
        }
        mEncodeMap->insert(make_pair(meta.hashKey, offset));
    }
    mOffsetDumper->PushBack(offset);
    mFileOffset += value.size();
    mDataFile->Write(value.data(), value.size());
    ++mDataItemCount;
    mMaxItemLen = std::max(mMaxItemLen, (uint32_t)value.size());
}

void VarNumPatchDataWriter::Close()
{
    AttributeDataInfo dataInfo(mDataItemCount, mMaxItemLen);
    string content = dataInfo.ToString();
    mDataInfoFile->Write(content.c_str(), content.length());

    assert(mOffsetDumper);
    if (mOffsetDumper->Size() > 0)
    {
        mOffsetDumper->PushBack(mFileOffset);
        mOffsetDumper->Dump(mOffsetFile);
        mOffsetDumper->Clear();  // release memory right now
    }

    mDataFile->Close();
    mOffsetFile->Close();
    mDataInfoFile->Close();
}

IE_NAMESPACE_END(partition);

