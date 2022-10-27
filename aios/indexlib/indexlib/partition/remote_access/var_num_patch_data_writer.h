#ifndef __INDEXLIB_VAR_NUM_PATCH_DATA_WRITER_H
#define __INDEXLIB_VAR_NUM_PATCH_DATA_WRITER_H

#include <tr1/memory>
#include <tr1/unordered_map>
#include "indexlib/indexlib.h"
#include "indexlib/common_define.h"
#include "indexlib/partition/remote_access/attribute_patch_data_writer.h"
#include "indexlib/util/simple_pool.h"

DECLARE_REFERENCE_CLASS(common, AttributeConvertor);
DECLARE_REFERENCE_CLASS(index, AdaptiveAttributeOffsetDumper);
DECLARE_REFERENCE_CLASS(file_system, FileWriter);
DECLARE_REFERENCE_CLASS(storage, IOConfig);
IE_NAMESPACE_BEGIN(partition);

class VarNumPatchDataWriter : public AttributePatchDataWriter
{
public:
    VarNumPatchDataWriter();
    ~VarNumPatchDataWriter();
    
public:
    bool Init(const config::AttributeConfigPtr& attrConfig,
              const storage::IOConfig& mergeIOConfig,
              const std::string& dirPath) override;
    
    void AppendValue(const autil::ConstString& value) override;

    void Close() override;

    file_system::FileWriterPtr TEST_GetDataFileWriter() override
    { return mDataFile; }
private:
    typedef std::tr1::unordered_map<uint64_t, uint64_t> EncodeMap;
    DEFINE_SHARED_PTR(EncodeMap);

    uint64_t mFileOffset;
    uint32_t mDataItemCount;
    uint32_t mMaxItemLen;

    common::AttributeConvertorPtr mAttrConvertor;
    EncodeMapPtr mEncodeMap;
    index::AdaptiveAttributeOffsetDumperPtr mOffsetDumper;
    util::SimplePool mPool;

    file_system::FileWriterPtr mDataFile;
    file_system::FileWriterPtr mOffsetFile;
    file_system::FileWriterPtr mDataInfoFile;

private:
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(VarNumPatchDataWriter);

IE_NAMESPACE_END(partition);

#endif //__INDEXLIB_VAR_NUM_PATCH_DATA_WRITER_H
