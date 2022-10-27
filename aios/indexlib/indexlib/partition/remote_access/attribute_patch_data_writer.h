#ifndef __INDEXLIB_ATTRIBUTE_PATCH_DATA_WRITER_H
#define __INDEXLIB_ATTRIBUTE_PATCH_DATA_WRITER_H

#include <tr1/memory>
#include <autil/ConstString.h>
#include "indexlib/indexlib.h"
#include "indexlib/common_define.h"

DECLARE_REFERENCE_CLASS(config, AttributeConfig);
DECLARE_REFERENCE_CLASS(storage, IOConfig);
DECLARE_REFERENCE_CLASS(file_system, FileWriter);

IE_NAMESPACE_BEGIN(partition);

class AttributePatchDataWriter
{
public:
    AttributePatchDataWriter() {}
    virtual ~AttributePatchDataWriter() {}
    
public:
    virtual bool Init(const config::AttributeConfigPtr& attrConfig,
                      const storage::IOConfig& mergeIOConfig,
                      const std::string& dirPath) = 0;
    
    virtual void AppendValue(const autil::ConstString& value) = 0;

    virtual void Close() = 0;
    virtual file_system::FileWriterPtr TEST_GetDataFileWriter() = 0;

protected:
    file_system::FileWriter* CreateBufferedFileWriter(
        size_t bufferSize, bool asyncWrite);
private:
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(AttributePatchDataWriter);

IE_NAMESPACE_END(partition);

#endif //__INDEXLIB_ATTRIBUTE_PATCH_DATA_WRITER_H
