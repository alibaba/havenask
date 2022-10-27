#ifndef __INDEXLIB_PRIMARY_KEY_ATTRIBUTE_SEGMENT_READER_H
#define __INDEXLIB_PRIMARY_KEY_ATTRIBUTE_SEGMENT_READER_H

#include <tr1/memory>
#include "indexlib/indexlib.h"

#include "indexlib/common_define.h"
#include "indexlib/index/normal/attribute/accessor/single_value_attribute_segment_reader.h"

IE_NAMESPACE_BEGIN(index);

template<typename Key>
class PrimaryKeyAttributeSegmentReader : 
    public SingleValueAttributeSegmentReader<Key>
{
public:
    PrimaryKeyAttributeSegmentReader(const config::AttributeConfigPtr& config)
        : SingleValueAttributeSegmentReader<Key>(config)
    { }
    ~PrimaryKeyAttributeSegmentReader() { }

private:
    file_system::FileReaderPtr CreateFileReader(
            const file_system::DirectoryPtr& directory,
            const std::string& fileName) const override
    {
        return directory->CreateIntegratedFileReader(fileName);
    }

private:
    IE_LOG_DECLARE();
};

IE_NAMESPACE_END(index);

#endif //__INDEXLIB_PRIMARY_KEY_ATTRIBUTE_SEGMENT_READER_H
