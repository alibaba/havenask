#ifndef __INDEXLIB_INDEX_FORMAT_H
#define __INDEXLIB_INDEX_FORMAT_H

#include <tr1/memory>
#include "indexlib/indexlib.h"

#include "indexlib/common_define.h"
#include "indexlib/file_system/directory.h"
#include "autil/legacy/jsonizable.h"

IE_NAMESPACE_BEGIN(index_base);

class IndexFormat : public autil::legacy::Jsonizable
{
public:
    IndexFormat() {}
    virtual ~IndexFormat() {}

public:
    virtual void Store(const file_system::DirectoryPtr& dir) = 0;
    virtual void Load(const file_system::DirectoryPtr& dir) = 0;
    
    virtual std::string ToString() = 0;
    virtual void FromString(const std::string& content) = 0;

private:
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(IndexFormat);

IE_NAMESPACE_END(index_base);

#endif //__INDEXLIB_INDEX_FORMAT_H
