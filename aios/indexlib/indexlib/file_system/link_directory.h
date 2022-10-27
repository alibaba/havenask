#ifndef __INDEXLIB_LINK_DIRECTORY_H
#define __INDEXLIB_LINK_DIRECTORY_H

#include <tr1/memory>
#include "indexlib/indexlib.h"
#include "indexlib/common_define.h"
#include "indexlib/file_system/local_directory.h"

IE_NAMESPACE_BEGIN(file_system);

// LinkDirectory is read only, for flushed rt segments
class LinkDirectory : public LocalDirectory
{
public:
    LinkDirectory(const std::string& path, 
                  const IndexlibFileSystemPtr& indexFileSystem);
    
    ~LinkDirectory();
    
public:
    DirectoryPtr MakeInMemDirectory(const std::string& dirName) override;
    DirectoryPtr MakeDirectory(const std::string& dirPath) override;
    DirectoryPtr GetDirectory(const std::string& dirPath,
                              bool throwExceptionIfNotExist) override;

    FileWriterPtr CreateFileWriter(const std::string& filePath,
                                   const FSWriterParam& param = FSWriterParam()) override;

private:
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(LinkDirectory);

IE_NAMESPACE_END(file_system);

#endif //__INDEXLIB_LINK_DIRECTORY_H
