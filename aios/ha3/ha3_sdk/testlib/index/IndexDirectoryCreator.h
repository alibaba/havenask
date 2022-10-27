#ifndef ISEARCH_DIRECTORYCREATOR_H
#define ISEARCH_DIRECTORYCREATOR_H

#include <ha3/common.h>
#include <ha3/isearch.h>
#include <ha3/util/Log.h>
#include <indexlib/indexlib.h>
#include <indexlib/common_define.h>
#include <indexlib/file_system/directory.h>

IE_NAMESPACE_BEGIN(index);

class IndexDirectoryCreator
{
public:
    IndexDirectoryCreator();
    ~IndexDirectoryCreator();

public:
    static file_system::DirectoryPtr Create(const std::string& path);
};

IE_NAMESPACE_END(index);

#endif //ISEARCH_DIRECTORYCREATOR_H
