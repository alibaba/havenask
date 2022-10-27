#include "indexlib/file_system/indexlib_file_system.h"
#include "indexlib/storage/file_system_wrapper.h"
#include "indexlib/util/path_util.h"

using namespace std;
using namespace fslib;
using namespace autil;

IE_NAMESPACE_USE(storage);
IE_NAMESPACE_USE(util);

IE_NAMESPACE_BEGIN(file_system);
IE_LOG_SETUP(file_system, IndexlibFileSystem);

void IndexlibFileSystem::DeleteRootLinks(const string& rootPath)
{
    FileList fileList;
    FileSystemWrapper::ListDir(rootPath, fileList, true);
    for (size_t i = 0; i < fileList.size(); i++)
    {
        if (fileList[i].find(FILE_SYSTEM_ROOT_LINK_NAME) == 0)
        {
            string linkPath = PathUtil::JoinPath(rootPath, fileList[i]);
            IE_LOG(INFO, "delete root link [%s]", linkPath.c_str());
            FileSystemWrapper::DeleteIfExist(linkPath);
        }
    }
}

IE_NAMESPACE_END(file_system);

