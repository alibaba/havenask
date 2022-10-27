// #include "indexlib/file_system/archive_folder.h"
// #include "indexlib/storage/file_system_wrapper.h"
// #include <iostream>
// using namespace std;
// IE_NAMESPACE_USE(storage);
// IE_NAMESPACE_USE(file_system);

// int main(int argc, char* argv[])
// {
//     if (argc != 5) {
//         cout << "invlid param, example:archive_folder_tool dirPath fileName openMode[READ/WRITE] needClose(TRUE/FALSE)" << endl;
//         return 0;
//     }
//     string dirPath(argv[1]);
//     string fileName(argv[2]);
//     fslib::Flag openMode;
//     if (string(argv[3]) == string("READ"))
//     {
//         openMode = fslib::READ;
//     }
//     else 
//     {
//         openMode = fslib::WRITE;
//     }
//     bool needClose;
//     if (string(argv[4]) == string("FALSE"))
//     {
//         needClose = false;
//     } else
//     {
//         needClose = true;
//     }

//     FileSystemWrapper::MkDirIfNotExist(dirPath);
//     ArchiveFolder folder;
//     folder.Open(dirPath);
//     ArchiveFilePtr file = folder.GetInnerFile(fileName, openMode);
//     char buffer[100];
//     if (openMode == fslib::READ)
//     {
//         size_t len = file->Read(buffer, 100);
//         string content(buffer, len);
//         cout << content;
//     }

//     if (openMode == fslib::WRITE)
//     {
//         file->Write(fileName.c_str(), fileName.size());
//     }
//     file->Close();
//     if (needClose) {
//         folder.Close();
//     }
//     return 0;
// }
