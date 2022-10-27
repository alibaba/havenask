#include "build_service/reader/FileReaderBase.h"
#include "build_service/reader/GZipFileReader.h"
#include "build_service/reader/FileReader.h"
#include "build_service/util/FileUtil.h"
#include <autil/StringUtil.h>

namespace build_service {
namespace reader {
BS_LOG_SETUP(reader, FileReaderBase);

using namespace std;
using namespace autil;
using namespace build_service::util;

FileReaderBase* FileReaderBase::createFileReader(
        const string& fileName, uint32_t bufferSize)
{
    FileReaderBase* fReader = NULL;
    FileType fileType = getFileType(fileName);
    if(fileType == GZIP_TYPE) {
        fReader = new(nothrow) GZipFileReader(bufferSize);
    } else {
        fReader = new(nothrow) FileReader;
    }

    return fReader;
}

FileReaderBase::FileType FileReaderBase::getFileType(const string& fileName) {
    string folder, file;
    FileUtil::splitFileName(fileName, folder, file);

    vector<string> exts = StringUtil::split(file, ".");
    if(exts.size() && (*exts.rbegin() == "gz")) {
        // this is a gzip file
        return GZIP_TYPE;
    } else {
        return FILE_TYPE;
    }
}



}
}
