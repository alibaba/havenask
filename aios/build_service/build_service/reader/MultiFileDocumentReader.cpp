#include "build_service/reader/MultiFileDocumentReader.h"
#include "build_service/reader/FixLenBinaryFileDocumentReader.h"
#include "build_service/reader/VarLenBinaryFileDocumentReader.h"
#include "build_service/reader/FormatFileDocumentReader.h"
#include "build_service/util/FileUtil.h"
#include <autil/TimeUtility.h>

using namespace std;
using namespace autil;
using namespace build_service::util;

namespace build_service {
namespace reader {
BS_LOG_SETUP(builder, MultiFileDocumentReader);

const int32_t MultiFileDocumentReader::FILE_INDEX_LOCATOR_BIT_COUNT = 23;
const uint64_t MultiFileDocumentReader::MAX_LOCATOR_FILE_INDEX = 
    (1LL << FILE_INDEX_LOCATOR_BIT_COUNT) - 1;
const int32_t MultiFileDocumentReader::OFFSET_LOCATOR_BIT_COUNT = 40;
const uint64_t MultiFileDocumentReader::MAX_LOCATOR_OFFSET = 
            (1LL << OFFSET_LOCATOR_BIT_COUNT) - 1;

MultiFileDocumentReader::MultiFileDocumentReader(
        const CollectResults &fileList)
    : _fileCursor(-1)
    , _fileList(fileList)
    , _processedFileSize(0)
    , _totalFileSize(-1)
{}

bool MultiFileDocumentReader::initForFormatDocument(const string &docPrefix, const string &docSuffix)
{
    _fileDocumentReaderPtr.reset(new FormatFileDocumentReader(docPrefix, docSuffix));
    
    _fileCursor = -1;
    _processedFileSize = 0;
    _totalFileSize = -1;
    _startTime = TimeUtility::currentTime();
    BS_LOG(INFO, "init format document reader!");
    return true;
}

bool MultiFileDocumentReader::initForFixLenBinaryDocument(size_t fixLen)
{
    if (fixLen == 0)
    {
        BS_LOG(ERROR, "unsupported length [%lu] for fix length binary document!", fixLen);
        return false;
    }

    _fileDocumentReaderPtr.reset(new FixLenBinaryFileDocumentReader(fixLen));
    _fileCursor = -1;
    _processedFileSize = 0;
    _totalFileSize = -1;
    _startTime = TimeUtility::currentTime();
    BS_LOG(INFO, "init binary document reader!");
    return true;
}

bool MultiFileDocumentReader::initForVarLenBinaryDocument()
{
    _fileDocumentReaderPtr.reset(new VarLenBinaryFileDocumentReader);
    _fileCursor = -1;
    _processedFileSize = 0;
    _totalFileSize = -1;
    _startTime = TimeUtility::currentTime();
    BS_LOG(INFO, "init binary document reader!");
    return true;
}

bool MultiFileDocumentReader::seek(int64_t locator) {
    int64_t offset;
    int64_t fileId;
    transLocatorToFileOffset(locator, fileId, offset);
    assert(fileId >= 0);
    if (!skipToFileId(fileId)) {
        stringstream ss;
        ss << "fileId[" << fileId << "] is not in fileList";
        string errorMsg = ss.str();
        BS_LOG(ERROR, "%s", errorMsg.c_str());
        return false;
    }
    if (_fileCursor < 0 || _fileCursor >= (int32_t)_fileList.size()) {
        stringstream ss;
        ss << "invalid fileCursor[" << _fileCursor << "]";
        string errorMsg = ss.str();
        BS_LOG(ERROR, "%s", errorMsg.c_str());
        return false;
    }
    const string &fileName = _fileList[_fileCursor]._fileName;
    if (!_fileDocumentReaderPtr->init(fileName, offset)) {
        stringstream ss;
        ss << "failed to init filedocumentreader[" << fileName << "], offset[" << offset << "]";
        string errorMsg = ss.str();
        BS_LOG(ERROR, "%s", errorMsg.c_str());
        return false;
    }
    BS_LOG(INFO, "seek to file[%s], offset[%lu]", fileName.c_str(), offset);
    return true;
}

bool MultiFileDocumentReader::skipToFileId(int64_t fileId) {
    for (size_t i = 0; i < _fileList.size(); i++) {
        if (_fileList[i]._globalId == fileId) {
            _fileCursor = (int32_t)i;
            return true;
        }
    }
    _fileCursor = -1;
    return false;
}

bool MultiFileDocumentReader::read(string &docStr, int64_t &locator) {
    while (true) {
        if (_fileDocumentReaderPtr->isEof() && !skipToNextFile()) {
            return false;
        }
        if (_fileDocumentReaderPtr->read(docStr)) {
            appendLocator(locator);
            return true;
        }
        // read return false and not eof, means read error.
        if (!_fileDocumentReaderPtr->isEof()) {
            return false;
        }
    }
    return true;
}

bool MultiFileDocumentReader::skipToNextFile() {
    if (_fileCursor + 1 >= (int32_t)_fileList.size()) {
        BS_LOG(INFO, "_fileCursor[%d] > _fileList.size()[%u]",
               _fileCursor, (uint32_t)_fileList.size());
        return false;
    }
    string fileName = _fileList[_fileCursor + 1]._fileName;
    BS_LOG(INFO, "init reader with fileName[%s]", fileName.c_str());

    _processedFileSize += _fileDocumentReaderPtr->getFileSize();

    if (!_fileDocumentReaderPtr->init(fileName, 0)) {
        string errorMsg = "failed to init filedocumentreader[" + fileName + "]";
        BS_LOG(ERROR, "%s", errorMsg.c_str());
        return false;
    }
    _fileCursor++;
    return true;
}

bool MultiFileDocumentReader::isEof() {
    assert(_fileDocumentReaderPtr);
    return (_fileCursor + 1 >= (int32_t)_fileList.size())
        && (_fileDocumentReaderPtr->isEof());
}

void MultiFileDocumentReader::appendLocator(int64_t &locator) {
    assert(_fileDocumentReaderPtr);
    int64_t offset = _fileDocumentReaderPtr->getFileOffset();
    transFileOffsetToLocator(_fileList[_fileCursor]._globalId, offset, locator);
}

void MultiFileDocumentReader::transFileOffsetToLocator(
        int64_t fileIndex, int64_t offset, int64_t &locator)
{
    // file index overflow. do not recover from current file offset, set offset=0 for now.
    uint64_t fileIndexLocator;
    uint64_t offsetLocator;
    if ((uint64_t)fileIndex > MAX_LOCATOR_FILE_INDEX) {
        fileIndexLocator = MAX_LOCATOR_FILE_INDEX;
        offsetLocator = 0;
    } else {
        fileIndexLocator = fileIndex;
        offsetLocator = min(MAX_LOCATOR_OFFSET, (uint64_t)offset);
    }
    locator = ((uint64_t(fileIndexLocator) << OFFSET_LOCATOR_BIT_COUNT) | offsetLocator);
}

void MultiFileDocumentReader::transLocatorToFileOffset(
        int64_t locator, int64_t &fileIndex, int64_t &offset)
{
    offset = locator & MAX_LOCATOR_OFFSET;
    fileIndex = locator >> OFFSET_LOCATOR_BIT_COUNT;
}

int64_t MultiFileDocumentReader::estimateLeftTime() {
    int64_t now = TimeUtility::currentTime();
    int64_t usedTime = now - _startTime;


    if (_totalFileSize == -1 && !calculateTotalFileSize()) {
        return numeric_limits<int64_t>::max(); // error
    }
    
    int64_t processedSize = _processedFileSize + _fileDocumentReaderPtr->getReadBytes();
    int64_t leftSize = _totalFileSize - processedSize;
  
    if (leftSize == 0) {
        return 0;
    }

    int64_t freshness = double(usedTime) * leftSize / processedSize;
 

    return std::max(freshness, 1L);
}

bool MultiFileDocumentReader::calculateTotalFileSize() {
    int64_t totalSize = 0;
    for (size_t i = 0; i < _fileList.size(); i++) {
        int64_t fileLength = 0;
        if (!FileUtil::getFileLength(_fileList[i]._fileName, fileLength)) {
            return false;
        }
        totalSize += fileLength;
    }
    _totalFileSize = totalSize;
    return true;
}

}
}
