/*
 * Copyright 2014-present Alibaba Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
#include "build_service/reader/MultiFileDocumentReader.h"

#include "autil/TimeUtility.h"
#include "build_service/reader/CipherFormatFileDocumentReader.h"
#include "build_service/reader/FixLenBinaryFileDocumentReader.h"
#include "build_service/reader/FormatFileDocumentReader.h"
#include "build_service/reader/VarLenBinaryFileDocumentReader.h"
#include "build_service/util/Monitor.h"
#include "fslib/util/FileUtil.h"

using namespace std;
using namespace autil;
using namespace fslib::util;
namespace build_service { namespace reader {
BS_LOG_SETUP(builder, MultiFileDocumentReader);

const int32_t MultiFileDocumentReader::FILE_INDEX_LOCATOR_BIT_COUNT = 23;
const uint64_t MultiFileDocumentReader::MAX_LOCATOR_FILE_INDEX = (1LL << FILE_INDEX_LOCATOR_BIT_COUNT) - 1;
const int32_t MultiFileDocumentReader::OFFSET_LOCATOR_BIT_COUNT = 40;
const uint64_t MultiFileDocumentReader::MAX_LOCATOR_OFFSET = (1LL << OFFSET_LOCATOR_BIT_COUNT) - 1;

MultiFileDocumentReader::MultiFileDocumentReader(const CollectResults& fileList,
                                                 const indexlib::util::MetricProviderPtr& metricProvider)
    : _fileCursor(-1)
    , _fileList(fileList)
    , _processedFileSize(0)
    , _totalFileSize(-1)
    , _progressMetric(DECLARE_METRIC(metricProvider, "basic/fileProgress", kmonitor::STATUS, "%"))
{
}

bool MultiFileDocumentReader::innerInit()
{
    _fileCursor = -1;
    _processedFileSize = 0;
    _totalFileSize = -1;
    return calculateTotalFileSize();
}

bool MultiFileDocumentReader::initForFormatDocument(const string& docPrefix, const string& docSuffix)
{
    _fileDocumentReaderPtr.reset(new FormatFileDocumentReader(docPrefix, docSuffix));
    BS_LOG(INFO, "init format document reader!");
    return innerInit();
}

bool MultiFileDocumentReader::initForCipherFormatDocument(autil::cipher::CipherOption option, const string& docPrefix,
                                                          const string& docSuffix)
{
    _fileDocumentReaderPtr.reset(new CipherFormatFileDocumentReader(option, docPrefix, docSuffix));
    BS_LOG(INFO, "init cipher format document reader!");
    return innerInit();
}

bool MultiFileDocumentReader::initForFixLenBinaryDocument(size_t fixLen)
{
    if (fixLen == 0) {
        BS_LOG(ERROR, "unsupported length [%lu] for fix length binary document!", fixLen);
        return false;
    }

    _fileDocumentReaderPtr.reset(new FixLenBinaryFileDocumentReader(fixLen));
    BS_LOG(INFO, "init binary document reader!");
    return innerInit();
}

bool MultiFileDocumentReader::initForVarLenBinaryDocument()
{
    _fileDocumentReaderPtr.reset(new VarLenBinaryFileDocumentReader);
    BS_LOG(INFO, "init binary document reader!");
    return innerInit();
}

bool MultiFileDocumentReader::seek(int64_t locator)
{
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
    const string& fileName = _fileList[_fileCursor]._fileName;
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

bool MultiFileDocumentReader::skipToFileId(int64_t fileId)
{
    _processedFileSize = 0;
    for (size_t i = 0; i < _fileList.size(); i++) {
        if (_fileList[i]._globalId == fileId) {
            _fileCursor = (int32_t)i;
            return true;
        }
        if (_progressMetric) {
            int64_t fileLength = 0;
            if (!FileUtil::getFileLength(_fileList[i]._fileName, fileLength)) {
                BS_LOG(INFO, "get file length for file [%s] failed", _fileList[i]._fileName.c_str());
                return false;
            }
            _processedFileSize += fileLength;
        }
    }
    _fileCursor = -1;
    return false;
}

bool MultiFileDocumentReader::read(string& docStr, int64_t& locator)
{
    while (true) {
        if (_fileDocumentReaderPtr->isEof() && !skipToNextFile()) {
            return false;
        }
        if (_fileDocumentReaderPtr->read(docStr)) {
            appendLocator(locator);
            REPORT_METRIC(_progressMetric, getProcessProgress());
            return true;
        }
        // read return false and not eof, means read error.
        if (!_fileDocumentReaderPtr->isEof()) {
            return false;
        }
    }
    return true;
}

bool MultiFileDocumentReader::skipToNextFile()
{
    if (_fileCursor + 1 >= (int32_t)_fileList.size()) {
        BS_LOG(INFO, "_fileCursor[%d] > _fileList.size()[%u]", _fileCursor, (uint32_t)_fileList.size());
        BS_LOG(INFO, "accumulate processed file size [%lu], readDocCount [%lu].",
               _processedFileSize + _fileDocumentReaderPtr->getFileSize(), _fileDocumentReaderPtr->getReadDocCount());
        return false;
    }

    string fileName = _fileList[_fileCursor + 1]._fileName;
    BS_LOG(INFO, "init reader with fileName[%s], cursor [%d]", fileName.c_str(), _fileCursor + 1);
    _processedFileSize += _fileDocumentReaderPtr->getFileSize();
    BS_LOG(INFO, "accumulate processed file size [%lu], readDocCount [%lu].", _processedFileSize,
           _fileDocumentReaderPtr->getReadDocCount());
    if (!_fileDocumentReaderPtr->init(fileName, 0)) {
        string errorMsg = "failed to init filedocumentreader[" + fileName + "]";
        BS_LOG(ERROR, "%s", errorMsg.c_str());
        return false;
    }
    _fileCursor++;
    return true;
}

bool MultiFileDocumentReader::isEof()
{
    assert(_fileDocumentReaderPtr);
    return (_fileCursor + 1 >= (int32_t)_fileList.size()) && (_fileDocumentReaderPtr->isEof());
}

void MultiFileDocumentReader::appendLocator(int64_t& locator)
{
    assert(_fileDocumentReaderPtr);
    int64_t offset = _fileDocumentReaderPtr->getFileOffset();
    transFileOffsetToLocator(_fileList[_fileCursor]._globalId, offset, locator);
}

void MultiFileDocumentReader::transFileOffsetToLocator(int64_t fileIndex, int64_t offset, int64_t& locator)
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

void MultiFileDocumentReader::transLocatorToFileOffset(int64_t locator, int64_t& fileIndex, int64_t& offset)
{
    offset = locator & MAX_LOCATOR_OFFSET;
    fileIndex = locator >> OFFSET_LOCATOR_BIT_COUNT;
}

double MultiFileDocumentReader::getProcessProgress() const
{
    return 1.0 * (_processedFileSize + _fileDocumentReaderPtr->getReadBytes()) / _totalFileSize;
}

bool MultiFileDocumentReader::calculateTotalFileSize()
{
    int64_t totalSize = 0;
    for (size_t i = 0; i < _fileList.size(); i++) {
        int64_t fileLength = 0;
        if (!fslib::util::FileUtil::getFileLength(_fileList[i]._fileName, fileLength)) {
            return false;
        }
        totalSize += fileLength;
    }
    _totalFileSize = totalSize;
    return true;
}

}} // namespace build_service::reader
