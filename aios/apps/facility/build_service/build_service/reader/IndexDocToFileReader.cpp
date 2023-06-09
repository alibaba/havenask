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
#include "build_service/reader/IndexDocToFileReader.h"

#include "autil/TimeUtility.h"
#include "build_service/config/TaskOutputConfig.h"
#include "build_service/io/FileOutput.h"
#include "build_service/reader/DummyDocParser.h"
#include "build_service/reader/IndexDocReader.h"
#include "build_service/util/Monitor.h"
#include "indexlib/file_system/fslib/FslibWrapper.h"

using namespace std;

namespace build_service { namespace reader {
BS_LOG_SETUP(reader, IndexDocToFileReader);

IndexDocToFileReader::IndexDocToFileReader() {}

IndexDocToFileReader::~IndexDocToFileReader() {}

bool IndexDocToFileReader::init(const ReaderInitParam& params)
{
    _params = params;
    _docReader = make_unique<IndexDocReader>();
    if (!_docReader->init(params)) {
        BS_LOG(ERROR, "indexDocReader init failed");
        return false;
    }

    _outputPath = getValueFromKeyValueMap(params.kvMap, "output_path");
    if (_outputPath.empty()) {
        BS_LOG(ERROR, "indexDocToFileReader output_path cannot be empty");
        return false;
    }

    _syncInterval = autil::EnvUtil::getEnv<uint64_t>(
        /*key*/ "BS_READER_DUMP_TO_FILE_INTERVAL", /*defaultValue*/ DEFAULT_SYNC_INTERVAL);

    _maxFileLength = autil::EnvUtil::getEnv<size_t>(/*key*/ "BS_READER_DUMP_MAX_FILE_LENGTH",
                                                    /*defaultValue*/ DEFAULT_MAX_FILE_LENGTH);

    _lastCommitTime = -1;
    if (!initOutput()) {
        return false;
    }

    auto checkDoc = getValueFromKeyValueMap(params.kvMap, "check_doc");
    if (checkDoc == "true") {
        _docCheck = true;
        _pkField = getValueFromKeyValueMap(params.kvMap, "pk_field");
        if (_pkField.empty()) {
            BS_LOG(ERROR, "check doc not set pk field name");
            return false;
        }
    }

    _writeLatencyMetric = DECLARE_METRIC(params.metricProvider, "perf/writeRawDocLatency", kmonitor::GAUGE, "ms");
    BS_LOG(INFO,
           "indexDocToFileReader init success, output file is [%s:%s], sync interval [%ld], max file length is [%ld]",
           _outputPath.c_str(), _currentFileName.c_str(), _syncInterval, _maxFileLength);
    return true;
}

bool IndexDocToFileReader::initOutput()
{
    int64_t now = autil::TimeUtility::currentTimeInSeconds();
    config::TaskOutputConfig outputConfig;
    _output.reset(new io::FileOutput(outputConfig));

    _currentFileName = OUTPUT_FILE_NAME_PREIFX + std::to_string(_params.range.from()) + "_" +
                       std::to_string(_params.range.to()) + "." + std::to_string(now) + TMP_SUFFIX;
    _fileNameLen.resize(sizeof(uint8_t));
    *((uint8_t*)_fileNameLen.data()) = _currentFileName.length();

    std::string outputFilePath = indexlib::file_system::FslibWrapper::JoinPath(_outputPath, _currentFileName);
    _params.kvMap["file_path"] = outputFilePath;

    if (!_output->init(_params.kvMap)) {
        BS_LOG(ERROR, " init writer failed");
        return false;
    }
    BS_LOG(INFO, "output file is [%s]", outputFilePath.c_str());
    return true;
}

bool IndexDocToFileReader::writerRecover(std::string& inheritFileName, size_t& fileLength)
{
    if (!autil::StringUtil::endsWith(inheritFileName, TMP_SUFFIX)) {
        BS_LOG(INFO, "checkpoint file [%s] is not tmp file, no need to clone", inheritFileName.c_str());
        return true;
    }

    std::string inheritFilePath = indexlib::file_system::FslibWrapper::JoinPath(_outputPath, inheritFileName);
    bool exist = false;
    auto status = indexlib::file_system::FslibWrapper::IsExist(inheritFilePath, exist).Status();
    if (!status.IsOK()) {
        BS_LOG(ERROR, "is exist file [%s] failed", inheritFilePath.c_str());
        return false;
    }

    if (exist) {
        return _output->cloneFrom(inheritFilePath, fileLength);
    }

    // check for final file exist
    autil::StringUtil::replaceLast(inheritFilePath, TMP_SUFFIX, "");
    exist = false;
    status = indexlib::file_system::FslibWrapper::IsExist(inheritFilePath, exist).Status();
    if (!status.IsOK()) {
        BS_LOG(ERROR, "is exist file [%s] failed", inheritFilePath.c_str());
        return false;
    }
    if (!exist) {
        BS_LOG(ERROR, "cannot find checkpoint file [%s] and it tmp file", inheritFilePath.c_str());
        return false;
    }
    return true;
}

bool IndexDocToFileReader::seek(const Checkpoint& checkpoint)
{
    std::string fileName;
    size_t fileLength = 0;
    Checkpoint readerCkpt;
    if (!resolveCheckpoint(checkpoint, fileName, fileLength, readerCkpt)) {
        BS_LOG(ERROR, "resolve checkpoint failed [%s]", checkpoint.userData.c_str());
        return false;
    }

    if (!writerRecover(fileName, fileLength)) {
        BS_LOG(ERROR, "writer recover from source file [%s], length [%ld] failed", fileName.c_str(), fileLength);
        return false;
    }

    if (!_docReader->seek(readerCkpt)) {
        BS_LOG(ERROR, "index doc reader seek failed");
        return false;
    }
    BS_LOG(INFO, "seek ckeckpoint success");
    return true;
}

bool IndexDocToFileReader::isEof() const { return _docReader->isEof(); }

void IndexDocToFileReader::waitAllDocCommit()
{
    while (!_output->commit()) {
        usleep(50 * 1000);
    }
}

std::string IndexDocToFileReader::serializeRawDoc(document::RawDocument& rawDoc)
{
    if (_docCheck) {
        return serializeDocCheckStr(rawDoc);
    }
    return serializeHa3RawDoc(rawDoc);
}

std::string IndexDocToFileReader::serializeDocCheckStr(document::RawDocument& rawDoc)
{
    std::string rawDocStr = rawDoc.toString();
    uint64_t docHash = autil::HashAlgorithm::hashString64(rawDocStr.c_str(), rawDocStr.length());
    auto fieldValue = rawDoc.getField(_pkField);
    stringstream docCheckStr;
    docCheckStr << fieldValue << " : " << docHash << std::endl;
    return docCheckStr.str();
}

std::string IndexDocToFileReader::serializeHa3RawDoc(document::RawDocument& rawDoc)
{
    indexlib::document::RawDocFieldIteratorPtr iter(rawDoc.CreateIterator());
    stringstream rawDocStr;
    rawDocStr << document::CMD_TAG << document::KEY_VALUE_EQUAL_SYMBOL << document::CMD_ADD
              << document::KEY_VALUE_SEPARATOR;
    while (iter->IsValid()) {
        rawDocStr << iter->GetFieldName().to_string();
        rawDocStr << document::KEY_VALUE_EQUAL_SYMBOL;
        rawDocStr << iter->GetFieldValue().to_string();
        rawDocStr << document::KEY_VALUE_SEPARATOR;
        iter->MoveToNext();
    }
    rawDocStr << document::CMD_SEPARATOR;
    return rawDocStr.str();
}

indexlib::document::RawDocumentParser* IndexDocToFileReader::createRawDocumentParser(const ReaderInitParam& params)
{
    return new DummyDocParser();
}

void IndexDocToFileReader::setAndSaveCheckpoint(size_t currentOutputFileLen, Checkpoint* checkpoint)
{
    // file len(size_t) + fileNameLen (uint8_t) + filename
    std::string currentWriterLen(sizeof(size_t), ' ');
    *((size_t*)currentWriterLen.data()) = currentOutputFileLen;
    checkpoint->userData = currentWriterLen + _fileNameLen + _currentFileName + checkpoint->userData;
    _checkpoint = *checkpoint;
}

bool IndexDocToFileReader::resolveCheckpoint(const Checkpoint& checkpoint, std::string& fileName, size_t& fileLength,
                                             Checkpoint& readerCkpt)
{
    auto checkpointData = checkpoint.userData;
    if (checkpointData.size() < sizeof(size_t)) {
        return false;
    }
    std::string fileLenStr = checkpointData.substr(0, sizeof(size_t));
    fileLength = *((size_t*)fileLenStr.data());
    std::string otherPart = checkpointData.substr(sizeof(size_t));
    if (otherPart.size() < sizeof(uint8_t)) {
        return false;
    }
    uint8_t fileNameLen = *((uint8_t*)(otherPart.substr(0, sizeof(uint8_t)).data()));
    fileName = otherPart.substr(sizeof(uint8_t), fileNameLen);
    readerCkpt.offset = checkpoint.offset;
    readerCkpt.userData = otherPart.substr(sizeof(uint8_t) + fileNameLen);
    return true;
}

bool IndexDocToFileReader::finalMove(size_t outputFileLen, Checkpoint* checkpoint)
{
    std::string lastFileName = _currentFileName;
    autil::StringUtil::replaceLast(_currentFileName, TMP_SUFFIX, "");
    *((uint8_t*)_fileNameLen.data()) = _currentFileName.length();

    std::string srcFilePath = indexlib::file_system::FslibWrapper::JoinPath(_outputPath, lastFileName);
    std::string destFilePath = indexlib::file_system::FslibWrapper::JoinPath(_outputPath, _currentFileName);

    bool exist = false;
    auto status = indexlib::file_system::FslibWrapper::IsExist(destFilePath, exist).Status();
    if (!status.IsOK()) {
        BS_LOG(ERROR, "is exist file [%s] failed", destFilePath.c_str());
        return false;
    }
    if (!exist) {
        auto status = indexlib::file_system::FslibWrapper::Rename(srcFilePath, destFilePath).Status();
        if (!status.IsOK()) {
            BS_LOG(ERROR, "rename file from [%s] to [%s] failed", srcFilePath.c_str(), destFilePath.c_str());
            return false;
        }
    }
    setAndSaveCheckpoint(outputFileLen, checkpoint);
    BS_LOG(INFO, "final move file [%s] to [%s]", srcFilePath.c_str(), destFilePath.c_str());
    return true;
}

RawDocumentReader::ErrorCode IndexDocToFileReader::getNextRawDoc(document::RawDocument& rawDoc, Checkpoint* checkpoint,
                                                                 DocInfo& docInfo)
{
    bool needClose = false;
    RawDocumentReader::ErrorCode ec = RawDocumentReader::ERROR_NONE;
    ec = _docReader->getNextRawDoc(rawDoc, checkpoint, docInfo);
    if (ec == RawDocumentReader::ERROR_EOF) {
        BS_LOG(INFO, "reader read eof");
        needClose = true;
    } else if (ec != RawDocumentReader::ERROR_NONE) {
        return ec;
    }

    size_t len = _output->getLength();
    if (len > _maxFileLength) {
        needClose = true;
    }

    if (needClose) {
        waitAllDocCommit();
        _output->close();
        if (!finalMove(len, checkpoint)) {
            return RawDocumentReader::ERROR_EXCEPTION;
        }
        if (ec == RawDocumentReader::ERROR_EOF) {
            return ec;
        }
        if (!initOutput()) {
            return RawDocumentReader::ERROR_EXCEPTION;
        }
    }

    {
        indexlib::util::ScopeLatencyReporter reporter(_writeLatencyMetric.get());
        std::string rawDocStr = serializeRawDoc(rawDoc);
        autil::legacy::Any rawDocAny = autil::StringView(rawDocStr);
        if (!_output->write(rawDocAny)) {
            BS_LOG(ERROR, "raw doc output failed");
            return RawDocumentReader::ERROR_EXCEPTION;
        }

        int64_t currentTime = autil::TimeUtility::currentTimeInSeconds();
        if ((_lastCommitTime == -1 || currentTime - _lastCommitTime >= _syncInterval) && !needClose) {
            waitAllDocCommit();
            size_t len = _output->getLength();
            _lastCommitTime = currentTime;
            setAndSaveCheckpoint(len, checkpoint);
        } else {
            rawDoc.setDocOperateType(SKIP_DOC);
            *checkpoint = _checkpoint;
        }
    }
    return ec;
}

std::string IndexDocToFileReader::TEST_getOutputFileName() const { return _currentFileName; }

}} // namespace build_service::reader
