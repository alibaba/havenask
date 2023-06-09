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
#include "build_service/reader/FileRawDocumentReader.h"

#include "autil/HashAlgorithm.h"
#include "autil/StringUtil.h"
#include "build_service/config/CLIOptionNames.h"
#include "build_service/proto/Heartbeat.pb.h"
#include "build_service/proto/ParserConfig.h"
#include "build_service/reader/DocumentSeparators.h"
#include "build_service/reader/FileListCollector.h"
#include "build_service/reader/MultiFileDocumentReader.h"
#include "build_service/reader/ParserCreator.h"
#include "build_service/util/Monitor.h"
#include "fslib/fslib.h"
#include "fslib/util/PathUtil.h"
#include "indexlib/util/counter/StateCounter.h"

using namespace std;
using namespace autil;
using namespace fslib;
using namespace build_service::document;
using namespace build_service::config;
using namespace build_service::proto;
using namespace build_service::common;

namespace build_service { namespace reader {

BS_LOG_SETUP(reader, FileRawDocumentReader);

FileRawDocumentReader::FileRawDocumentReader() : _documentReader(NULL), _lastOffset(0) {}

FileRawDocumentReader::~FileRawDocumentReader() { DELETE_AND_SET_NULL(_documentReader); }

bool FileRawDocumentReader::init(const ReaderInitParam& params)
{
    if (!RawDocumentReader::init(params)) {
        return false;
    }

    CollectResults fileList;
    if (!FileListCollector::collect(params.kvMap, params.range, fileList)) {
        string errorMsg =
            "Init file reader failed : collect fileList failed, kvMap is: " + keyValueMapToString(params.kvMap);
        REPORT_ERROR_WITH_ADVICE(SERVICE_ERROR_CONFIG, errorMsg, BS_STOP);
        return false;
    }

    if (!rewriteFilePathForDcacheIfNeeded(params.kvMap, fileList)) {
        string errorMsg = "Init file reader failed : rewrite file path for dcache failed, kvMap is: " +
                          keyValueMapToString(params.kvMap);
        REPORT_ERROR_WITH_ADVICE(SERVICE_ERROR_CONFIG, errorMsg, BS_STOP);
        return false;
    }

    _documentReader = createMultiFileDocumentReader(fileList, params.kvMap, params.metricProvider);
    if (!_documentReader) {
        return false;
    }
    return true;
}

bool FileRawDocumentReader::rewriteFilePathForDcacheIfNeeded(const KeyValueMap& kv, CollectResults& fileList)
{
    auto it = kv.find(READ_THROUGH_DCACHE);
    if (kv.end() == it || 0 != strcasecmp(it->second.c_str(), "true")) {
        BS_LOG(INFO, "file path need not rewrite for dcache");
        return true;
    }
    BS_LOG(INFO, "file path need rewrite for dcache");
    CollectResults newFileList;
    for (CollectResult file : fileList) {
        string newFileName;
        if (!fslib::util::PathUtil::rewriteToDcachePath(file._fileName, newFileName)) {
            BS_LOG(ERROR, "file path [%s] rewrite to dcache path failed", file._fileName.c_str());
            return false;
        }
        CollectResult newFile = file;
        newFile._fileName = newFileName;
        newFileList.push_back(newFile);
    }
    fileList.swap(newFileList);
    return true;
}

MultiFileDocumentReader*
FileRawDocumentReader::createMultiFileDocumentReader(const CollectResults& collectResults, const KeyValueMap& kvMap,
                                                     const indexlib::util::MetricProviderPtr& metricProvider)
{
    assert(_documentFactoryWrapper);
    ParserCreator parserCreator(_documentFactoryWrapper);
    vector<ParserConfig> parserConfigs;
    if (!parserCreator.createParserConfigs(kvMap, parserConfigs)) {
        string errorMsg = "create parserConfigs failed";
        REPORT_ERROR_WITH_ADVICE(SERVICE_ERROR_CONFIG, errorMsg, BS_STOP);
        return NULL;
    }

    if (parserConfigs.size() != 1) {
        string errorMsg = "file source supports only one ParserConfig,"
                          "while parserConfig size = " +
                          StringUtil::toString(parserConfigs.size());
        REPORT_ERROR_WITH_ADVICE(SERVICE_ERROR_CONFIG, errorMsg, BS_STOP);
        return NULL;
    }

    string rawDocPrefix;
    string rawDocSuffix;
    string format = parserConfigs[0].type;
    if (format == RAW_DOCUMENT_HA3_DOCUMENT_FORMAT) {
        rawDocPrefix = RAW_DOCUMENT_HA3_SEP_PREFIX;
        rawDocSuffix = RAW_DOCUMENT_HA3_SEP_SUFFIX;
    } else if (format == RAW_DOCUMENT_ISEARCH_DOCUMENT_FORMAT) {
        rawDocPrefix = RAW_DOCUMENT_ISEARCH_SEP_PREFIX;
        rawDocSuffix = RAW_DOCUMENT_ISEARCH_SEP_SUFFIX;
    } else {
        rawDocPrefix = getValueFromKeyValueMap(kvMap, RAW_DOCUMENT_SEP_PREFIX, RAW_DOCUMENT_HA3_SEP_PREFIX);
        rawDocSuffix = getValueFromKeyValueMap(kvMap, RAW_DOCUMENT_SEP_SUFFIX, RAW_DOCUMENT_HA3_SEP_SUFFIX);
    }

    if (rawDocSuffix.empty()) {
        string errorMsg = "separator_suffix can not be empty!";
        REPORT_ERROR_WITH_ADVICE(SERVICE_ERROR_CONFIG, errorMsg, BS_STOP);
        return NULL;
    }

    std::unique_ptr<MultiFileDocumentReader> reader(new MultiFileDocumentReader(collectResults, metricProvider));
    autil::cipher::CipherOption option(autil::cipher::CT_UNKNOWN);
    bool hasError = false;
    if (extractCipherOption(kvMap, option, hasError)) {
        if (!reader->initForCipherFormatDocument(option, rawDocPrefix, rawDocSuffix)) {
            string errorMsg = "init multi file document reader for cipher failed";
            REPORT_ERROR_WITH_ADVICE(SERVICE_ERROR_CONFIG, errorMsg, BS_STOP);
            return NULL;
        }
        return reader.release();
    } else if (hasError) {
        string errorMsg = "init multi file document reader for cipher failed";
        REPORT_ERROR_WITH_ADVICE(SERVICE_ERROR_CONFIG, errorMsg, BS_STOP);
        return NULL;
    } else if (!reader->initForFormatDocument(rawDocPrefix, rawDocSuffix)) {
        string errorMsg = "init multi file document reader failed";
        REPORT_ERROR_WITH_ADVICE(SERVICE_ERROR_CONFIG, errorMsg, BS_STOP);
        return NULL;
    }
    return reader.release();
}

bool FileRawDocumentReader::seek(const Checkpoint& checkpoint) { return _documentReader->seek(checkpoint.offset); }

RawDocumentReader::ErrorCode FileRawDocumentReader::readDocStr(string& docStr, Checkpoint* checkpoint, DocInfo& docInfo)
{
    bool ret = _documentReader->read(docStr, checkpoint->offset);
    if (!ret) {
        return _documentReader->isEof() ? ERROR_SEALED : ERROR_EXCEPTION;
    }
    _lastOffset = checkpoint->offset;
    return ERROR_NONE;
}

bool FileRawDocumentReader::isEof() const { return _documentReader->isEof(); }

void FileRawDocumentReader::doFillDocTags(document::RawDocument& rawDoc)
{
    int64_t fileIndex;
    int64_t offset;
    MultiFileDocumentReader::transLocatorToFileOffset(_lastOffset, fileIndex, offset);
    rawDoc.AddTag("fileIndex", StringUtil::toString(fileIndex));
    rawDoc.AddTag("fileOffset", StringUtil::toString(offset));
}

bool FileRawDocumentReader::extractCipherOption(const KeyValueMap& kvMap, autil::cipher::CipherOption& option,
                                                bool& hasError) const
{
    hasError = false;
    std::string cipherParamStr = getValueFromKeyValueMap(kvMap, CIPHER_FILE_READER_PARAM);
    if (cipherParamStr.empty()) {
        return false;
    }
    std::string useBase64Str = getValueFromKeyValueMap(kvMap, CIPHER_PARAMETER_ENABLE_BASE64);
    autil::StringUtil::toLowerCase(useBase64Str);

    bool enableBase64 = (useBase64Str == "true" || useBase64Str == "yes");
    BS_LOG(INFO, "cipher parameter: [%s], useBase64 [%s]", cipherParamStr.c_str(),
           autil::StringUtil::toString(enableBase64).c_str());
    if (!option.fromKVString(cipherParamStr, enableBase64)) {
        hasError = true;
        return false;
    }
    return true;
}

}} // namespace build_service::reader
