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
#include "build_service/reader/MultiIndexDocReader.h"

#include "autil/StringUtil.h"
#include "build_service/config/CLIOptionNames.h"
#include "build_service/reader/DummyDocParser.h"
#include "build_service/util/Monitor.h"
#include "build_service/util/RangeUtil.h"

using namespace std;
using namespace build_service::config;
using namespace build_service::util;
using namespace build_service::proto;
using namespace autil;
using namespace autil::legacy;

namespace build_service { namespace reader {
BS_LOG_SETUP(reader, MultiIndexDocReader);

const int32_t MultiIndexDocReader::OFFSET_LOCATOR_BIT_COUNT = 33;

MultiIndexDocReader::MultiIndexDocReader() : _currentItemIdx(-1), _nextDocId(-1) {}

MultiIndexDocReader::~MultiIndexDocReader() {}

bool MultiIndexDocReader::init(const ReaderInitParam& params)
{
    string indexRoot = getValueFromKeyValueMap(params.kvMap, READ_INDEX_ROOT);
    if (indexRoot.empty()) {
        BS_LOG(ERROR, "index reader's indexRoot cannot be empty");
        return false;
    }
    vector<pair<pair<uint32_t, uint32_t>, string>> rangePath;
    if (!RangeUtil::getPartitionRange(indexRoot, rangePath)) {
        BS_LOG(ERROR, "failed to get partition info from [%s]", indexRoot.c_str());
        return false;
    }

    string versionStr = getValueFromKeyValueMap(params.kvMap, READ_INDEX_VERSION);
    versionid_t versionId = indexlib::INVALID_VERSIONID;
    if (versionStr.empty() || !StringUtil::fromString(versionStr, versionId) || versionId < 0) {
        BS_LOG(ERROR, "index reader's version[%s] is invalid", versionStr.c_str());
        return false;
    }

    bool preferSourceIndex = true;
    string preferSourceIndexStr = getValueFromKeyValueMap(params.kvMap, PREFER_SOURCE_INDEX);
    if (!preferSourceIndexStr.empty() && !StringUtil::parseTrueFalse(preferSourceIndexStr, preferSourceIndex)) {
        BS_LOG(ERROR, "index reader's preferSourceIndex[%s] is invalid", preferSourceIndexStr.c_str());
        return false;
    }

    bool ignoreError = false;
    string ignoreErrorStr = getValueFromKeyValueMap(params.kvMap, READ_IGNORE_ERROR);
    if (!ignoreErrorStr.empty() && !StringUtil::parseTrueFalse(ignoreErrorStr, ignoreError)) {
        BS_LOG(ERROR, "index reader's ignore [%s] is invalid", ignoreErrorStr.c_str());
        return false;
    }

    uint64_t blockCacheSizeInMB = 10;
    string blockCacheSizeInMBStr = getValueFromKeyValueMap(params.kvMap, READ_INDEX_CACHE_SIZE);
    if (!blockCacheSizeInMBStr.empty() && !StringUtil::fromString(blockCacheSizeInMBStr, blockCacheSizeInMB)) {
        BS_LOG(ERROR, "index reader's cache size [%s] is invalid", blockCacheSizeInMBStr.c_str());
        return false;
    }

    float cacheBlockSizeInMB = 0.5;
    string cacheBlockSizeInMBStr = getValueFromKeyValueMap(params.kvMap, READ_CACHE_BLOCK_SIZE);
    if (!cacheBlockSizeInMBStr.empty() && !StringUtil::fromString(cacheBlockSizeInMBStr, cacheBlockSizeInMB)) {
        BS_LOG(ERROR, "index reader's cache block size [%s] is invalid", cacheBlockSizeInMBStr.c_str());
        return false;
    }

    std::vector<IndexQueryCondition> userDefineIndexParam;
    string userDefineIndexParamStr = getValueFromKeyValueMap(params.kvMap, USER_DEFINE_INDEX_PARAM);
    if (!userDefineIndexParamStr.empty()) {
        try {
            FromJsonString(userDefineIndexParam, userDefineIndexParamStr);
        } catch (const ExceptionBase& e) {
            BS_LOG(ERROR, "parse user define index param [%s] failed", userDefineIndexParamStr.c_str());
            return false;
        }
    }
    BS_LOG(INFO, "required [%lu] conditions", userDefineIndexParam.size());

    string requiredFieldsStr = getValueFromKeyValueMap(params.kvMap, USER_REQUIRED_FIELDS);
    vector<string> requiredFields;
    if (!requiredFieldsStr.empty()) {
        try {
            FromJsonString(requiredFields, requiredFieldsStr);
        } catch (const ExceptionBase& e) {
            BS_LOG(ERROR, "parse required fields [%s] failed", requiredFieldsStr.c_str());
            return false;
        }
    }
    BS_LOG(INFO, "required [%lu] fields", requiredFields.size());

    for (const auto& range : rangePath) {
        uint32_t myFrom = params.range.from();
        uint32_t myTo = params.range.to();
        uint32_t left = max(myFrom, range.first.first);
        uint32_t right = min(myTo, range.first.second);
        if (left <= right) {
            _totalItems.emplace_back(range.second, versionId, preferSourceIndex, ignoreError, make_pair(left, right),
                                     range.first, blockCacheSizeInMB * 1024 * 1024, cacheBlockSizeInMB * 1024 * 1024,
                                     requiredFields, userDefineIndexParam);
        }
    }
    if (!switchPartition(0, 0)) {
        BS_LOG(ERROR, "init reader to offset [0, 0] failed");
        return false;
    }
    return true;
}

bool MultiIndexDocReader::switchPartition(int32_t itemIdx, int64_t offset)
{
    if (itemIdx == _currentItemIdx && _indexDocReader && _indexDocReader->getCurrentDocid() == offset) {
        return true;
    }
    if (itemIdx < 0 || itemIdx >= (int32_t)_totalItems.size()) {
        BS_LOG(ERROR, "can not switch to target idx [%d]", itemIdx);
        return false;
    }

    _indexDocReader.reset(new SingleIndexDocReader);
    if (!_indexDocReader->init(_totalItems[itemIdx], offset)) {
        BS_LOG(ERROR, "switch to item [%d] failed", itemIdx);
        return false;
    }
    BS_LOG(INFO, "switch to offset [%d, %ld]", itemIdx, offset);
    _currentItemIdx = itemIdx;
    _nextDocId = _indexDocReader->getCurrentDocid();
    return true;
}

int64_t MultiIndexDocReader::getLocator(int64_t itemIdx, int64_t docOffset) const
{
    return (itemIdx << OFFSET_LOCATOR_BIT_COUNT) + docOffset;
}

indexlib::document::RawDocumentParser* MultiIndexDocReader::createRawDocumentParser(const ReaderInitParam& params)
{
    return new DummyDocParser();
}

RawDocumentReader::ErrorCode MultiIndexDocReader::getNextRawDoc(document::RawDocument& rawDoc, Checkpoint* checkpoint,
                                                                DocInfo& docInfo)
{
    while (true) {
        if (!_indexDocReader->seek(_nextDocId)) {
            string errorMsg = "seek to docid " + to_string(_nextDocId) + " failed";
            REPORT_ERROR_WITH_ADVICE(READER_ERROR_READ_EXCEPTION, errorMsg, BS_IGNORE);
            return ERROR_EXCEPTION;
        }
        if (_indexDocReader->read(rawDoc)) {
            _nextDocId = _indexDocReader->getCurrentDocid() + 1;
            checkpoint->offset = getLocator(_currentItemIdx, _indexDocReader->getCurrentDocid());
            rawDoc.setDocOperateType(ADD_DOC);
            reportRawDocSize(rawDoc);
            _lastDocInfo.docId = _indexDocReader->getCurrentDocid();
            _lastDocInfo.indexVersion = _totalItems[_currentItemIdx].indexVersion;
            _lastDocInfo.indexRange = _totalItems[_currentItemIdx].indexRange;
            return ERROR_NONE;
        }
        if (_indexDocReader->isEof()) {
            if (isEof()) {
                BS_LOG(INFO, "multi index doc reader get EOF");
                return ERROR_EOF;
            } else {
                if (!switchPartition(_currentItemIdx + 1, 0)) {
                    string errorMsg = "switch partition failed";
                    REPORT_ERROR_WITH_ADVICE(READER_ERROR_READ_EXCEPTION, errorMsg, BS_IGNORE);
                    return ERROR_EXCEPTION;
                }
            }
        } else {
            string errorMsg = string("read document failed");
            REPORT_ERROR_WITH_ADVICE(READER_ERROR_READ_EXCEPTION, errorMsg, BS_IGNORE);
            return ERROR_EXCEPTION;
        }
    }
}

void MultiIndexDocReader::reportRawDocSize(document::RawDocument& rawDoc)
{
    size_t rawDocSize = rawDoc.EstimateMemory();
    REPORT_METRIC(_rawDocSizeMetric, rawDocSize);
    INCREASE_VALUE(_rawDocSizePerSecMetric, rawDocSize);
}

bool MultiIndexDocReader::seek(const Checkpoint& checkpoint)
{
    int64_t itemIdx = 0, docOffset = 0;
    docOffset = checkpoint.offset & ((1ll << OFFSET_LOCATOR_BIT_COUNT) - 1);
    itemIdx = checkpoint.offset >> OFFSET_LOCATOR_BIT_COUNT;
    if (itemIdx < 0 || itemIdx >= (int64_t)_totalItems.size()) {
        BS_LOG(ERROR, "failed to seek with item idx [%ld]", itemIdx);
        return false;
    }

    if (!switchPartition(itemIdx, docOffset)) {
        BS_LOG(ERROR, "switch to target partition [%ld] failed", itemIdx);
        return false;
    }

    BS_LOG(INFO, "multi index doc reader seek to partition [%u, %u], offset [%ld]",
           _totalItems[_currentItemIdx].indexRange.first, _totalItems[_currentItemIdx].indexRange.second, docOffset);
    return true;
}
bool MultiIndexDocReader::isEof() const
{
    assert(_indexDocReader);
    return _currentItemIdx + 1 == (int32_t)_totalItems.size() && _indexDocReader->isEof();
}

void MultiIndexDocReader::doFillDocTags(document::RawDocument& rawDoc)
{
    rawDoc.AddTag("index_range", StringUtil::toString(_lastDocInfo.indexRange.first) + "_" +
                                     StringUtil::toString(_lastDocInfo.indexRange.second));
    rawDoc.AddTag("index_version", StringUtil::toString(_lastDocInfo.indexVersion));
    rawDoc.AddTag("docid", StringUtil::toString(_lastDocInfo.docId));
}

}} // namespace build_service::reader
