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
#include "build_service/reader/SwiftTopicStreamRawDocumentReader.h"

#include "autil/StringUtil.h"
#include "build_service/config/CLIOptionNames.h"
#include "build_service/util/LocatorUtil.h"
#include "build_service/util/SwiftTopicStreamReaderCreator.h"
#include "swift/client/SwiftTopicStreamReader.h"

namespace build_service::reader {
BS_LOG_SETUP(reader, SwiftTopicStreamRawDocumentReader);

SwiftTopicStreamRawDocumentReader::SwiftTopicStreamRawDocumentReader(
    const util::SwiftClientCreatorPtr& swiftClientCreator)
    : SwiftRawDocumentReader(swiftClientCreator)
{
}

SwiftTopicStreamRawDocumentReader::~SwiftTopicStreamRawDocumentReader() {}

bool SwiftTopicStreamRawDocumentReader::init(const ReaderInitParam& params)
{
    if (!SwiftRawDocumentReader::init(params)) {
        return false;
    }
    if (_enableSchema) {
        BS_LOG(ERROR, "do not support swift schema mode");
        return false;
    }
    if (_enableRawTopicFastSlowQueue) {
        BS_LOG(ERROR, "do not support fast slow queue mode");
        return false;
    }
    BS_LOG(INFO, "SwiftTopicStreamRawDocumentReader init successfully");
    return true;
}

SwiftReaderWrapper* SwiftTopicStreamRawDocumentReader::createSwiftReader(const ReaderInitParam& params)
{
    // TODO: support more read parameters
    std::string readerConfigStr;
    auto readerConfig = getValueFromKeyValueMap(params.kvMap, config::SWIFT_READER_CONFIG);
    if (!readerConfig.empty()) {
        readerConfigStr += readerConfig;
    }
    if (!readerConfigStr.empty()) {
        readerConfigStr += ";";
    }
    readerConfigStr += "from=" + autil::StringUtil::toString(params.range.from()) + ";";
    readerConfigStr += "to=" + autil::StringUtil::toString(params.range.to());

    auto zfsRoot = getValueFromKeyValueMap(params.kvMap, config::SWIFT_ZOOKEEPER_ROOT);
    auto topicListStr = getValueFromKeyValueMap(params.kvMap, util::SwiftTopicStreamReaderCreator::TOPIC_LIST);
    auto reader = util::SwiftTopicStreamReaderCreator::create(_swiftClient, topicListStr, readerConfigStr, zfsRoot);
    if (!reader) {
        return nullptr;
    }
    _swiftTopicStreamReader = dynamic_cast<swift::client::SwiftTopicStreamReader*>(reader.get());
    assert(_swiftTopicStreamReader);
    // fill swift param
    _swiftParam.reader = reader.get();
    _swiftParam.isMultiTopic = false;
    _swiftParam.disableSwiftMaskFilter = false;
    _swiftParam.topicName = getValueFromKeyValueMap(params.kvMap, config::SWIFT_TOPIC_NAME);
    _swiftParam.from = params.range.from();
    _swiftParam.to = params.range.to();
    _swiftParam.maskFilterPairs.push_back({0, 0});

    return new SwiftReaderWrapper(reader.release());
}

RawDocumentReader::ErrorCode SwiftTopicStreamRawDocumentReader::readDocStr(std::string& docStr, Checkpoint* checkpoint,
                                                                           DocInfo& docInfo)
{
    auto ec = SwiftRawDocumentReader::readDocStr(docStr, checkpoint, docInfo);
    if (ec != ERROR_NONE) {
        return ec;
    }
    if (!_swiftTopicStreamReader) {
        return ERROR_EXCEPTION;
    }
    auto streamIdx = _swiftTopicStreamReader->getCurrentIdx();
    for (auto& singleProgress : checkpoint->progress) {
        if (!util::LocatorUtil::EncodeOffset(streamIdx, singleProgress.offset.first, &singleProgress.offset.first)) {
            BS_LOG(ERROR, "encode offset failed, streamIdx [%d], singleProgress offset [%ld]", streamIdx,
                   singleProgress.offset.first);
            return ERROR_EXCEPTION;
        }
    }
    if (!util::LocatorUtil::EncodeOffset(streamIdx, docInfo.timestamp, &docInfo.timestamp)) {
        BS_LOG(ERROR, "encode timestamp failed, streamIdx [%d], tiemstamp [%ld]", streamIdx, docInfo.timestamp);
        return ERROR_EXCEPTION;
    }
    if (!util::LocatorUtil::EncodeOffset(streamIdx, checkpoint->offset, &checkpoint->offset)) {
        BS_LOG(ERROR, "encode offset failed, streamIdx [%d], tiemstamp [%ld]", streamIdx, checkpoint->offset);
        return ERROR_EXCEPTION;
    }
    return ERROR_NONE;
}

bool SwiftTopicStreamRawDocumentReader::seek(const Checkpoint& checkpoint)
{
    auto progress = checkpoint.progress;
    int8_t streamIdx;
    int64_t timestamp;
    util::LocatorUtil::DecodeOffset(checkpoint.offset, &streamIdx, &timestamp);
    auto expectTopicName = checkpoint.userData;
    auto topicName = _swiftTopicStreamReader->getTopicName((size_t)streamIdx);
    if (topicName.empty()) {
        BS_LOG(ERROR, "get topic name failed. idx [%d]", streamIdx);
        return false;
    }
    if (expectTopicName != topicName) {
        BS_LOG(ERROR, "check topic name failed. topicname in locator [%s], topicname in topic list [%s]",
               expectTopicName.c_str(), topicName.c_str());
        return false;
    }
    for (auto& singleProgress : progress) {
        util::LocatorUtil::DecodeOffset(singleProgress.offset.first, &streamIdx, &singleProgress.offset.first);
    }
    BS_LOG(INFO, "topic stream seek start, timestamp [%ld], topicName [%s], checkpoint [%s]", timestamp,
           topicName.c_str(), checkpoint.debugString().c_str());
    return SwiftRawDocumentReader::doSeek(timestamp, progress, topicName);
}

} // namespace build_service::reader
