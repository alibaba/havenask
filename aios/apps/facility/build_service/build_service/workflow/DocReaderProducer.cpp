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
#include "build_service/workflow/DocReaderProducer.h"

#include <cstdint>
#include <iosfwd>
#include <string>
#include <utility>

#include "autil/EnvUtil.h"
#include "autil/Log.h"
#include "autil/StringUtil.h"
#include "autil/TimeUtility.h"
#include "build_service/common/End2EndLatencyReporter.h"
#include "build_service/document/DocumentDefine.h"
#include "build_service/reader/SwiftRawDocumentReader.h"
#include "build_service/workflow/RawDocChecksumer.h"
#include "indexlib/base/Progress.h"
#include "indexlib/base/Types.h"
#include "indexlib/document/RawDocument.h"

using namespace std;

using namespace build_service::reader;
using namespace build_service::document;
namespace build_service { namespace workflow {

BS_LOG_SETUP(workflow, DocReaderProducer);

std::string packUserLocator(uint64_t checksum, const std::string& userLocator)
{
    std::string locator(sizeof(checksum), ' ');
    *((uint64_t*)locator.data()) = checksum;
    locator += userLocator;
    return locator;
}

std::pair<uint64_t, std::string> extractUserLocator(const std::string& locatorStr)
{
    std::pair<uint64_t, std::string> ret(0, "");
    if (!locatorStr.empty()) {
        ret.first = *((uint64_t*)(locatorStr.c_str()));
        ret.second = locatorStr.substr(sizeof(ret.first));
    }
    return ret;
}

DocReaderProducer::DocReaderProducer(const proto::PartitionId& pid, const config::ResourceReaderPtr& resourceReader,
                                     reader::RawDocumentReader* reader, uint64_t srcSignature)
    : _reader(reader)
    , _srcSignature(srcSignature)
    , _lastReadOffset(0)
    , _ckpDocReportInterval(60)
{
    _checksum.reset(new RawDocChecksumer(pid, resourceReader));
    _ckpDocReportTime = autil::TimeUtility::currentTimeInSeconds();
    string param = autil::EnvUtil::getEnv("checkpoint_report_interval_in_s");
    int64_t interval;
    if (!param.empty() && autil::StringUtil::fromString(param, interval)) {
        _ckpDocReportInterval = interval;
    }
    if (_ckpDocReportInterval >= 0) {
        AUTIL_LOG(INFO, "Use CHECKPOINT_DOC, _ckpDocReportInterval is [%ld]s.", _ckpDocReportInterval);
    } else {
        AUTIL_LOG(INFO, "Forbidden CHECKPOINT_DOC, _ckpDocReportInterval is [%ld]s.", _ckpDocReportInterval);
    }
}

DocReaderProducer::~DocReaderProducer() {}

FlowError DocReaderProducer::produce(document::RawDocumentPtr& rawDocPtr)
{
    if (!_checksum->ensureInited()) {
        AUTIL_LOG(ERROR, "prepare checksum checkerr failed");
        return FE_FATAL;
    }
    Checkpoint checkpoint;
    RawDocumentReader::ErrorCode ec = _reader->read(rawDocPtr, &checkpoint);
    _lastReadOffset.store(checkpoint.offset, std::memory_order_relaxed);
    if (RawDocumentReader::ERROR_NONE == ec) {
        common::Locator locator(_srcSignature, checkpoint.offset);
        if (checkpoint.progress.size() > 0) {
            locator.SetMultiProgress(checkpoint.progress);
        }
        if (_checksum->enabled()) {
            _checksum->evaluate(rawDocPtr);
            locator.SetUserData(packUserLocator(_checksum->getChecksum(), checkpoint.userData));
        } else {
            locator.SetUserData(checkpoint.userData);
        }
        rawDocPtr->SetLocator(locator);
        return FE_OK;
    } else if (RawDocumentReader::ERROR_EOF == ec) {
        if (!_checksum->finish()) {
            return FE_FATAL;
        }
        return FE_EOF;
    } else if (RawDocumentReader::ERROR_SEALED == ec) {
        return FE_SEALED;
    } else if (RawDocumentReader::ERROR_WAIT == ec) {
        int64_t currentTime = autil::TimeUtility::currentTimeInSeconds();
        if (_ckpDocReportInterval >= 0 && currentTime - _ckpDocReportTime >= _ckpDocReportInterval) {
            // TODO(tianxiao) high bit, detect locator slow
            AUTIL_LOG(DEBUG, "Create CHECKPOINT_DOC, locator: src[%lu], offset[%ld], userData[%s].", _srcSignature,
                      checkpoint.offset, checkpoint.userData.c_str());
            _ckpDocReportTime = currentTime;
            common::Locator locator(_srcSignature, checkpoint.offset);
            if (_checksum->enabled()) {
                locator.SetUserData(packUserLocator(_checksum->getChecksum(), checkpoint.userData));
            } else {
                locator.SetUserData(checkpoint.userData);
            }
            if (checkpoint.progress.size() > 0) {
                locator.SetMultiProgress(checkpoint.progress);
            }
            rawDocPtr->SetLocator(locator);
            rawDocPtr->setDocOperateType(CHECKPOINT_DOC);
            return FE_OK;
            // no message more than 60s, report processor checkpoint use current time
        }
        return FE_WAIT;
    } else if (RawDocumentReader::ERROR_PARSE == ec || RawDocumentReader::ERROR_SKIP == ec) {
        return FE_SKIP;
    } else if (RawDocumentReader::ERROR_EXCEPTION == ec) {
        return FE_FATAL;
    } else {
        return FE_RETRY;
    }
}

bool DocReaderProducer::seek(const common::Locator& locator)
{
    if (!locator.IsSameSrc(common::Locator(_srcSignature, 0), false)) {
        BS_LOG(INFO, "do not seek, current src[%lu], seek locator [%s], src not equal ", _srcSignature,
               locator.DebugString().c_str());
        return true;
    }
    if (!_checksum->ensureInited()) {
        AUTIL_LOG(ERROR, "prepare checksum checkerr failed");
        return false;
    }
    std::string userData = locator.GetUserData();
    if (_checksum->enabled()) {
        auto [checksumLocator, tmpUserData] = extractUserLocator(locator.GetUserData());
        _checksum->recover(checksumLocator);
        userData = tmpUserData;
    }
    BS_LOG(INFO, "seek locator [%s]", locator.DebugString().c_str());
    return _reader->seek(Checkpoint(locator.GetOffset().first, locator.GetMultiProgress(), userData));
}

bool DocReaderProducer::stop(StopOption stopOption) { return true; }

bool DocReaderProducer::getMaxTimestamp(int64_t& timestamp)
{
    if (!_reader) {
        BS_LOG(WARN, "getMaxTimestamp failed because has no reader.");
        return false;
    }

    SwiftRawDocumentReader* swiftReader = dynamic_cast<SwiftRawDocumentReader*>(_reader);
    if (swiftReader) {
        return swiftReader->getMaxTimestampAfterStartTimestamp(timestamp);
    }

    if (_reader->isEof()) {
        BS_LOG(INFO, "getMaxTimestamp, read file source EOF.");
        timestamp = -1;
        return true;
    }
    return false;
}

bool DocReaderProducer::getLastReadTimestamp(int64_t& timestamp)
{
    if (!_reader) {
        BS_LOG(WARN, "getMaxTimestamp failed because has no reader.");
        return false;
    }

    SwiftRawDocumentReader* swiftReader = dynamic_cast<SwiftRawDocumentReader*>(_reader);
    if (swiftReader) {
        timestamp = _lastReadOffset.load(std::memory_order_relaxed);
        return true;
    }

    if (_reader->isEof()) {
        BS_LOG(INFO, "getLastReadTimestamp, read file source EOF.");
        timestamp = -1;
        return true;
    }
    return false;
}

void DocReaderProducer::setRecovered(bool isRecoverd)
{
    if (_reader) {
        _reader->GetEnd2EndLatencyReporter().setIsServiceRecovered(isRecoverd);
    }
}

}} // namespace build_service::workflow
