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
#include "build_service/reader/FilterRawDocumentReader.h"

using namespace std;

namespace build_service { namespace reader {

BS_LOG_SETUP(reader, RawDocFilter);

RawDocFilter::RawDocFilter(const std::string& fieldName, const std::string& filterValue,
                           const std::function<bool(const std::string&, const std::string&)>& func)
    : _fieldName(fieldName)
    , _filterValue(filterValue)
    , _func(func)
{
}

RawDocFilter::~RawDocFilter() = default;

bool RawDocFilter::filter(const document::RawDocument& rawDoc)
{
    string value = rawDoc.getField(_fieldName);
    return _func(value, _filterValue);
}

FilterRawDocumentReader::FilterRawDocumentReader(
    RawDocumentReader* impl, const std::string& filterFieldName, const std::string& filterValue,
    const std::function<bool(const std::string&, const std::string&)>& func)
    : _impl(impl)
    , _filter(filterFieldName, filterValue, func)
{
}

FilterRawDocumentReader::~FilterRawDocumentReader() = default;

bool FilterRawDocumentReader::init(const ReaderInitParam& params) { return _impl->init(params); }

bool FilterRawDocumentReader::seek(const Checkpoint& checkpoint) { return _impl->seek(checkpoint); }

bool FilterRawDocumentReader::isEof() const { return _impl->isEof(); }

void FilterRawDocumentReader::suspendReadAtTimestamp(int64_t timestamp, common::ExceedTsAction action)
{
    return _impl->suspendReadAtTimestamp(timestamp, action);
}

bool FilterRawDocumentReader::isExceedSuspendTimestamp() const { return _impl->isExceedSuspendTimestamp(); }

RawDocumentReader::ErrorCode FilterRawDocumentReader::getNextRawDoc(document::RawDocument& rawDoc,
                                                                    Checkpoint* checkpoint, DocInfo& docInfo)
{
    auto ec = _impl->getNextRawDoc(rawDoc, checkpoint, docInfo);
    if (ec != ERROR_NONE) {
        return ec;
    }

    if (!_filter.filter(rawDoc)) {
        return ERROR_SKIP;
    }
    return ec;
}

RawDocumentReader::ErrorCode FilterRawDocumentReader::readDocStr(std::string& docStr, Checkpoint* checkpoint,
                                                                 DocInfo& docInfo)
{
    return _impl->readDocStr(docStr, checkpoint, docInfo);
}

RawDocumentReader::ErrorCode FilterRawDocumentReader::readDocStr(std::vector<Message>& msgs, Checkpoint* checkpoint,
                                                                 size_t maxMessageCount)
{
    return _impl->readDocStr(msgs, checkpoint, maxMessageCount);
}

indexlib::document::RawDocumentParser* FilterRawDocumentReader::createRawDocumentParser(const ReaderInitParam& params)
{
    return _impl->createRawDocumentParser(params);
}

bool FilterRawDocumentReader::initDocumentFactoryWrapper(const ReaderInitParam& params)
{
    return RawDocumentReader::initDocumentFactoryWrapper(params);
}

}} // namespace build_service::reader
