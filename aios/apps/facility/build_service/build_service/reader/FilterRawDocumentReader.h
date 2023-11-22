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
#include <memory>
#include <string>

#include "build_service/common_define.h"
#include "build_service/reader/RawDocumentReader.h"

namespace build_service { namespace reader {

class RawDocFilter
{
public:
    RawDocFilter(const std::string& fieldName, const std::string& filterValue,
                 const std::function<bool(const std::string&, const std::string&)>& func);
    ~RawDocFilter();

public:
    bool filter(const document::RawDocument& rawDoc);

private:
    std::string _fieldName;
    std::string _filterValue;
    std::function<bool(const std::string&, const std::string&)> _func;

private:
    BS_LOG_DECLARE();
};

class FilterRawDocumentReader : public RawDocumentReader
{
public:
    FilterRawDocumentReader(RawDocumentReader* impl, const std::string& filterFiledName, const std::string& filterValue,
                            const std::function<bool(const std::string&, const std::string&)>& func);
    ~FilterRawDocumentReader();

private:
    FilterRawDocumentReader(const FilterRawDocumentReader&);
    FilterRawDocumentReader& operator=(const FilterRawDocumentReader&);

public:
    bool init(const ReaderInitParam& params) override;
    bool seek(const Checkpoint& checkpoint) override;
    bool isEof() const override;
    void suspendReadAtTimestamp(int64_t timestamp, common::ExceedTsAction action) override;
    bool isExceedSuspendTimestamp() const override;
    ErrorCode getNextRawDoc(document::RawDocument& rawDoc, Checkpoint* checkpoint, DocInfo& docInfo) override;

protected:
    ErrorCode readDocStr(std::string& docStr, Checkpoint* checkpoint, DocInfo& docInfo) override;
    ErrorCode readDocStr(std::vector<Message>& msgs, Checkpoint* checkpoint, size_t maxMessageCount) override;

    virtual indexlib::document::RawDocumentParser* createRawDocumentParser(const ReaderInitParam& params) override;
    virtual bool initDocumentFactoryWrapper(const ReaderInitParam& params) override;

private:
    std::unique_ptr<RawDocumentReader> _impl;
    RawDocFilter _filter;
};

}} // namespace build_service::reader
