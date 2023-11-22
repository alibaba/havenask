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
#pragma once

#include <map>
#include <memory>
#include <stddef.h>
#include <stdint.h>
#include <string>
#include <unordered_map>
#include <vector>

#include "autil/StringView.h"
#include "indexlib/base/Types.h"
#include "indexlib/framework/Locator.h"

namespace autil::mem_pool {
class Pool;
}

namespace indexlibv2::document {
class DocumentInitParam;
class RawDocFieldIterator;

class RawDocument
{
protected:
    using TagInfoMap = std::map<std::string, std::string>;

public:
    using Snapshot = std::unordered_map<autil::StringView, autil::StringView>;

    RawDocument();
    RawDocument(const RawDocument& other);
    RawDocument& operator=(const RawDocument& other);
    RawDocument(RawDocument&& other);
    virtual ~RawDocument();

public:
    virtual bool Init(const std::shared_ptr<DocumentInitParam>& initParam) = 0;
    virtual RawDocFieldIterator* CreateIterator() const = 0;
    virtual int64_t GetUserTimestamp() const = 0;

    virtual void setField(const autil::StringView& fieldName, const autil::StringView& fieldValue) = 0;
    virtual void setFieldNoCopy(const autil::StringView& fieldName, const autil::StringView& fieldValue) = 0;
    virtual const autil::StringView& getField(const autil::StringView& fieldName) const = 0;
    virtual bool exist(const autil::StringView& fieldName) const = 0;
    virtual void eraseField(const autil::StringView& fieldName) = 0;
    virtual uint32_t getFieldCount() const = 0;
    virtual void clear() = 0;
    virtual RawDocument* clone() const = 0;
    virtual RawDocument* createNewDocument() const = 0;
    virtual DocOperateType getDocOperateType() const = 0;
    virtual void setDocOperateType(DocOperateType type) = 0;
    virtual std::string getIdentifier() const = 0;
    virtual void extractFieldNames(std::vector<autil::StringView>& fieldNames) const = 0;
    virtual int64_t getDocTimestamp() const = 0;
    virtual void setDocTimestamp(int64_t timestamp) = 0;
    virtual std::string toString() const = 0;
    virtual void AddTag(const std::string& key, const std::string& value) = 0;
    virtual const std::string& GetTag(const std::string& key) const = 0;
    virtual const std::map<std::string, std::string>& GetTagInfoMap() const = 0;
    virtual const framework::Locator& getLocator() const = 0;
    virtual void SetLocator(const framework::Locator& locator) = 0;
    virtual void SetIngestionTimestamp(int64_t ingestionTimestamp) = 0;
    virtual int64_t GetIngestionTimestamp() const = 0;
    virtual void SetDocInfo(const indexlibv2::framework::Locator::DocInfo& docInfo) = 0;
    virtual indexlibv2::framework::Locator::DocInfo GetDocInfo() const = 0;
    virtual size_t EstimateMemory() const = 0;

public:
    bool NeedTrace() const;
    std::string GetTraceId() const;

    void setField(const char* fieldName, size_t nameLen, const char* fieldValue, size_t valueLen);
    void setField(const std::string& fieldName, const std::string& fieldValue);
    std::string getField(const std::string& fieldName) const;
    bool exist(const std::string& fieldName) const;
    void eraseField(const std::string& fieldName);
    autil::StringView GetEvaluatorState() const;
    void SetEvaluatorState(const autil::StringView& state);
    autil::mem_pool::Pool* getPool() const;
    const std::string& getDocSource() const;
    void setDocSource(const std::string& source);
    void setIgnoreEmptyField(bool ignoreEmptyField);
    bool ignoreEmptyField() const;
    bool IsUserDoc();
    Snapshot* GetSnapshot() const;

public:
    static DocOperateType getDocOperateType(const autil::StringView& cmdString);
    static const std::string& getCmdString(DocOperateType type);

protected:
    autil::StringView _evaluatorState;
    autil::mem_pool::Pool* _recyclePool = nullptr;
    bool _ignoreEmptyField = false;
};

} // namespace indexlibv2::document
