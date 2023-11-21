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

#include "gmock/gmock.h"

#include "indexlib/document/IDocument.h"
#include "indexlib/document/IDocumentBatch.h"
#include "indexlib/framework/Locator.h"
namespace indexlibv2::framework {

class MockDocument : public document::IDocument
{
public:
    MockDocument() {}
    virtual ~MockDocument() {}

public:
    MOCK_METHOD(const framework::Locator&, GetLocatorV2, (), (const, override));
    MOCK_METHOD(void, SetLocator, (const Locator&), (override));
    MOCK_METHOD(void, SetTrace, (bool), (override));
    MOCK_METHOD(framework::Locator::DocInfo, GetDocInfo, (), (const, override));
    MOCK_METHOD(void, SetDocInfo, (const framework::Locator::DocInfo&), (override));
    MOCK_METHOD(uint32_t, GetTTL, (), (const, override));
    MOCK_METHOD(docid_t, GetDocId, (), (const, override));
    MOCK_METHOD(autil::StringView, GetTraceId, (), (const, override));
    MOCK_METHOD(size_t, EstimateMemory, (), (const, override));
    MOCK_METHOD(DocOperateType, GetDocOperateType, (), (const, override));
    MOCK_METHOD(int64_t, GetIngestionTimestamp, (), (const, override));
    MOCK_METHOD(schemaid_t, GetSchemaId, (), (const, override));
    MOCK_METHOD(autil::StringView, GetSource, (), (const, override));
    MOCK_METHOD(void, SetDocOperateType, (DocOperateType), (override));
    MOCK_METHOD(bool, HasFormatError, (), (const, override));
    MOCK_METHOD(void, serialize, (autil::DataBuffer&), (const, override));
    MOCK_METHOD(void, deserialize, (autil::DataBuffer&), (override));
};

class MockDocumentBatch : public document::IDocumentBatch
{
public:
    MockDocumentBatch()
    {
        static Locator dummyLocator;
        ON_CALL(*this, GetLastLocator()).WillByDefault(testing::ReturnRef(dummyLocator));
    }
    ~MockDocumentBatch() = default;

    MOCK_METHOD(std::unique_ptr<IDocumentBatch>, Create, (), (const, override));
    MOCK_METHOD(int64_t, GetMaxTimestamp, (), (const, override));
    MOCK_METHOD(const Locator&, GetLastLocator, (), (const, override));

    MOCK_METHOD(void, SetBatchLocator, (const framework::Locator&), (override));
    MOCK_METHOD(const Locator&, GetBatchLocator, (), (const, override));
    MOCK_METHOD(void, DropDoc, (int64_t), (override));
    MOCK_METHOD(void, ReleaseDoc, (int64_t), (override));
    MOCK_METHOD(int64_t, GetMaxTTL, (), (const, override));
    MOCK_METHOD(void, SetMaxTTL, (int64_t), (override));
    MOCK_METHOD(void, AddDocument, (DocumentPtr), (override));
    MOCK_METHOD(std::shared_ptr<document::IDocument>, BracketOp, (int64_t), (const));
    MOCK_METHOD(size_t, GetAddedDocCount, (), (const, override));

    size_t EstimateMemory() const override { return 0; }
    std::shared_ptr<document::IDocument> operator[](int64_t docIdx) const override { return BracketOp(docIdx); }
    std::shared_ptr<document::IDocument> TEST_GetDocument(int64_t docIdx) const override { return BracketOp(docIdx); }
    size_t GetBatchSize() const override { return _docVec.size(); }
    bool IsDropped(int64_t docIdx) const override { return _docVec[docIdx] < 0; }
    size_t GetValidDocCount() const override
    {
        size_t validDocCount = 0;
        for (size_t i = 0; i < _docVec.size(); ++i) {
            if (_docVec[i] >= 0) {
                ++validDocCount;
            }
        }
        return validDocCount;
    }

public:
    void SetDocs(std::vector<int> docVec) { _docVec = docVec; }

private:
    std::vector<int> _docVec;
};
} // namespace indexlibv2::framework
