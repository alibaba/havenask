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

#include <memory>

#include "autil/Log.h"
#include "autil/mem_pool/Pool.h"
#include "indexlib/document/normal/ModifiedTokens.h"
#include "indexlib/file_system/file/ReadOption.h"
#include "indexlib/index/common/DictKeyInfo.h"
#include "indexlib/index/inverted_index/SegmentPosting.h"

namespace indexlibv2::index {
class AttributeMemReader;
} // namespace indexlibv2::index

namespace indexlib::index {
class DictionaryReader;
class InMemBitmapIndexSegmentReader;
class DynamicIndexSegmentReader;
class InMemDynamicIndexSegmentReader;
class AttributeSegmentReader;
class InvertedIndexSearchTracer;

class IndexSegmentReader
{
public:
    IndexSegmentReader() = default;
    virtual ~IndexSegmentReader() = default;

    virtual std::shared_ptr<index::AttributeSegmentReader> GetSectionAttributeSegmentReader() const
    {
        assert(false);
        return std::shared_ptr<index::AttributeSegmentReader>();
    }

    virtual std::shared_ptr<indexlibv2::index::AttributeMemReader> GetSectionAttributeSegmentReaderV2() const
    {
        assert(false);
        return nullptr;
    }

    virtual std::shared_ptr<index::InMemBitmapIndexSegmentReader> GetBitmapSegmentReader() const
    {
        assert(false);
        return std::shared_ptr<index::InMemBitmapIndexSegmentReader>();
    }

    virtual std::shared_ptr<index::DynamicIndexSegmentReader> GetDynamicSegmentReader() const
    {
        return std::shared_ptr<index::DynamicIndexSegmentReader>();
    }
    // TODO: @qingran For v1 code
    virtual std::shared_ptr<index::InMemDynamicIndexSegmentReader> GetInMemDynamicSegmentReader() const
    {
        assert(false);
        return nullptr;
    }
    // TODO(xiaohao.yxh) for v1 code, remove nin the future
    virtual bool GetSegmentPosting(const index::DictKeyInfo& key, docid_t baseDocId, SegmentPosting& segPosting,
                                   autil::mem_pool::Pool* sessionPool,
                                   InvertedIndexSearchTracer* tracer = nullptr) const
    {
        return false;
    }

    virtual bool GetSegmentPosting(const index::DictKeyInfo& key, docid_t baseDocId, SegmentPosting& segPosting,
                                   autil::mem_pool::Pool* sessionPool, indexlib::file_system::ReadOption option,
                                   InvertedIndexSearchTracer* tracer = nullptr) const
    {
        return GetSegmentPosting(key, baseDocId, segPosting, sessionPool, tracer);
    }

    virtual std::shared_ptr<DictionaryReader> GetDictionaryReader() const
    {
        assert(false);
        return nullptr;
    }
    virtual std::shared_ptr<AttributeSegmentReader> GetPKAttributeReader() const
    {
        assert(false);
        return std::shared_ptr<AttributeSegmentReader>();
    }
    // Used when updating some updatable inverted index types.
    virtual void Update(docid_t docId, const document::ModifiedTokens& tokens)
    {
        for (size_t i = 0; i < tokens.NonNullTermSize(); ++i) {
            auto item = tokens[i];
            Update(docId, index::DictKeyInfo(item.first), item.second == document::ModifiedTokens::Operation::REMOVE);
        }
        document::ModifiedTokens::Operation nullTermOp;
        if (tokens.GetNullTermOperation(&nullTermOp)) {
            Update(docId, index::DictKeyInfo::NULL_TERM, nullTermOp == document::ModifiedTokens::Operation::REMOVE);
        }
    }

    virtual void Update(docid_t docId, const index::DictKeyInfo& key, bool isDelete) {}

private:
    AUTIL_LOG_DECLARE();
};

} // namespace indexlib::index
