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

#include "indexlib/index/common/AtomicValueTyped.h"
#include "indexlib/index/common/MultiValue.h"
#include "indexlib/index/inverted_index/config/InvertedIndexConfig.h"
#include "indexlib/index/inverted_index/format/DocListFormatOption.h"
#include "indexlib/index/inverted_index/format/DocListSkipListFormat.h"

namespace indexlib::index {

using DocIdValue = AtomicValueTyped<docid_t>;
using TfValue = AtomicValueTyped<tf_t>;
using DocPayloadValue = AtomicValueTyped<docpayload_t>;
using FieldMapValue = AtomicValueTyped<fieldmap_t>;

class DocListFormat : public MultiValue
{
public:
    DocListFormat(const DocListFormatOption& option,
                  format_versionid_t versionId = indexlibv2::config::InvertedIndexConfig::BINARY_FORMAT_VERSION)
        : _docListSkipListFormat(nullptr)
        , _DocIdValue(nullptr)
        , _TfValue(nullptr)
        , _DocPayloadValue(nullptr)
        , _FieldMapValue(nullptr)
        , _formatVersion(versionId)
    {
        Init(option);
    }

    ~DocListFormat() { DELETE_AND_SET_NULL(_docListSkipListFormat); }

    DocIdValue* GetDocIdValue() const { return _DocIdValue; }
    TfValue* GetTfValue() const { return _TfValue; }
    DocPayloadValue* GetDocPayloadValue() const { return _DocPayloadValue; }
    FieldMapValue* GetFieldMapValue() const { return _FieldMapValue; }

    const DocListSkipListFormat* GetDocListSkipListFormat() const { return _docListSkipListFormat; }

private:
    void Init(const DocListFormatOption& option);
    void InitDocListSkipListFormat(const DocListFormatOption& option);

    DocListSkipListFormat* _docListSkipListFormat;
    DocIdValue* _DocIdValue;
    TfValue* _TfValue;
    DocPayloadValue* _DocPayloadValue;
    FieldMapValue* _FieldMapValue;
    format_versionid_t _formatVersion;

    AUTIL_LOG_DECLARE();
};

} // namespace indexlib::index
