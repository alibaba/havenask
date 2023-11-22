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
#include "build_service/workflow/RawDocumentRewriter.h"

#include <assert.h>
#include <cstddef>
#include <ext/alloc_traits.h>
#include <set>
#include <vector>

#include "autil/Log.h"
#include "autil/legacy/exception.h"
#include "autil/mem_pool/Pool.h"
#include "indexlib/base/Constant.h"
#include "indexlib/base/Status.h"
#include "indexlib/base/Types.h"
#include "indexlib/config/FieldConfig.h"
#include "indexlib/config/IIndexConfig.h"
#include "indexlib/config/ITabletSchema.h"
#include "indexlib/document/RawDocumentDefine.h"
#include "indexlib/document/normal/SourceDocument.h"
#include "indexlib/framework/ITablet.h"
#include "indexlib/framework/ITabletReader.h"
#include "indexlib/index/primary_key/Common.h"
#include "indexlib/index/primary_key/PrimaryKeyIndexReader.h"
#include "indexlib/index/source/Common.h"
#include "indexlib/index/source/SourceReader.h"

using namespace std;

namespace build_service { namespace workflow {
BS_LOG_SETUP(workflow, RawDocumentRewriter);

class RawDocumentRewriter::Rewriter
{
public:
    Rewriter() = default;
    virtual ~Rewriter() = default;
    virtual bool rewrite(build_service::document::RawDocument* rawDoc) = 0;
};

// PkSourceRewriter
//------------------------------------------------------------------------------------------------------

class PkSourceRewriter : public RawDocumentRewriter::Rewriter
{
public:
    PkSourceRewriter(const std::shared_ptr<indexlibv2::framework::ITablet>& tablet,
                     const std::shared_ptr<indexlibv2::config::IIndexConfig>& pkConfig,
                     const std::string& primaryKeyField)
        : _tablet(tablet)
        , _pkConfig(pkConfig)
        , _primaryKeyField(primaryKeyField)
    {
    }
    bool rewrite(build_service::document::RawDocument* rawDoc) override;

private:
    std::shared_ptr<indexlibv2::framework::ITablet> _tablet;
    std::shared_ptr<indexlibv2::config::IIndexConfig> _pkConfig;
    std::string _primaryKeyField;
    autil::mem_pool::Pool _pool;

private:
    BS_LOG_DECLARE();
};

BS_LOG_SETUP(workflow, PkSourceRewriter);

bool PkSourceRewriter::rewrite(build_service::document::RawDocument* rawDoc)
{
    if (!rawDoc) {
        AUTIL_LOG(ERROR, "rawdoc is nullptr");
        return false;
    }
    if (rawDoc->getDocOperateType() != UPDATE_FIELD) {
        return true;
    }
    auto tabletReader = _tablet->GetTabletReader();
    if (!tabletReader) {
        AUTIL_LOG(ERROR, "get tablet reader from tablet failed");
        return false;
    }
    auto pkReader = std::dynamic_pointer_cast<indexlib::index::PrimaryKeyIndexReader>(
        tabletReader->GetIndexReader(_pkConfig->GetIndexType(), _pkConfig->GetIndexName()));
    auto sourceReader = std::dynamic_pointer_cast<indexlibv2::index::SourceReader>(
        tabletReader->GetIndexReader(indexlibv2::index::SOURCE_INDEX_TYPE_STR, indexlibv2::index::SOURCE_INDEX_NAME));
    if (!pkReader || !sourceReader) {
        AUTIL_LOG(ERROR, "pk reader [%p] of source reader [%p] is nullptr", pkReader.get(), sourceReader.get());
        return false;
    }
    if (!rawDoc->exist(_primaryKeyField)) {
        AUTIL_LOG(ERROR, "pk field [%s] not exist for doc [%s]", _primaryKeyField.c_str(), rawDoc->toString().c_str());
        return false;
    }
    indexlib::docid_t docid = indexlib::INVALID_DOCID;
    try {
        docid = pkReader->Lookup(rawDoc->getField(_primaryKeyField));
    } catch (const autil::legacy::ExceptionBase& e) {
        AUTIL_LOG(ERROR, "look up pk [%s] get exception [%s], rewrite failed",
                  rawDoc->getField(_primaryKeyField).c_str(), e.what());
        return false;
    } catch (...) {
        AUTIL_LOG(ERROR, "look up pk [%s] get unknow exception , rewrite failed",
                  rawDoc->getField(_primaryKeyField).c_str());
        return false;
    }
    if (docid == indexlib::INVALID_DOCID) {
        // TODO(xiaohao.yxh) add interval log and metrics
        return true;
    }
    // TODO(xiaohao.yxh) release pool if memory use is huge
    _pool.reset();
    indexlib::document::SourceDocument sourceDocument(&_pool);
    if (!sourceReader->GetDocument(docid, &sourceDocument).IsOK()) {
        AUTIL_LOG(ERROR, "get source document for docid [%d] failed", docid);
        return false;
    }
    std::vector<std::string> fieldNames;
    std::vector<std::string> fieldValues;
    sourceDocument.ExtractFields(fieldNames, fieldValues);
    assert(fieldNames.size() == fieldValues.size());
    for (size_t i = 0; i < fieldNames.size(); ++i) {
        if (!rawDoc->exist(fieldNames[i])) {
            rawDoc->setField(fieldNames[i], fieldValues[i]);
        }
    }
    rawDoc->setDocOperateType(ADD_DOC);
    rawDoc->setField(indexlibv2::document::CMD_TAG, build_service::document::RawDocument::getCmdString(ADD_DOC));
    return true;
}

// KVRewriter
//------------------------------------------------------------------------------------------------------
class KVRewriter : public RawDocumentRewriter::Rewriter
{
public:
    KVRewriter() {}
    // TODO(xiaohao.yxh) move code from kv/kkv table writer to here
    bool rewrite(build_service::document::RawDocument* rawDoc) override { return true; }
};

bool RawDocumentRewriter::tableTypeUpdatable(const std::string& tableType)
{
    std::set<std::string> updatableTables = {"kv", "kkv", "normal"};
    return updatableTables.find(tableType) != updatableTables.end();
}

RawDocumentRewriter::RawDocumentRewriter() {}

RawDocumentRewriter::~RawDocumentRewriter() {}

bool RawDocumentRewriter::init(std::shared_ptr<indexlibv2::framework::ITablet> tablet)
{
    // TODO(xiaohao.yxh) add situation for init failed
    // eg. no source in normal schema
    if (!tablet) {
        AUTIL_LOG(ERROR, "tablet is empty");
        return false;
    }
    _tablet = tablet;

    if (!selectRewriter()) {
        AUTIL_LOG(WARN, "can't select rewriter for this schema");
        return false;
    }
    assert(_rewriter);
    AUTIL_LOG(INFO, "init raw document rewriter success, tablet [%p]", _tablet.get());
    return true;
}

bool RawDocumentRewriter::selectRewriter()
{
    auto schema = _tablet->GetTabletSchema();
    if (!schema) {
        AUTIL_LOG(ERROR, "schema is nullptr, get pk field failed");
        return false;
    }

    // try pkSource selector
    auto pkConfigs = schema->GetIndexConfigs(indexlibv2::index::PRIMARY_KEY_INDEX_TYPE_STR);
    auto sourceConfig =
        schema->GetIndexConfig(indexlibv2::index::SOURCE_INDEX_TYPE_STR, indexlibv2::index::SOURCE_INDEX_NAME);
    if (pkConfigs.size() == 1 && sourceConfig) {
        auto fieldConfigs = pkConfigs[0]->GetFieldConfigs();
        if (fieldConfigs.size() != 1) {
            AUTIL_LOG(ERROR, "field config for pk is [%lu], can not get pk field", fieldConfigs.size());
            return false;
        }
        _rewriter = std::make_unique<PkSourceRewriter>(_tablet, pkConfigs[0], fieldConfigs[0]->GetFieldName());
        AUTIL_LOG(INFO, "use pk and source to rewrite");
        return true;
    }

    // try kv selector
    if (schema->GetTableType() == "kkv" || schema->GetTableType() == "kv") {
        AUTIL_LOG(INFO, "use kv/kkv rewrite");
        _rewriter = std::make_unique<KVRewriter>();
        return true;
    }
    return false;
}

bool RawDocumentRewriter::rewrite(build_service::document::RawDocument* rawDoc) { return _rewriter->rewrite(rawDoc); }
}} // namespace build_service::workflow
