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
#include <string>
#include <vector>

#include "build_service/common_define.h"
#include "build_service/document/ExtendDocument.h"
#include "build_service/processor/DocumentProcessor.h"
#include "build_service/processor/ModifiedFieldsModifier.h"
#include "build_service/util/Log.h"
#include "indexlib/base/Types.h"
#include "indexlib/config/ITabletSchema.h"
#include "indexlib/config/index_partition_schema.h"
#include "indexlib/document/extend_document/indexlib_extend_document.h"
#include "indexlib/indexlib.h"

namespace build_service { namespace processor {
// ModifiedFieldsDocumentProcessor does 3 things:
// 1. Rewrite ModifiedFields according to schema, for example if some fields in ModifiedFields are not in schema, those
// fields will be skipped.
// 2. Some field in ModifiedFields might cause changes in multiple other fields, thus we need to mark the corresponding
// fields as well.
// 3. If HA_RESERVED_MODIFIED_VALUES is not empty, copy values in these fields to LAST_VALUE_PREFIX+field_name. This is
// used to save value from last doc, so that we can do diff between old and new doc and convert ADD to UPDATE later.
class ModifiedFieldsDocumentProcessor : public PooledDocumentProcessor
{
public:
    typedef std::vector<ModifiedFieldsModifierPtr> ModifiedFieldsModifierVec;
    typedef std::map<std::string, fieldid_t> FieldIdMap;
    typedef indexlib::document::IndexlibExtendDocument::FieldIdSet FieldIdSet;
    typedef document::ExtendDocument::ExtendDocumentVec ExtendDocumentVec;

    static const std::string PROCESSOR_NAME;
    static const std::string DERIVATIVE_RELATIONSHIP;
    static const std::string IGNORE_FIELDS;
    static const std::string IGNORE_FIELDS_SEP;
    static const std::string DERIVATIVE_FAMILY_SEP;
    static const std::string DERIVATIVE_PATERNITY_SEP;
    static const std::string DERIVATIVE_BROTHER_SEP;
    static const std::string REWRITE_MODIFY_FIELDS_BY_SCHEMA;

public:
    ModifiedFieldsDocumentProcessor();
    ~ModifiedFieldsDocumentProcessor();

private:
    ModifiedFieldsDocumentProcessor& operator=(const ModifiedFieldsDocumentProcessor&);

public:
    bool process(const document::ExtendDocumentPtr& document) override;
    void batchProcess(const std::vector<document::ExtendDocumentPtr>& docs) override;
    void destroy() override;
    DocumentProcessor* clone() override;
    bool init(const DocProcessorInitParam& param) override;
    std::string getDocumentProcessorName() const override { return PROCESSOR_NAME; }

private:
    bool parseParamIgnoreFields(const KeyValueMap& kvMap);
    bool parseRelationshipSinglePaternity(const std::string& paternityStr);
    bool parseParamRelationship(const KeyValueMap& kvMap);

    void initModifiedFieldSet(const document::ExtendDocumentPtr& document, FieldIdSet* unknownFieldIdSet);

    void rewriteModifyFields(const document::ExtendDocumentPtr& document, bool* needSkipDoc);

    template <typename SchemaType, typename SubSchemaType>
    void doInitModifiedFieldSet(const std::shared_ptr<SchemaType>& schema,
                                const std::shared_ptr<SubSchemaType>& subSchema,
                                const document::ExtendDocumentPtr& document, FieldIdSet* unknownFieldIdSet);
    bool checkModifiedFieldSet(const document::ExtendDocumentPtr& document) const;
    void clearFieldId(const document::ExtendDocumentPtr& document) const;

private:
    ModifiedFieldsModifierVec _modifiedFieldsModifierVec;
    FieldIdMap _unknownFieldIdMap;
    // indexlib::config::IndexPartitionSchemaPtr _schema;
    std::shared_ptr<indexlibv2::config::ITabletSchema> _schema;
    indexlib::config::IndexPartitionSchemaPtr _subSchema; // legacy sub table compatiblity
    bool _needRewriteModifyFields;
    indexlib::util::MetricPtr _rewriteModifyFieldQps;

    std::vector<std::string> _modifiedFieldsVec;

private:
    BS_LOG_DECLARE();
};

BS_TYPEDEF_PTR(ModifiedFieldsDocumentProcessor);

}} // namespace build_service::processor
