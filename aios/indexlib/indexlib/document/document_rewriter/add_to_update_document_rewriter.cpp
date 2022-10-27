#include "indexlib/document/document_rewriter/add_to_update_document_rewriter.h"
#include "indexlib/document/index_document/normal_document/normal_document.h"

using namespace std;
IE_NAMESPACE_USE(config);
IE_NAMESPACE_USE(common);

IE_NAMESPACE_BEGIN(document);
IE_LOG_SETUP(document, AddToUpdateDocumentRewriter);

AddToUpdateDocumentRewriter::AddToUpdateDocumentRewriter() 
{
}

AddToUpdateDocumentRewriter::~AddToUpdateDocumentRewriter() 
{
}

void AddToUpdateDocumentRewriter::Init(
        const IndexPartitionSchemaPtr& schema,
        const vector<TruncateOptionConfigPtr>& truncateOptionConfigs,
        const vector<SortDescriptions>& sortDescVec)
{
    mFieldSchema = schema->GetFieldSchema();
    AllocFieldBitmap();
    AddUpdatableFields(schema, sortDescVec);
    mAttrFieldExtractor.reset(new AttributeDocumentFieldExtractor());
    
    if (!mAttrFieldExtractor->Init(schema->GetAttributeSchema()))
    {
        INDEXLIB_FATAL_ERROR(InitializeFailed, "fail to init AttributeDocumentFieldExtractor!"); 
    }
    
    for (size_t i = 0; i < truncateOptionConfigs.size(); i++)
    {
        FilterTruncateSortFields(truncateOptionConfigs[i]);
    }
}

void AddToUpdateDocumentRewriter::AllocFieldBitmap()
{
    assert(mFieldSchema);
    size_t fieldCount = mFieldSchema->GetFieldCount();
    mUpdatableFieldIds.Clear();
    mUpdatableFieldIds.Alloc(fieldCount);

    mUselessFieldIds.Clear();
    mUselessFieldIds.Alloc(fieldCount);
}

void AddToUpdateDocumentRewriter::AddUpdatableFields(
        const IndexPartitionSchemaPtr& schema, 
        const vector<SortDescriptions>& sortDescVec)
{
    const IndexSchemaPtr& indexSchema = schema->GetIndexSchema();
    const AttributeSchemaPtr& attrSchema = schema->GetAttributeSchema();
    assert(indexSchema);
    assert(attrSchema);

    FieldSchema::Iterator iter = mFieldSchema->Begin();
    for (; iter != mFieldSchema->End(); iter++)
    {
        const FieldConfigPtr& fieldConfig = *iter;
        if (fieldConfig->IsDeleted())
        {
            continue;
        }
        
        fieldid_t fieldId = fieldConfig->GetFieldId();
        const string& fieldName = fieldConfig->GetFieldName();

        if (!schema->IsUsefulField(fieldName))
        {
            SetUselessField(fieldName);
            continue;
        }

        if (fieldConfig->IsAttributeUpdatable() &&  //updatable field
            attrSchema->IsInAttribute(fieldId) &&  // in attribute
            !indexSchema->IsInIndex(fieldId) && // not in index
            !IsSortField(fieldName, sortDescVec)) // not in sort field
        {
            SetField(fieldName, true);
        }
    }
}

bool AddToUpdateDocumentRewriter::IsSortField(
        const string& fieldName, const vector<SortDescriptions>& sortDescVec)
{
    for (size_t i = 0; i < sortDescVec.size(); i++)
    {
        for (auto sortDesc : sortDescVec[i])
        {
            if (sortDesc.sortFieldName == fieldName)
            {
                return true;
            }
        }
    }
    return false;
}

void AddToUpdateDocumentRewriter::FilterTruncateSortFields(
        const TruncateOptionConfigPtr& truncOptionConfig)
{
    if (!truncOptionConfig) { return; }

    const TruncateIndexConfigMap& truncateIndexConfigs 
        = truncOptionConfig->GetTruncateIndexConfigs();
    TruncateIndexConfigMap::const_iterator iter = truncateIndexConfigs.begin();
    for (; iter != truncateIndexConfigs.end(); ++iter)
    {
        const TruncateIndexPropertyVector& truncIndexPropeties
            = iter->second.GetTruncateIndexProperties();
        for (size_t i = 0; i < truncIndexPropeties.size(); ++i)
        {
            const TruncateProfilePtr& profile 
                = truncIndexPropeties[i].mTruncateProfile;
            FilterTruncateProfileField(profile);

            const TruncateStrategyPtr& strategy 
                = truncIndexPropeties[i].mTruncateStrategy;
            FilterTruncateStrategyField(strategy);
        }
    }
}

void AddToUpdateDocumentRewriter::FilterTruncateProfileField(
        const TruncateProfilePtr& profile)
{
    assert (profile);
    const SortParams& sortParams = profile->mSortParams;
    for (size_t j = 0; j < sortParams.size(); ++j)
    {
        const string& sortField = sortParams[j].GetSortField();
        if (!SetField(sortField, false))
        {

            IE_LOG(WARN, "invalid field %s in %s", sortField.c_str(),
                   profile->mTruncateProfileName.c_str());
            ERROR_COLLECTOR_LOG(WARN, "invalid field %s in %s", sortField.c_str(),
                    profile->mTruncateProfileName.c_str());
        }
    }
}

bool AddToUpdateDocumentRewriter::SetUselessField(const string& uselessFieldName)
{
    fieldid_t fid = mFieldSchema->GetFieldId(uselessFieldName);
    if (fid == INVALID_FIELDID) 
    {
        return false;
    }
    
    mUselessFieldIds.Set(fid);
    return true;
}

bool AddToUpdateDocumentRewriter::SetField(
        const string& fieldName, bool isUpdatable)
{
    fieldid_t fid = mFieldSchema->GetFieldId(fieldName);
    if (fid == INVALID_FIELDID) 
    {
        return false;
    }
    if (isUpdatable)
    {
        mUpdatableFieldIds.Set(fid);
    }
    else
    {
        mUpdatableFieldIds.Reset(fid);
    }
    return true;
}

void AddToUpdateDocumentRewriter::FilterTruncateStrategyField(
        const TruncateStrategyPtr& strategy)
{
    assert (strategy);
    const string& filterField = 
        strategy->GetDiversityConstrain().GetFilterField();
    if (filterField.empty()) { return; }
    if (!SetField(filterField, false))
    {
        IE_LOG(WARN, "invalid field %s in %s", filterField.c_str(),
               strategy->GetStrategyName().c_str());
        ERROR_COLLECTOR_LOG(WARN, "invalid field %s in %s", filterField.c_str(),
               strategy->GetStrategyName().c_str());
    }
}

void AddToUpdateDocumentRewriter::Rewrite(const DocumentPtr& document)
{
    NormalDocumentPtr doc = DYNAMIC_POINTER_CAST(NormalDocument, document);
    AttributeDocumentPtr attrDoc = TryRewrite(doc);
    if (!attrDoc)
    {
        doc->ClearModifiedFields();
        doc->ClearSubModifiedFields();
        return;
    }

    RewriteIndexDocument(doc);
    doc->SetSummaryDocument(SerializedSummaryDocumentPtr());
    doc->SetAttributeDocument(attrDoc);
    doc->ModifyDocOperateType(UPDATE_FIELD);
    doc->ClearModifiedFields();
    doc->ClearSubModifiedFields();
}

void AddToUpdateDocumentRewriter::RewriteIndexDocument(
        const document::NormalDocumentPtr& doc)
{
    const IndexDocumentPtr& indexDoc = doc->GetIndexDocument();
    if (!indexDoc)
    {
        INDEXLIB_FATAL_ERROR(UnSupported, "indexDocument is NULL!");
    }

    string pkStr = indexDoc->GetPrimaryKey();
    indexDoc->ClearData();
    indexDoc->SetPrimaryKey(pkStr);
}

AttributeDocumentPtr AddToUpdateDocumentRewriter::TryRewrite(
        const document::NormalDocumentPtr& doc)
{
    //TODO: summary store in attribute
    if (doc->GetDocOperateType() != ADD_DOC
        || !doc->GetAttributeDocument())
    {
        return AttributeDocumentPtr();
    }

    const std::vector<fieldid_t>& modifiedFields = doc->GetModifiedFields();
    if (modifiedFields.empty())
    {
        return AttributeDocumentPtr();
    }

    // TODO: support sub doc
    if (!doc->GetSubModifiedFields().empty())
    {
        return AttributeDocumentPtr();
    }

    AttributeDocumentPtr attrDoc(new AttributeDocument);
    const AttributeDocumentPtr& oriAttrDoc = doc->GetAttributeDocument();

    assert(oriAttrDoc);
    for (size_t i = 0; i < modifiedFields.size(); ++i) 
    {
        fieldid_t fid = modifiedFields[i];
        if (mUselessFieldIds.Test(fid))
        {
            continue;
        }

        if (!mUpdatableFieldIds.Test(fid))
        {
            IE_LOG(DEBUG, "doc (pk = %s) modified field [%s] is not updatable",
                   doc->GetIndexDocument()->GetPrimaryKey().c_str(),
                   mFieldSchema->GetFieldConfig(fid)->GetFieldName().c_str());
            ERROR_COLLECTOR_LOG(ERROR, "doc (pk = %s) modified field [%s] is not updatable",
                    doc->GetIndexDocument()->GetPrimaryKey().c_str(),
                   mFieldSchema->GetFieldConfig(fid)->GetFieldName().c_str());
            return AttributeDocumentPtr();
        }
        attrDoc->SetField(fid, mAttrFieldExtractor->GetField(oriAttrDoc, fid, doc->GetPool()));
    }
    return attrDoc;
}

IE_NAMESPACE_END(document);

