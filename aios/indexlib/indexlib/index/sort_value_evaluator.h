#ifndef __INDEXLIB_SORT_VALUE_EVALUATOR_H
#define __INDEXLIB_SORT_VALUE_EVALUATOR_H

#include <tr1/memory>
#include "indexlib/indexlib.h"
#include "indexlib/common_define.h"
#include "indexlib/config/attribute_config.h"
#include "indexlib/common/field_format/attribute/attribute_convertor_factory.h"
#include "indexlib/index/normal/inverted_index/truncate/doc_info_allocator.h"
#include "indexlib/document/index_document/normal_document/attribute_document.h"

IE_NAMESPACE_BEGIN(index);

class SortValueEvaluator
{
public:
    SortValueEvaluator()
        : mOffset(0)
        , mFieldId(INVALID_FIELDID)
    {}

    virtual ~SortValueEvaluator() {}

public:
    void Init(const config::FieldConfigPtr& fieldConfig,
              index::Reference* refer)
    {
        assert(fieldConfig);
        assert(refer);
        
        mAttrConvertor.reset(common::AttributeConvertorFactory
                             ::GetInstance()->CreateAttrConvertor(fieldConfig));
        assert(mAttrConvertor);

        mOffset = refer->GetOffset();
        mFieldId = fieldConfig->GetFieldId();
    }

    virtual void Evaluate(const document::AttributeDocumentPtr& attrDoc,
                          index::DocInfo* docInfo) = 0;

protected:
    common::AttributeConvertorPtr mAttrConvertor;
    size_t mOffset;
    fieldid_t mFieldId;
    
private:
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(SortValueEvaluator);


template <typename T>
class SortValueEvaluatorTyped : public SortValueEvaluator
{
public:
    SortValueEvaluatorTyped() {}
    ~SortValueEvaluatorTyped() {}

    
    void Evaluate(const document::AttributeDocumentPtr& attrDoc,
                  index::DocInfo* docInfo) override
    {
        assert(attrDoc);
        const autil::ConstString& field = attrDoc->GetField(mFieldId);

        uint8_t* buffer = docInfo->Get(mOffset);
        T& value = *(T*)buffer;

        if (likely(!field.empty()))
        {
            assert(mAttrConvertor);
            common::AttrValueMeta meta = mAttrConvertor->Decode(field);
            value = *(T*)meta.data.data();
        }
        else
        {
            value = 0;
        }
    }
};

IE_NAMESPACE_END(index);

#endif //__INDEXLIB_SORT_VALUE_EVALUATOR_H
