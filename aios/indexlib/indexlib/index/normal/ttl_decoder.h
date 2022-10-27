#ifndef __INDEXLIB_TTLDECODER_H
#define __INDEXLIB_TTLDECODER_H

#include <tr1/memory>
#include "indexlib/indexlib.h"
#include "indexlib/common_define.h"

DECLARE_REFERENCE_CLASS(document, Document);
DECLARE_REFERENCE_CLASS(config, IndexPartitionSchema);
DECLARE_REFERENCE_CLASS(common, AttributeConvertor);

IE_NAMESPACE_BEGIN(index);

class TTLDecoder
{
public:
    TTLDecoder(const config::IndexPartitionSchemaPtr& schema);
    ~TTLDecoder();
public:
    void SetDocumentTTL(const document::DocumentPtr& document) const;
    
private:
    fieldid_t mTTLFieldId;
    common::AttributeConvertorPtr mConverter;
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(TTLDecoder);

IE_NAMESPACE_END(index);

#endif //__INDEXLIB_TTLDECODER_H
