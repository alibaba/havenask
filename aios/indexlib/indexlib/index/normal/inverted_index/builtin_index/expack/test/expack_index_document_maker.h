#ifndef __INDEXLIB_EXPACK_INDEX_DOCUMENT_MAKER_H
#define __INDEXLIB_EXPACK_INDEX_DOCUMENT_MAKER_H

#include <tr1/memory>
#include <vector>
#include "indexlib/common_define.h"
#include "indexlib/indexlib.h"

#include "indexlib/config/field_schema.h"
#include "indexlib/config/index_schema.h"
#include "indexlib/config/package_index_config.h"
#include "indexlib/config/impl/package_index_config_impl.h"
#include "indexlib/index/test/index_document_maker.h"

IE_NAMESPACE_BEGIN(index);

class ExpackIndexDocumentMaker : public IndexDocumentMaker
{
public:
    static void MakeIndexDocuments(autil::mem_pool::Pool* pool,
                                   std::vector<document::IndexDocumentPtr>& indexDocs,
                                   uint32_t docNum, docid_t begDocId, Answer* answer, 
                                   config::PackageIndexConfigPtr& indexConf);

private:
    IE_LOG_DECLARE();
};

typedef std::tr1::shared_ptr<ExpackIndexDocumentMaker> ExPackIndexDocumentMakerPtr;

IE_NAMESPACE_END(index);

#endif //__INDEXLIB_EXPACK_INDEX_DOCUMENT_MAKER_H
