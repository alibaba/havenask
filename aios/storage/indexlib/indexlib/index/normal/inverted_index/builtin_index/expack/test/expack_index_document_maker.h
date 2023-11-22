#pragma once

#include <memory>
#include <vector>

#include "indexlib/common_define.h"
#include "indexlib/config/field_schema.h"
#include "indexlib/config/index_schema.h"
#include "indexlib/config/package_index_config.h"
#include "indexlib/index/test/index_document_maker.h"
#include "indexlib/indexlib.h"

namespace indexlib { namespace index {

class ExpackIndexDocumentMaker : public IndexDocumentMaker
{
public:
    static void MakeIndexDocuments(autil::mem_pool::Pool* pool, std::vector<document::IndexDocumentPtr>& indexDocs,
                                   uint32_t docNum, docid_t begDocId, Answer* answer,
                                   config::PackageIndexConfigPtr& indexConf);

private:
    IE_LOG_DECLARE();
};

typedef std::shared_ptr<ExpackIndexDocumentMaker> ExPackIndexDocumentMakerPtr;
}} // namespace indexlib::index
