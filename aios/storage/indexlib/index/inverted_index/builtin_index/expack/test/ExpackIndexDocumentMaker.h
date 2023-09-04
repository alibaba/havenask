#pragma once

#include <memory>
#include <vector>

#include "autil/Log.h"
#include "autil/mem_pool/Pool.h"
#include "indexlib/index/inverted_index/test/InvertedTestHelper.h"

namespace indexlib::index {

class ExpackIndexDocumentMaker
{
public:
    static void MakeIndexDocuments(autil::mem_pool::Pool* pool,
                                   std::vector<std::shared_ptr<indexlib::document::IndexDocument>>& indexDocs,
                                   uint32_t docNum, docid_t begDocId, InvertedTestHelper::Answer* answer,
                                   std::shared_ptr<indexlibv2::config::PackageIndexConfig>& indexConf);

private:
    AUTIL_LOG_DECLARE();
};

} // namespace indexlib::index
