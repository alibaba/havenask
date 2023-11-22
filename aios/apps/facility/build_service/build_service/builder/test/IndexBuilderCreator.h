#pragma once

#include <string>

#include "build_service/builder/test/MockIndexBuilder.h"
#include "build_service/common_define.h"
#include "build_service/util/Log.h"
#include "indexlib/partition/index_builder.h"

namespace build_service { namespace builder {

class IndexBuilderCreator
{
public:
    IndexBuilderCreator();
    ~IndexBuilderCreator();

private:
    IndexBuilderCreator(const IndexBuilderCreator&);
    IndexBuilderCreator& operator=(const IndexBuilderCreator&);

public:
    static indexlib::index::MockIndexBuilderPtr CreateMockIndexBuilder(const std::string& rootDir);
    static indexlib::partition::IndexBuilderPtr
    CreateIndexBuilder(const std::string& rootDir, const indexlib::config::IndexPartitionSchemaPtr& schema);

public:
private:
    BS_LOG_DECLARE();
};

BS_TYPEDEF_PTR(IndexBuilderCreator);

}} // namespace build_service::builder
