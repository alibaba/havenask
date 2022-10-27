#ifndef ISEARCH_BS_INDEXBUILDERCREATOR_H
#define ISEARCH_BS_INDEXBUILDERCREATOR_H

#include "build_service/common_define.h"
#include "build_service/util/Log.h"
#include "build_service/builder/test/MockIndexBuilder.h"

namespace build_service {
namespace builder {

class IndexBuilderCreator
{
public:
    IndexBuilderCreator();
    ~IndexBuilderCreator();
private:
    IndexBuilderCreator(const IndexBuilderCreator &);
    IndexBuilderCreator& operator=(const IndexBuilderCreator &);

public:
    static IE_NAMESPACE(index)::MockIndexBuilderPtr CreateMockIndexBuilder(
            const std::string& rootDir);
    static IE_NAMESPACE(partition)::IndexBuilderPtr CreateIndexBuilder(
            const std::string& rootDir,
            const IE_NAMESPACE(config)::IndexPartitionSchemaPtr& schema);
public:

private:
    BS_LOG_DECLARE();
};

BS_TYPEDEF_PTR(IndexBuilderCreator);

}
}

#endif //ISEARCH_BS_INDEXBUILDERCREATOR_H
