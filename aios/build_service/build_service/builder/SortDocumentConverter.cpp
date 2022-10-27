#include "build_service/builder/SortDocumentConverter.h"
#include <autil/HashAlgorithm.h>
using namespace std;
using namespace autil;

IE_NAMESPACE_USE(document);
IE_NAMESPACE_USE(config);
IE_NAMESPACE_USE(index_base);

namespace build_service {
namespace builder {
BS_LOG_SETUP(builder, SortDocumentConverter);

SortDocumentConverter::SortDocumentConverter()
    : _pool(new mem_pool::Pool)
{
}

SortDocumentConverter::~SortDocumentConverter() {
}

}
}
