#include "build_service/workflow/test/FakeBuilder.h"
#include <indexlib/partition/index_builder.h>

using namespace std;
using namespace autil;
using namespace build_service::builder;
IE_NAMESPACE_USE(index_base);
IE_NAMESPACE_USE(document);
IE_NAMESPACE_USE(partition);

namespace build_service {
namespace workflow {
BS_LOG_SETUP(workflow, FakeBuilder);

FakeBuilder::FakeBuilder()
    : Builder(IndexBuilderPtr())
    , _buildCount(0)
{
}

FakeBuilder::~FakeBuilder() {
}

bool FakeBuilder::build(const DocumentPtr &doc) {
    assert(doc);
    const ConstString& locatorStr = doc->GetLocator().GetLocator();
    assert(!locatorStr.empty());
    _lastLocator.fromString(locatorStr);
    _buildCount++;
    return true;
}

}
}
