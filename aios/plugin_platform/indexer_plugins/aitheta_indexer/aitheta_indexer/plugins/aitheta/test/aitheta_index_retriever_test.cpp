#include "aitheta_indexer/plugins/aitheta/test/aitheta_index_retriever_test.h"

using namespace std;
IE_NAMESPACE_USE(file_system);

IE_NAMESPACE_BEGIN(aitheta_plugin);

void AithetaIndexRetrieverTest::TestInit() {
    AithetaIndexRetriever retriever({});
    ASSERT_TRUE(retriever.Init(IE_NAMESPACE(index)::DeletionMapReaderPtr(),{}, {}));
}

IE_NAMESPACE_END(aitheta_plugin);
