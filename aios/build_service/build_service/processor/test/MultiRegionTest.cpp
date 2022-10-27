#include "build_service/test/unittest.h"
#include "build_service/processor/Processor.h"
#include "build_service/processor/ProcessorWorkItem.h"
#include "build_service/processor/MultiThreadProcessorWorkItemExecutor.h"
#include "build_service/processor/SingleThreadProcessorWorkItemExecutor.h"
#include "build_service/processor/SingleDocProcessorChain.h"
#include "build_service/processor/DocumentProcessor.h"
#include "build_service/processor/RegionDocumentProcessor.h"
#include "build_service/processor/test/ProcessorTestUtil.h"
#include "build_service/document/RawDocument.h"

#include <autil/TimeUtility.h>
#include <autil/AtomicCounter.h>
#include <indexlib/util/counter/counter_map.h>
#include <indexlib/test/partition_state_machine.h>
#include <indexlib/index_base/schema_adapter.h>
#include <indexlib/document/index_document/normal_document/normal_document.h>

using namespace std;
using namespace autil;
using namespace build_service::config;
using namespace build_service::document;

IE_NAMESPACE_USE(index_base);
IE_NAMESPACE_USE(config);
IE_NAMESPACE_USE(test);


using namespace std;
using namespace testing;

namespace build_service {
namespace processor {

class MultiRegionTest : public BUILD_SERVICE_TESTBASE {
public:
    void setUp();
    void tearDown();

private:
    vector<IE_NAMESPACE(document)::DocumentPtr> createDocuments(
            Processor& processor, const string& docStr);
private:
    proto::PartitionId _partitionId;
    IE_NAMESPACE(util)::CounterMapPtr _counterMap;
};

void MultiRegionTest::setUp() {
}

void MultiRegionTest::tearDown() {
}

vector<IE_NAMESPACE(document)::DocumentPtr> MultiRegionTest::createDocuments(
        Processor& processor, const string& docStr)
{
    vector<document::RawDocumentPtr> rawDocs = ProcessorTestUtil::createRawDocuments(docStr, "#");
    vector<IE_NAMESPACE(document)::DocumentPtr> indexDocs;
    for (const auto& rawDoc : rawDocs) {
        processor.processDoc(rawDoc);
        ProcessedDocumentVecPtr processedDocVec = processor.getProcessedDoc();
        for (size_t i = 0; i < processedDocVec->size(); ++i)
        {
            indexDocs.push_back((*processedDocVec)[i]->getDocument());
        }
    }
    return indexDocs;
}

}
}
