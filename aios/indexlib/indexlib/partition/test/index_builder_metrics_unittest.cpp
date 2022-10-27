#include <autil/StringUtil.h>
#include "indexlib/partition/test/index_builder_metrics_unittest.h"
#include "indexlib/document/index_document/normal_document/normal_document.h"

using namespace std;
using namespace autil;
IE_NAMESPACE_USE(document);

IE_NAMESPACE_BEGIN(partition);
IE_LOG_SETUP(partition, IndexBuilderMetricsTest);

IndexBuilderMetricsTest::IndexBuilderMetricsTest()
{
}

IndexBuilderMetricsTest::~IndexBuilderMetricsTest()
{
}

void IndexBuilderMetricsTest::CaseSetUp()
{
}

void IndexBuilderMetricsTest::CaseTearDown()
{
}

void IndexBuilderMetricsTest::TestAddToUpdate()
{
    IndexBuilderMetrics metrics;

    NormalDocumentPtr doc(new NormalDocument);
    doc->SetDocOperateType(ADD_DOC);
    doc->ModifyDocOperateType(UPDATE_FIELD);
    metrics.ReportMetrics(1, doc);

    ASSERT_EQ((uint32_t)1, metrics.GetSuccessDocCount());
    ASSERT_EQ((uint32_t)1, metrics.mAddToUpdateDocCount);

    metrics.ReportMetrics(0, doc);

    ASSERT_EQ((uint32_t)1, metrics.GetSuccessDocCount());
    ASSERT_EQ((uint32_t)1, metrics.mAddToUpdateDocCount);
}

IE_NAMESPACE_END(partition);

