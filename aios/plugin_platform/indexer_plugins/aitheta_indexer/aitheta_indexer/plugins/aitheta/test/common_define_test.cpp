#include "aitheta_indexer/plugins/aitheta/test/common_define_test.h"

#include <fstream>

#include <aitheta/index_distance.h>
#include <autil/ConstString.h>
#include <autil/mem_pool/Pool.h>
#include <indexlib/config/customized_index_config.h>
#include <indexlib/index/normal/inverted_index/customized_index/index_module_factory.h>
#include <indexlib/index/normal/inverted_index/customized_index/index_reduce_item.h>
#include <indexlib/index/normal/inverted_index/customized_index/index_reducer.h>
#include <indexlib/index/normal/inverted_index/customized_index/index_retriever.h>
#include <indexlib/index/normal/inverted_index/customized_index/indexer.h>
#include <indexlib/index_base/schema_adapter.h>
#include <indexlib/plugin/Module.h>
#include <indexlib/config/module_info.h>
#include <indexlib/test/schema_maker.h>

#include "aitheta_indexer/plugins/aitheta/aitheta_index_reducer.h"
#include "aitheta_indexer/plugins/aitheta/aitheta_index_retriever.h"
#include "aitheta_indexer/plugins/aitheta/common_define.h"
#include "aitheta_indexer/plugins/aitheta/index_segment.h"
#include "aitheta_indexer/plugins/aitheta/util/indexlib_io_wrapper.h"

using namespace std;

IE_NAMESPACE_USE(config);
IE_NAMESPACE_USE(document);
IE_NAMESPACE_USE(plugin);
IE_NAMESPACE_USE(test);
IE_NAMESPACE_USE(index);
IE_NAMESPACE_USE(file_system);
IE_NAMESPACE_USE(common);
IE_NAMESPACE_USE(storage);

IE_NAMESPACE_BEGIN(aitheta_plugin);

void CommonDefineTest::Test() {
    {
        util::KeyValueMap params;
        CommonParam parameters(params);
        EXPECT_EQ("", parameters.mIndexType);
        EXPECT_EQ(INVALID_DIMENSION, parameters.mDim);
        EXPECT_EQ(0U, parameters.mKnnLrThold);
        EXPECT_EQ("&n=", parameters.mTopKFlag);
        EXPECT_EQ("&sf=", parameters.mScoreFilterFlag);
        EXPECT_EQ(';', parameters.mQuerySeparator);
        EXPECT_EQ('#', parameters.mCategoryEmbeddingSeparator);
        EXPECT_EQ(',', parameters.mEmbeddingSeparator);
        EXPECT_EQ(',', parameters.mCategorySeparator);
        EXPECT_EQ(':', parameters.mExtendSeparator);
        EXPECT_EQ(false, parameters.mEnableReportMetric);
    }
    {
        util::KeyValueMap params = {{INDEX_TYPE, "pq"},
                                    {DIMENSION, "71"},
                                    {TOPK_FLAG, "&x="},
                                    {USE_LINEAR_THRESHOLD, "100000"},
                                    {SCORE_FILTER_FLAG, "&scorefilt="},
                                    {ENABLE_REPORT_METRIC, "true"}};

        CommonParam parameters(params);
        EXPECT_EQ(INDEX_TYPE_PQ, parameters.mIndexType);
        EXPECT_EQ(71, parameters.mDim);
        EXPECT_EQ(100000U, parameters.mKnnLrThold);
        EXPECT_EQ("&x=", parameters.mTopKFlag);
        EXPECT_EQ("&scorefilt=", parameters.mScoreFilterFlag);
        EXPECT_EQ(';', parameters.mQuerySeparator);
        EXPECT_EQ('#', parameters.mCategoryEmbeddingSeparator);
        EXPECT_EQ(',', parameters.mEmbeddingSeparator);
        EXPECT_EQ(',', parameters.mCategorySeparator);
        EXPECT_EQ(':', parameters.mExtendSeparator);
        EXPECT_EQ(true, parameters.mEnableReportMetric);
    }
}

IE_NAMESPACE_END(aitheta_plugin);
