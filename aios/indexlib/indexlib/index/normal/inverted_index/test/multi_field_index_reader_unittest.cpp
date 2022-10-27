#include "indexlib/index/normal/inverted_index/test/multi_field_index_reader_unittest.h"
#include "indexlib/index/normal/inverted_index/accessor/multi_field_index_segment_reader.h"
#include "indexlib/config/single_field_index_config.h"
#include "indexlib/config/impl/single_field_index_config_impl.h"
#include "indexlib/index/normal/inverted_index/accessor/normal_index_reader.h"
#include <autil/StringUtil.h>

using namespace std;
using namespace autil;


IE_NAMESPACE_USE(config);

IE_NAMESPACE_BEGIN(index);
IE_LOG_SETUP(index, MultiFieldIndexReaderTest);

MultiFieldIndexReaderTest::MultiFieldIndexReaderTest()
{
}

MultiFieldIndexReaderTest::~MultiFieldIndexReaderTest()
{
}

void MultiFieldIndexReaderTest::CaseSetUp()
{
}

void MultiFieldIndexReaderTest::CaseTearDown()
{
}

void MultiFieldIndexReaderTest::TestGetIndexReaderWithIndexId()
{
    SCOPED_TRACE("Failed");

    MultiFieldIndexReaderPtr reader = CreateReader(
            "index1:indexTrunc1,indexTrunc2;index2;index3");

    CheckReaderExist(reader, "index1", 0);
    CheckReaderExist(reader, "index2", 1);
    CheckReaderExist(reader, "index3", 2);
    CheckReaderExist(reader, "indexTrunc1", 3);
    CheckReaderExist(reader, "indexTrunc2", 4);
}

// indexName[:truncName1,truncIndex2];indexName;...
MultiFieldIndexReaderPtr MultiFieldIndexReaderTest::CreateReader(
        const string& readerStr)
{
    MultiFieldIndexReaderPtr multiReader(new MultiFieldIndexReader);
    vector<vector<string> > readerInfos;

    StringUtil::fromString(readerStr, readerInfos, ":", ";");

    indexid_t truncIndexId = (indexid_t)readerInfos.size();
    for (size_t i = 0; i < readerInfos.size(); ++i)
    {
        const string& indexName = readerInfos[i][0];
        IndexConfigPtr indexConfig(new SingleFieldIndexConfig(
                        indexName, it_string));

        indexConfig->SetIndexId((indexid_t)i);
        NormalIndexReaderPtr indexReader(new NormalIndexReader);
        indexReader->SetIndexConfig(indexConfig);
        multiReader->AddIndexReader(indexConfig, indexReader);

        if (readerInfos[i].size() > 1)
        {
            // truncate
            vector<string> truncNames;
            StringUtil::fromString(readerInfos[i][1], truncNames, ",");
            for (size_t j = 0; j < truncNames.size(); ++j)
            {
                IndexConfigPtr truncIndexConfig(indexConfig->Clone());
                truncIndexConfig->SetIndexName(truncNames[j]);                
                truncIndexConfig->SetNonTruncateIndexName(indexName);
                truncIndexConfig->SetIndexId(truncIndexId++);

                NormalIndexReaderPtr truncIndexReader(new NormalIndexReader);
                truncIndexReader->SetIndexConfig(truncIndexConfig);
                multiReader->AddIndexReader(truncIndexConfig, truncIndexReader);
            }
        }
    }
    return multiReader;
}


void MultiFieldIndexReaderTest::CheckReaderExist(
        const MultiFieldIndexReaderPtr& reader,
        const string& indexName, fieldid_t indexId)
{
    IndexReaderPtr reader1 = reader->GetIndexReader(indexName);
    ASSERT_TRUE(reader1);

    IndexReaderPtr reader2 = reader->GetIndexReaderWithIndexId(indexId);
    ASSERT_TRUE(reader2);
    ASSERT_EQ(reader1.get(), reader2.get());
}


IE_NAMESPACE_END(index);

