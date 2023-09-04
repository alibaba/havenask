#include "indexlib/common_define.h"
#include "indexlib/config/single_field_index_config.h"
#include "indexlib/index/normal/adaptive_bitmap/adaptive_bitmap_index_writer.h"
#include "indexlib/index/normal/adaptive_bitmap/adaptive_bitmap_index_writer_creator.h"
#include "indexlib/index/normal/adaptive_bitmap/df_adaptive_bitmap_trigger.h"
#include "indexlib/index/normal/adaptive_bitmap/multi_adaptive_bitmap_index_writer.h"
#include "indexlib/test/directory_creator.h"
#include "indexlib/test/unittest.h"
#include "indexlib/util/PathUtil.h"

using namespace std;

using namespace indexlib::util;
using namespace indexlib::file_system;
using namespace indexlib::file_system;

namespace indexlib { namespace index { namespace legacy {

#define CheckWriter(Creator, InvertedIndexType, WriterType)                                                            \
    {                                                                                                                  \
        config::IndexConfigPtr indexConf(new config::SingleFieldIndexConfig("phrase", InvertedIndexType));             \
        config::MergeIOConfig ioConfig;                                                                                \
        MultiAdaptiveBitmapIndexWriterPtr multiWriter = Creator.Create(indexConf, ioConfig);                           \
        auto writer = multiWriter->GetSingleAdaptiveBitmapWriter(0);                                                   \
        INDEXLIB_TEST_TRUE(writer.get() != NULL);                                                                      \
        std::shared_ptr<WriterType> subWriter = DYNAMIC_POINTER_CAST(WriterType, writer);                              \
        INDEXLIB_TEST_TRUE(subWriter.get() != NULL);                                                                   \
    }

class AdaptiveBitmapIndexWriterCreatorMock : public AdaptiveBitmapIndexWriterCreator
{
public:
    AdaptiveBitmapIndexWriterCreatorMock(const ArchiveFolderPtr& mergeFolder, const ReclaimMapPtr& reclaimMap,
                                         const index_base::OutputSegmentMergeInfos& outputSegmentMergeInfos)
        : AdaptiveBitmapIndexWriterCreator(mergeFolder, reclaimMap, outputSegmentMergeInfos)
    {
    }
    ~AdaptiveBitmapIndexWriterCreatorMock() {}

private:
    AdaptiveBitmapTriggerPtr CreateAdaptiveBitmapTrigger(const config::IndexConfigPtr& indexConf) override
    {
        AdaptiveBitmapTriggerPtr trigger(new DfAdaptiveBitmapTrigger(10));
        return trigger;
    }
};

class AdaptiveBitmapIndexWriterCreatorTest : public INDEXLIB_TESTBASE
{
public:
    DECLARE_CLASS_NAME(AdaptiveBitmapIndexWriterCreatorTest);
    void CaseSetUp() override { mRootDir = GET_TEMP_DATA_PATH(); }

    void CaseTearDown() override {}

    void TestCaseForCreate()
    {
        ReclaimMapPtr reclaimMap(new ReclaimMap);
        index_base::OutputSegmentMergeInfo outputSegmentMergeInfo;
        outputSegmentMergeInfo.targetSegmentId = 100;
        outputSegmentMergeInfo.targetSegmentIndex = 0;
        outputSegmentMergeInfo.path = mRootDir;
        outputSegmentMergeInfo.directory = GET_PARTITION_DIRECTORY();

        ArchiveFolderPtr rootFolder(new ArchiveFolder(false));
        ASSERT_EQ(FSEC_OK, rootFolder->Open(GET_PARTITION_DIRECTORY()->GetIDirectory()));
        AdaptiveBitmapIndexWriterCreatorMock creator(rootFolder, reclaimMap, {outputSegmentMergeInfo});
        CheckWriter(creator, it_number_int8, AdaptiveBitmapIndexWriter);
        CheckWriter(creator, it_number_int16, AdaptiveBitmapIndexWriter);
        CheckWriter(creator, it_number_int32, AdaptiveBitmapIndexWriter);
        CheckWriter(creator, it_number_int64, AdaptiveBitmapIndexWriter);
        CheckWriter(creator, it_number_uint8, AdaptiveBitmapIndexWriter);
        CheckWriter(creator, it_number_uint16, AdaptiveBitmapIndexWriter);
        CheckWriter(creator, it_number_uint32, AdaptiveBitmapIndexWriter);
        CheckWriter(creator, it_number_uint64, AdaptiveBitmapIndexWriter);
        CheckWriter(creator, it_text, AdaptiveBitmapIndexWriter);
        CheckWriter(creator, it_pack, AdaptiveBitmapIndexWriter);
        CheckWriter(creator, it_expack, AdaptiveBitmapIndexWriter);
        CheckWriter(creator, it_string, AdaptiveBitmapIndexWriter);
    }

private:
    string mRootDir;

private:
    IE_LOG_DECLARE();
};

IE_LOG_SETUP(index, AdaptiveBitmapIndexWriterCreatorTest);

INDEXLIB_UNIT_TEST_CASE(AdaptiveBitmapIndexWriterCreatorTest, TestCaseForCreate);
}}} // namespace indexlib::index::legacy
