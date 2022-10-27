#include "indexlib/index/normal/attribute/accessor/pack_attribute_writer.h"
#include "indexlib/common/field_format/attribute/attribute_convertor_factory.h"
#include "indexlib/index/normal/attribute/accessor/in_mem_var_num_attribute_reader.h"

using namespace std;
using namespace autil::mem_pool;
using namespace autil;

IE_NAMESPACE_USE(common);
IE_NAMESPACE_USE(config);
IE_NAMESPACE_USE(util);

IE_NAMESPACE_BEGIN(index);
IE_LOG_SETUP(index, PackAttributeWriter);

PackAttributeWriter::PackAttributeWriter(
        const PackAttributeConfigPtr& packAttrConfig)
    : StringAttributeWriter(packAttrConfig->CreateAttributeConfig())
    , mPackAttrConfig(packAttrConfig)
    , mBuffer(PACK_ATTR_BUFF_INIT_SIZE, &mSimplePool)
{
    mAttrConvertor.reset(
        AttributeConvertorFactory::GetInstance()
        ->CreatePackAttrConvertor(packAttrConfig));
    mPackAttrFormatter.reset(new PackAttributeFormatter);
    mPackAttrFormatter->Init(mPackAttrConfig);
}


PackAttributeWriter::~PackAttributeWriter()
{}

bool PackAttributeWriter::UpdateEncodeFields(
    docid_t docId, const PackAttributeFields& packAttrFields)
{
    assert(mAccessor);

    AttributeConfigPtr dataConfig = mPackAttrConfig->CreateAttributeConfig();
    InMemVarNumAttributeReader<char> inMemReader(mAccessor,
            dataConfig->GetCompressType(),
            dataConfig->GetFieldConfig()->GetFixedMultiValueCount());

    MultiChar packValue;
    if (!inMemReader.Read(docId, packValue))
    {
        IE_LOG(ERROR, "read doc [%d] fail!", docId);
        ERROR_COLLECTOR_LOG(ERROR, "read doc [%d] fail!", docId);
        return false;
    }

    ConstString mergeValue =
        mPackAttrFormatter->MergeAndFormatUpdateFields(
            packValue.data(), packAttrFields, true, mBuffer);
    if (mergeValue.empty())
    {
        IE_LOG(ERROR, "MergeAndFormat pack attribute fields"
               " for doc [%d] fail!", docId);
        ERROR_COLLECTOR_LOG(ERROR, "MergeAndFormat pack attribute fields"
               " for doc [%d] fail!", docId);
        return false;
    }
    return UpdateField(docId, mergeValue);
}

IE_NAMESPACE_END(index);

