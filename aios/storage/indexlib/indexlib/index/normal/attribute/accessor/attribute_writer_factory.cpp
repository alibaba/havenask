/*
 * Copyright 2014-present Alibaba Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
#include "indexlib/index/normal/attribute/accessor/attribute_writer_factory.h"

#include <sstream>

#include "indexlib/common/field_format/attribute/attribute_convertor_factory.h"
#include "indexlib/config/attribute_config.h"
#include "indexlib/index/normal/attribute/accessor/attribute_fs_writer_param_decider.h"
#include "indexlib/index/normal/attribute/accessor/date_attribute_writer.h"
#include "indexlib/index/normal/attribute/accessor/float_attribute_writer_creator.h"
#include "indexlib/index/normal/attribute/accessor/line_attribute_writer.h"
#include "indexlib/index/normal/attribute/accessor/location_attribute_writer.h"
#include "indexlib/index/normal/attribute/accessor/multi_string_attribute_writer.h"
#include "indexlib/index/normal/attribute/accessor/polygon_attribute_writer.h"
#include "indexlib/index/normal/attribute/accessor/single_value_attribute_writer.h"
#include "indexlib/index/normal/attribute/accessor/string_attribute_writer.h"
#include "indexlib/index/normal/attribute/accessor/time_attribute_writer.h"
#include "indexlib/index/normal/attribute/accessor/timestamp_attribute_writer.h"
#include "indexlib/index/normal/attribute/accessor/var_num_attribute_writer.h"
#include "indexlib/util/Exception.h"

using namespace std;
using namespace autil;
using namespace autil::mem_pool;
using namespace autil::legacy;

using namespace indexlib::common;
using namespace indexlib::util;
using namespace indexlib::config;

namespace indexlib { namespace index {

AttributeWriterFactory::AttributeWriterFactory() { Init(); }

AttributeWriterFactory::~AttributeWriterFactory() {}

void AttributeWriterFactory::Init()
{
    RegisterCreator(AttributeWriterCreatorPtr(new Int16AttributeWriter::Creator()));
    RegisterCreator(AttributeWriterCreatorPtr(new UInt16AttributeWriter::Creator()));
    RegisterCreator(AttributeWriterCreatorPtr(new Int8AttributeWriter::Creator()));
    RegisterCreator(AttributeWriterCreatorPtr(new UInt8AttributeWriter::Creator()));
    RegisterCreator(AttributeWriterCreatorPtr(new Int32AttributeWriter::Creator()));
    RegisterCreator(AttributeWriterCreatorPtr(new UInt32AttributeWriter::Creator()));
    RegisterCreator(AttributeWriterCreatorPtr(new Int64AttributeWriter::Creator()));
    RegisterCreator(AttributeWriterCreatorPtr(new UInt64AttributeWriter::Creator()));
    RegisterCreator(AttributeWriterCreatorPtr(new Hash64AttributeWriter::Creator()));
    RegisterCreator(AttributeWriterCreatorPtr(new Hash128AttributeWriter::Creator()));
    RegisterCreator(AttributeWriterCreatorPtr(new FloatAttributeWriter::Creator()));
    RegisterCreator(AttributeWriterCreatorPtr(new FloatFp16AttributeWriterCreator()));
    RegisterCreator(AttributeWriterCreatorPtr(new FloatInt8AttributeWriterCreator()));
    RegisterCreator(AttributeWriterCreatorPtr(new DoubleAttributeWriter::Creator()));
    RegisterCreator(AttributeWriterCreatorPtr(new TimeAttributeWriter::Creator()));
    RegisterCreator(AttributeWriterCreatorPtr(new TimestampAttributeWriter::Creator()));
    RegisterCreator(AttributeWriterCreatorPtr(new DateAttributeWriter::Creator()));

    RegisterCreator(AttributeWriterCreatorPtr(new StringAttributeWriter::Creator()));
    RegisterMultiValueCreator(AttributeWriterCreatorPtr(new Int8MultiValueAttributeWriter::Creator()));
    RegisterMultiValueCreator(AttributeWriterCreatorPtr(new UInt8MultiValueAttributeWriter::Creator()));
    RegisterMultiValueCreator(AttributeWriterCreatorPtr(new Int16MultiValueAttributeWriter::Creator()));
    RegisterMultiValueCreator(AttributeWriterCreatorPtr(new UInt16MultiValueAttributeWriter::Creator()));
    RegisterMultiValueCreator(AttributeWriterCreatorPtr(new Int32MultiValueAttributeWriter::Creator()));
    RegisterMultiValueCreator(AttributeWriterCreatorPtr(new UInt32MultiValueAttributeWriter::Creator()));
    RegisterMultiValueCreator(AttributeWriterCreatorPtr(new Int64MultiValueAttributeWriter::Creator()));
    RegisterMultiValueCreator(AttributeWriterCreatorPtr(new UInt64MultiValueAttributeWriter::Creator()));
    RegisterMultiValueCreator(AttributeWriterCreatorPtr(new FloatMultiValueAttributeWriter::Creator()));
    RegisterMultiValueCreator(AttributeWriterCreatorPtr(new DoubleMultiValueAttributeWriter::Creator()));
    RegisterMultiValueCreator(AttributeWriterCreatorPtr(new MultiStringAttributeWriter::Creator()));
    RegisterMultiValueCreator(AttributeWriterCreatorPtr(new LocationAttributeWriter::Creator()));
    RegisterMultiValueCreator(AttributeWriterCreatorPtr(new LineAttributeWriter::Creator()));
    RegisterMultiValueCreator(AttributeWriterCreatorPtr(new PolygonAttributeWriter::Creator()));
}

void AttributeWriterFactory::RegisterCreator(AttributeWriterCreatorPtr creator)
{
    assert(creator != NULL);

    autil::ScopedLock l(mLock);
    FieldType type = creator->GetAttributeType();
    CreatorMap::iterator it = mWriterCreators.find(type);
    if (it != mWriterCreators.end()) {
        mWriterCreators.erase(it);
    }
    mWriterCreators.insert(make_pair(type, creator));
}

void AttributeWriterFactory::RegisterMultiValueCreator(AttributeWriterCreatorPtr creator)
{
    assert(creator != NULL);

    autil::ScopedLock l(mLock);
    FieldType type = creator->GetAttributeType();
    CreatorMap::iterator it = mMultiValueWriterCreators.find(type);
    if (it != mMultiValueWriterCreators.end()) {
        mMultiValueWriterCreators.erase(it);
    }
    mMultiValueWriterCreators.insert(make_pair(type, creator));
}

AttributeWriter* AttributeWriterFactory::CreateAttributeWriter(const AttributeConfigPtr& attrConfig,
                                                               const IndexPartitionOptions& options,
                                                               BuildResourceMetrics* buildResourceMetrics)
{
    autil::ScopedLock l(mLock);
    AttributeWriter* writer = NULL;
    bool isMultiValue = attrConfig->IsMultiValue();
    FieldType fieldType = attrConfig->GetFieldType();
    if (!isMultiValue) {
        CreatorMap::const_iterator it = mWriterCreators.find(fieldType);
        if (it != mWriterCreators.end()) {
            writer = it->second->Create(attrConfig);
        } else {
            stringstream s;
            s << "Attribute writer type: " << fieldType << " not implemented yet!";
            INDEXLIB_THROW(util::UnImplementException, "%s", s.str().c_str());
            return NULL;
        }
    } else {
        CreatorMap::const_iterator it = mMultiValueWriterCreators.find(fieldType);
        if (it != mMultiValueWriterCreators.end()) {
            writer = it->second->Create(attrConfig);
        } else {
            stringstream s;
            s << "MultiValue Attribute writer type: " << fieldType << " not implemented yet!";
            INDEXLIB_THROW(util::UnImplementException, "%s", s.str().c_str());
            return NULL;
        }
    }
    AttributeConvertorPtr attrConvertor(AttributeConvertorFactory::GetInstance()->CreateAttrConvertor(attrConfig));
    assert(attrConvertor);
    assert(writer);
    writer->SetAttrConvertor(attrConvertor);

    FSWriterParamDeciderPtr writerParamDecider(new AttributeFSWriterParamDecider(attrConfig, options));
    writer->Init(writerParamDecider, buildResourceMetrics);
    return writer;
}

PackAttributeWriter* AttributeWriterFactory::CreatePackAttributeWriter(const PackAttributeConfigPtr& packAttrConfig,
                                                                       const IndexPartitionOptions& options,
                                                                       BuildResourceMetrics* buildResourceMetrics)
{
    PackAttributeWriter* packAttrWriter = new PackAttributeWriter(packAttrConfig);

    FSWriterParamDeciderPtr writerParamDecider(
        new AttributeFSWriterParamDecider(packAttrConfig->CreateAttributeConfig(), options));
    packAttrWriter->Init(writerParamDecider, buildResourceMetrics);
    return packAttrWriter;
}
}} // namespace indexlib::index
