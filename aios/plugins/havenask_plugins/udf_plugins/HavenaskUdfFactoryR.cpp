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
#include "havenask_plugins/udf_plugins/HavenaskUdfFactoryR.h"

BEGIN_HAVENASK_UDF_NAMESPACE(factory)

const std::string HavenaskUdfFactoryR::RESOURCE_ID =
    "havenask_udf_factory_r";

HavenaskUdfFactoryR::HavenaskUdfFactoryR() {}
HavenaskUdfFactoryR::~HavenaskUdfFactoryR() {}

void HavenaskUdfFactoryR::def(navi::ResourceDefBuilder &builder) const {
    builder.name(RESOURCE_ID, navi::RS_BIZ)
        .dependByResource(suez::turing::FunctionInterfaceCreatorR::RESOURCE_ID,
                          suez::turing::FunctionInterfaceCreatorR::DYNAMIC_GROUP);
}


REGISTER_RESOURCE(HavenaskUdfFactoryR);

END_HAVENASK_UDF_NAMESPACE(factory)
