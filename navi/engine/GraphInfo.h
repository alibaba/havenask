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
#ifndef NAVI_GRAPHINFO_H
#define NAVI_GRAPHINFO_H

#include "navi/config/NaviConfig.h"
#include "navi/proto/GraphDef.pb.h"

namespace navi {

class GraphInfo
{
public:
    GraphInfo(const std::string &configPath, const GraphConfig &config);
    ~GraphInfo();
private:
    GraphInfo(const GraphInfo &);
    GraphInfo &operator=(const GraphInfo &);
public:
    bool init();
    const GraphDef &getGraphDef() const;
private:
    bool loadGraphDef();
private:
    std::string _configPath;
    GraphConfig _config;
    GraphDef _graphDef;
};

}

#endif //NAVI_GRAPHINFO_H
