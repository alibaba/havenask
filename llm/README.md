# 介绍
本项目将Havenask与大模型结合，Havenask作为向量数据库，实现基于文档的问答系统。可以使用ChatGPT服务，也可以全部基于开源方案（ChatGLM-6b）实现本地的私有部署。

流程图如下：

![流程图](/llm/resources/flow.jpg)

# 开始
## 环境要求
* CPU 16核, 64G内存
* 需要安装docker环境

## 准备容器
* 克隆havenask仓库
```shell
git clone https://github.com/alibaba/havenask.git
```

* 创建容器
```shell
cd ~/havenask/docker
./create_container.sh havenask registry.cn-hangzhou.aliyuncs.com/havenask/ha3_runtime:0.3.0
```

* 登陆容器
```shell
./havenask/sshme
```

详情请参考[havenask](https://github.com/alibaba/havenask#%E5%BC%80%E5%A7%8B%E4%BD%BF%E7%94%A8)

## 安装依赖
使用[conda](https://conda.io/projects/conda/en/stable/user-guide/install/linux.html)管理python环境。下载linux环境的```Anaconda```，按提示步骤完成安装。

```shell
bash Anaconda3-2023.03-1-Linux-x86_64.sh
```

安装完conda后， 执行下面的命令创建并激活python环境
```shell
source ~/.bashrc
conda create --name havenask_llm python=3.10
conda activate havenask_llm
```

安装python依赖
```shell
cd ~/havenask/llm  # 当前目录为/home/$USER
pip install -r requirements.txt
```

## 配置文件
在 ```.env```文件中配置大模型相关信息

* 使用ChatGPT
```conf
LLM_NAME=OpenAI

OPENAI_API_KEY=***
OPENAI_API_BASE=***

OPENAI_EMBEDDING_ENGINE=text-embedding-ada-002
OPENAI_CHAT_ENGINE=gpt-3.5-turbo

HA_QRS_ADDRESS=127.0.0.1:45800
HA_TABLE_NAME=llm
```

* 使用微软azure OpenAI
```conf
LLM_NAME=OpenAI

OPENAI_API_KEY=***
OPENAI_API_BASE=***
OPENAI_API_VERSION=2023-03-15-preview
OPENAI_API_TYPE=azure

# 填写azure中OpenAI模型的deployment
OPENAI_EMBEDDING_ENGINE=embedding_deployment_id
OPENAI_CHAT_ENGINE=chat_deployment_id

# Havenask信息
HA_QRS_ADDRESS=127.0.0.1:45800
HA_TABLE_NAME=llm
```

* 使用ChatGLM-6b
```conf
LLM_NAME=ChatGLM

HA_QRS_ADDRESS=127.0.0.1:45800
HA_TABLE_NAME=llm

# 模型服务地址, 可选
#CHATGLM_SERVER_ENDPOINT=http://127.0.0.1:8001/
#EMBEDDING_SERVER_ENDPOINT=http://127.0.0.1:8008/

# 开源模型名称，如果服务地址为空，则本地加载模型
EMBEDDING_MODEL=GanymedeNil/text2vec-large-chinese
CHATGLM_MODEL=THUDM/chatglm-6b
```

## 构建知识库
使用[embed_files.py](script/embed_files.py)脚本对数据文件进行处理，目前支持```.md```和```.pdf```格式的文件。处理完成后会生成一个Havenask引擎格式的数据文件。
例如处理Havenask wiki数据，忽略```.git```和```english```目录下的文件。
```shell
git clone https://github.com/alibaba/havenask.wiki.git
python -m script.embed_files havenask.wiki ./llm.data .git,english
```

如果从```Huggingface```自动下载模型失败，参考[从本地加载模型](README.md#本地加载模型)，手动下载模型。

### 构建索引
将上一步生成的数据文件```llm.data```拷贝到```~/havenask/example/data/llm.data```，构建索引。
```shell
cp ./llm.data ~/havenask/example/data/llm.data 
cd ~/havenask/example/
/usr/bin/python2.7 build_demo_data.py /ha3_install llm
```

* Havenask服务目前需要python2.7启动任务

### 启动服务
索引构建结束后，启动havenask服务，根据需要可以修改端口号。并将```.env```中配置的QRS端口一并修改。
```shell
cd ~/havenask/example/
/usr/bin/python2.7 start_demo_searcher.py /ha3_install llm 45800
```

## 启动问答服务
### api部署
```shell
cd ~/havenask/llm
python api.py
```
测试请求
```shell
curl -H "Content-Type: application/json" http://127.0.0.1:8000/chat -d '{"query": "havenask是什么"}'
```
输出:
```json
{
    "success":true,
    "result":"havenask 是一个分布式数据库管理系统，由阿里巴巴开发。它支持 SQL 查询语句，并提供了内置的 UDF(User Defined Function)功能，允许客户以插件形式开发自己的 UDF。havenask 可以用于存储、处理和分析大规模数据，具有高可用性、可扩展性和高性能的特点。havenask 可以通过分布式运维工具进行部署，支持在物理机上拉起分布式集群，也可以在云平台上使用云原生架构进行部署。",
    "history":[
        [
            "已知信息：\n                Content: NAME> havenask/ha3_runtime:0.3.0\n```\n\n* 登陆容器\n```\n./<CONTAINER_NAME>/sshme\n```\n\n* 运行测试 \n启动运行时容器后，构建测试数据索引以及查询引擎的方法见example\n\n## 集群模式\n集群模式需要通过Havenask的分布式运维工具部署，详细信息请参考Havenask分布式运维工具\n# 编译代码\n\n## 编译环境\n* 请确保编译的机器内存在15G以上，mac编译时需调整Docker容器资源上限（包括CPU、Memory、Swap等），具体路径：Docker Desktop->setting->Resources。\n* 请确保cpu位8core以上，不然编译比较慢。\n\n## 获取开发镜像\n\n```\ndocker pull havenask/ha3_dev:0.3.0\n```\n## 下载代码\n```\ncd ~\ngit clone git@github.com:alibaba/havenask.git\n```\n\n## 启动容器\n```\ncd ~/havenask/docker\n./\nContent: ### 简介\n\nhavenask SQL支持在查询语句中使用内置的UDF（User Defined Function)，也允许客户以插件形式开发自己的UDF。\nContent: # 介绍\nHavenask PE tools，简称hape，是havenask的简易分布式运维工具。用户可以利用hape在单台或者多台物理机上拉起分布式havenask集群，并进行基础的运维操作。\n\n# 名词解释\n* domain\n * 一个havenask集群被称为一个domain，一个hape工具可以管理多个domain\n* config\n * hape config：关于havenask集群部署计划的配置文件，一个domain有一个hape config\n * biz config：havenask引擎本身加载的配置，分为离线配置offline config和在线配置online config\n* hape cmd\n * hape命令行工具，负责向domain daemon提交用户请求\n* hape domain daemon\n * 每当用户使用hape cmd创建一个domain，hape cmd都会在所在机器后台启动一个domain daemon进程用于专门管理该domain。所有hape cmd命令都会在后台被domain daemon异步处理\n* hape worker & worker daemon\n * hape worker\n \n\n                根据上述已知信息，简洁和专业的来回答用户的问题。如果无法从中得到答案，请说不知道，不允许在答案中添加编造成分，答案请使用中文。\n                问题是：havenask是什么",
            "havenask 是一个分布式数据库管理系统，由阿里巴巴开发。它支持 SQL 查询语句，并提供了内置的 UDF(User Defined Function)功能，允许客户以插件形式开发自己的 UDF。havenask 可以用于存储、处理和分析大规模数据，具有高可用性、可扩展性和高性能的特点。havenask 可以通过分布式运维工具进行部署，支持在物理机上拉起分布式集群，也可以在云平台上使用云原生架构进行部署。"
        ]
    ],
    "source":["快速开始.md","UDF.md","Havenask分布式运维工具.md"],
    "finished":true
}
```

### cli方式
```shell
cd ~/havenask/llm
python cli_demo.py
```
输入exit退出，输入clear会清空history，输入其他内容进行问答。
![cli-demo](/llm/resources/clidemo.jpg)

### webdemo
```shell
cd ~/havenask/llm
python webdemo.py
```
在浏览器中打开返回的web地址，方便使用
![cli-demo](/llm/resources/webdemo.jpg)

# FAQ
## 远程部署模型
ChatGLM-6b部署所需要的资源可以参考[官方文档](https://github.com/THUDM/ChatGLM-6B#%E4%BD%BF%E7%94%A8%E6%96%B9%E5%BC%8F)

如果本地机器资源不足，可以选择远程部署模型，如在带GPU的ECS上。部署脚本参考[deploy_embedding.py](script/deploy_embedding.py)和[deploy_chatglm.py](script/deploy_chatglm.py)。脚本会从```huggingface```自动下载模型文件。部署脚本依赖```torch```和```transformers```。

* 部署向量化模型
```shell
python script/deploy_embedding.py
```
默认端口8001

* 部署ChatGLM-6b
```shell
python script/deploy_chatglm.py
```
默认端口8008

* 修改```.env```中的服务地址
```conf
CHATGLM_SERVER_ENDPOINT=http://{ip}:8001/
EMBEDDING_SERVER_ENDPOINT=http://{ip}:8008/
```

## 更换模型

本项目默认使用大模型[ChatGLM6b](https://huggingface.co/THUDM/chatglm-6b)与向量化模型[GanymedeNil/text2vec-large-chinese](https://huggingface.co/GanymedeNil/text2vec-large-chinese)

更换模型需要修改```.env```文件中的模型名称，并在llm_adapter中增加加载新模型的adapter，可以参考[local_chatglm.py](llm_adapter/local_chatglm.py)，远程部署参考[deploy_chatglm.py](script/deploy_chatglm.py)。

因为不同的文本向量化模型生成的embedding维度不同，因此需要修改havenask配置中的维度信息，修改以下两个文件中```dimension```的值：
```
example/cases/llm/config/online_config/bizs/0/schemas/llm_schema.json
example/cases/llm/config/offline_config/0/schemas/llm_schema.json
```

## 本地加载模型
代码会使用```transformers```自动从huggingface下载模型。如果网络环境比较差，下载模型很慢或者经常超时。可以手动下载模型到本地目录，然后从本地加载模型。

* 安装 [Git LFS](https://docs.github.com/zh/repositories/working-with-files/managing-large-files/installing-git-large-file-storage)
* 从Huggingface下载模型至本地:
```shell
cd ~/havenask
git clone https://huggingface.co/THUDM/chatglm-6b
git clone https://huggingface.co/GanymedeNil/text2vec-large-chinese
```
* 将```.env```中的模型名称换成本地的```绝对路径```，如：
```
EMBEDDING_MODEL=/home/{USER}/havenask/text2vec-large-chinese
CHATGLM_MODEL=/home/{USER}/havenask/chatglm-6b
```
将USER替换成实际的用户名称，这里需要模型目录的绝对路径。

## 更换文件内容切片方式
默认的切片方式为按token数加overlap的方式进行切片，参考[sentence_spliter.py](spliter/sentence_spliter.py)实现新的切片方式

# 特别提醒：
本解决方案中的“开源向量模型”、“大模型（如chatgpt）”等来自第三方（合称“第三方模型”）。阿里巴巴无法保证第三方模型合规、准确，也不对第三方模型本身、及您使用第三方模型的行为和结果等承担任何责任。请您在访问和使用前慎重考虑。

此外我们也提醒您，第三方模型附有“开源许可证”、“License”等协议，您应当仔细阅读并严格遵守这些协议的约定。
