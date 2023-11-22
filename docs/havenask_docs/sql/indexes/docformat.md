---
toc: content
order: 3
---

# 实时推送文档格式
文档格式示例：
```
CMD=add^_
PK=12345321^_
url=http://www.aliyun.com/index.html^_
title=阿里云计算有限公司^_
body=xxxxxx xxx^_
time=3123423421^_
multi_value_field=1234^]324^]342^_
bidwords=mp3^\price=35.8^Ptime=13867236221^]mp4^\price=32.8^Ptime=13867236221^_
^^
CMD=delete^_
PK=12345321^_
^^
CMD=update_field^_
PK=12345321^_
time=3123423421^_
^^
```
数据文件中总共有三个命令，分别是add、delete、update_field。每个命令由多行组成，每行都是一个key-value对。
命令与命令之间用`^^\n`分隔，每一对key-value之间以`^_\n`分隔，多值之间以`^]`分隔。

- cmd：文档的更新方式。
  - add：添加一篇文档，如果文档已经存在会先删除然后再添加 
  - update_field：更新一篇文档，目前只支持更新属性字段
  - delete：删除一篇文档，删除文档时需要指定文档主键

<div class="lake-content" typography="classic"><h2 id="zabBj" style="font-size: 24px; line-height: 32px; margin: 21px 0 5px 0"><span class="ne-text">注意事项</span></h2><ul class="ne-ul" style="margin: 0; padding-left: 23px"><li id="u35b62be1"><span class="ne-text">引擎有些内置的字符请不要在文档中使用，内置的字符如下表所示：</span></li></ul>

编码 | (emacs/vi)中的显示形态 | emacs中输入方法 | vi中输入方法
-- | -- | -- | --
"\x1E\n" | ^^（接换行） | C-q C-6 | C-v C-6
"\x1F\n" | ^_（接换行） | C-q C-7 | C-v C-7
"\x1D" | ^] | C-q C-5 | C-v C-5
"\x1C" | ^\ | C-q C-4 | C-v C-4
"\x03" | ^C | C-q C-c | C-v C-c

<span class="ne-text">TEXT类型的字段中不要出现多值分隔符'\x1D' ('^]')。</span></li></ul></div>
