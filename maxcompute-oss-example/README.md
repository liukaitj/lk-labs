# MaxCompute读取分析OSS非结构化数据的实践经验总结
## 1.	本文背景
很多行业的信息系统中，例如金融行业的信息系统，相当多的数据交互工作是通过传统的文本文件进行交互的。此外，很多系统的业务日志和系统日志由于各种原因并没有进入ELK之类的日志分析系统，也是以文本文件的形式存在的。随着数据量的指数级增长，对超大文本文件的分析越来越成为挑战。好在阿里云的MaxCompute产品从2.0版本开始正式支持了直接读取并分析存储在OSS上的文本文件，可以用结构化查询的方式去分析非结构化的数据。

本文对使用MaxCompute分析OSS文本数据的实践过程中遇到的一些问题和优化经验进行了总结。作为前提，读者需要详细了解MaxCompute读取OSS文本数据的一些基础知识，对这篇官方文档 [《访问 OSS 非结构化数据》](https://help.aliyun.com/document_detail/45389.html?spm=5176.doc27810.6.576.DsP5cs)最好有过实践经验。本文所描述的内容主要是针对这个文档中提到的自定义Extractor做出的一些适配和优化。
## 2. 场景实践
### 2.1 场景一：分析zip压缩后的文本文件
#### 场景说明
很多时候我们会对历史的文本数据进行压缩，然后上传到OSS上进行归档，那么如果要对这部分数据导入MaxCompute进行离线分析，我们可以自定义Extractor让MaxCompute直接读取OSS上的归档文件，避免了把归档文件下载到本地、解压缩、再上传回OSS这样冗长的链路。
#### 实现思路
如 [《访问 OSS 非结构化数据》](https://help.aliyun.com/document_detail/45389.html?spm=5176.doc27810.6.576.DsP5cs)文档中所述，MaxCompute读取OSS上的文本数据本质上是读取一个InputStream流，那么我们只要构造出适当的归档字节流，就可以直接获取这个InputStream中的数据了。  

以Zip格式的归档文件为例，我们可以参考 [DataX](https://github.com/alibaba/DataX) 中关于读取OSS上Zip文件的源码，构造一个Zip格式的InputStream，代码见 [ZipCycleInputStream.java](https://github.com/alibaba/DataX/blob/master/plugin-unstructured-storage-util/src/main/java/com/alibaba/datax/plugin/unstructuredstorage/reader/ZipCycleInputStream.java) 。构造出这个Zip格式的InputStream后，在自定义Extractor中获取文件流的部分就可以直接使用了，例如：
