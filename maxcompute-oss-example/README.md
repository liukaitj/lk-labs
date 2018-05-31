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

```java
    private BufferedReader moveToNextStream() throws IOException {
        SourceInputStream stream = inputs.next();
        // ......
        ZipCycleInputStream zipCycleInputStream = new ZipCycleInputStream(stream);
        return new BufferedReader(new InputStreamReader(zipCycleInputStream, "UTF-8"), 8192);
        // ......
     }
```
#### 优化经验
大家可能知道，MaxCompute中进行批量计算的时候，可以通过设置 **odps.stage.mapper.split.size** 这个参数来调整数据分片的大小，从而影响到执行计算任务的Mapper的个数，在一定程度上提高Mapper的个数可以增加计算的并行度，进而提高计算效率 *（但也不是说Mapper个数越多越好，因为这样可能会造成较长时间的资源等待，或者可能会造成长尾的后续Reducer任务，反而降低整体的计算效率）* 。

同样道理，对OSS上的文本文件进行解析的时候，也可以通过设置 **odps.sql.unstructured.data.split.size** 这个参数来达到调整Mapper个数的目的 *（注意这个参数可能需要提工单开通使用权限）*：
```sql
set odps.sql.unstructured.data.split.size=16;
```
上述设定的含义是，将OSS上的文件拆分为若干个16M左右大小的分片，让MaxCompute尽力做到每个分片启动一个Mapper任务进行计算——之所以说是“尽力做到”，是因为MaxCompute默认不会对单个文件进行拆分及分片处理*（除非设定了其他参数，我们后面会讲到）*，也就是说，如果把单个分片按照上面的设定为16M，而OSS上某个文件大小假设为32M，则MaxCompute仍然会把这个文件整体（即32M）的数据量作为一个分片进行Mapper任务计算。
#### 注意点
我们在这个场景中处理的是压缩后的文件，而InputStream处理的字节量大小是不会因压缩而变小的。举个例子，假设压缩比为1:10，则上述这个32M的压缩文件实际代表了320M的数据量，即MaxCompute会把1个Mapper任务分配给这320M的数据量进行处理；同理假设压缩比为1:20，则MaxCompute会把1个Mapper任务分配给640M的数据量进行处理，这样就会较大的影响计算效率。因此，我们需要根据实际情况调整分片参数的大小，并尽量把OSS上的压缩文件大小控制在一个比较小的范围内，从而可以灵活配置分片参数，否则分片参数的值会因为文件太大并且文件不会被拆分而失效。
#### 完整代码参考
[ZipExtractor](https://github.com/liukaitj/lk-labs/blob/master/maxcompute-oss-example/src/main/java/lk/lab/maxcompute/oss/zip/ZipExtractor.java)

### 2.2 场景二：过滤文本文件中的特定行
#### 场景说明
对于一些业务数据文件，特别是金融行业的数据交换文件，通常会有文件头或文件尾的设定要求，即文件头部的若干行数据是一些元数据信息，真正要分析的业务数据需要把这些元信息的行过滤掉，只分析业务数据部分的行，否则执行结构化查询的SQL语句的时候必然会造成任务失败。
#### 实现思路
在 [《访问 OSS 非结构化数据》](https://help.aliyun.com/document_detail/45389.html?spm=5176.doc27810.6.576.DsP5cs)文档中提到的 [代码示例](https://github.com/aliyun/aliyun-odps-java-sdk/blob/master/odps-sdk-impl/odps-udf-example/src/main/java/com/aliyun/odps/udf/example/text/TextExtractor.java) 中，对 **readNextLine()** 方法进行一些改造，对读取的每一个文件，即每个 currentReader 读取下一行的时候，记录下来当前处理的行数，用这个行数判断是否到达了业务数据行，如果未到业务数据行，则继续读取下一条记录，如果已经到达数据行，则将该行内容返回处理；而当跳转到下一个文件的时候，将 该行数值重置。

代码示例：
```java
    private String readNextLine() throws IOException {
        if (firstRead) {
            firstRead = false;
            currentReader = moveToNextStream();
            if (currentReader == null) {
                return null;
            }
        }
        // 读取行级数据
        while (currentReader != null) {
            String line = currentReader.readLine();
            if (line != null) {
                if (currentLine < dataLineStart) { // 若当前行小于数据起始行，则继续读取下一条记录
                    currentLine++;
                    continue;
                }
                if (!"EOF".equals(line)) { // 若未到达文件尾则将该行内容返回，若到达文件尾则直接跳到下个文件
                    return line;
                }
            }
            currentReader = moveToNextStream();
            currentLine = 1;
        }
        return null;
    }
```
此处 **dataLineStart** 表示业务数据的起始行，可以通过 **DataAttributes** 在建立外部表的时候从外部作为参数传入。当然也可以随便定义其他逻辑来过滤掉特定行，比如本例中的对文件尾的“EOF”行进行了简单的丢弃处理。
#### 完整代码参考
[ZipExtractor](https://github.com/liukaitj/lk-labs/blob/master/maxcompute-oss-example/src/main/java/lk/lab/maxcompute/oss/zip/ZipExtractor.java)

### 2.3 场景三：忽略文本中的空行
#### 场景说明
在 [《访问 OSS 非结构化数据》](https://help.aliyun.com/document_detail/45389.html?spm=5176.doc27810.6.576.DsP5cs)文档中提到的 [代码示例](https://github.com/aliyun/aliyun-odps-java-sdk/blob/master/odps-sdk-impl/odps-udf-example/src/main/java/com/aliyun/odps/udf/example/text/TextExtractor.java) 中，已可以应对大多数场景下的文本数据处理，但有时候在业务数据文本中会存在一些空行，这些空行可能会造成程序的误判，因此我们需要忽略掉这些空行，让程序继续分析处理后面有内容的行。
#### 实现思路
类似于上述 **场景二** ，只需要判断为空行后，让程序继续读取下一行文本即可。  
代码示例：
```java
    public Record extract() throws IOException {
        String line = readNextLine();
        if (line == null) {
            return null;// 返回null标志已经读取完成
        }
        while ("".equals(line.trim()) || line.length() == 0 || line.charAt(0) == '\r' // 遇到空行则继续处理
                || line.charAt(0) == '\n') {
            line = readNextLine();
            if (line == null)
                return null;
        }
        return textLineToRecord(line);
    }
```
#### 完整代码参考
[ZipExtractor](https://github.com/liukaitj/lk-labs/blob/master/maxcompute-oss-example/src/main/java/lk/lab/maxcompute/oss/zip/ZipExtractor.java)

### 2.4 场景四：选择OSS上文件夹下的部分文件进行处理
#### 场景说明
阅读 [《访问 OSS 非结构化数据》](https://help.aliyun.com/document_detail/45389.html?spm=5176.doc27810.6.576.DsP5cs)文档可知，一张MaxCompute的外部表连接的是OSS上的一个文件夹*（严格来说OSS没有“文件夹”这个概念，所有对象都是以Object来存储的，所谓的文件夹其实就是在OSS创建的一个字节数为0且名称以“/”结尾的对象。MaxCompute建立外部表时连接的是OSS上这样的以“/”结尾的对象，即连接一个“文件夹”）*，在处理外部表时，默认会对该文件夹下 **所有的文件** 进行解析处理。该文件夹下所有的文件集合即被封装为 [InputStreamSet](https://github.com/aliyun/aliyun-odps-java-sdk/blob/master/odps-sdk/odps-sdk-udf/src/main/java/com/aliyun/odps/io/InputStreamSet.java) ，然后通过其 **next()** 方法来依次获得每一个InputStream流、即每个文件流。

但有时我们可能会希望只处理OSS上文件夹下的 **部分** 文件，而不是全部文件，例如只分析那些文件名中含有“2018_”字样的文件，表示只分析2018年以来的业务数据文件。
#### 实现思路
在获取到每一个InputStream的时候，通过 [SourceInputStream](https://github.com/aliyun/aliyun-odps-java-sdk/blob/master/odps-sdk/odps-sdk-udf/src/main/java/com/aliyun/odps/io/SourceInputStream.java) 类的 **getFileName()** 方法获取正在处理的文件流所代表的文件名，然后可以通过正则表达式等方式判断该文件流是否为所需要处理的文件，如果不是则继续调用 **next()** 方法来获取下一个文件流。

代码示例：
```java
    private BufferedReader moveToNextStream() throws IOException {
        SourceInputStream stream = null;
        while ((stream = inputs.next()) != null) {
            String fileName = stream.getFileName();
            System.out.println("========inputs.next():" + fileName + "========");
            if (patternModel.matcher(fileName).matches()) {
                System.out.println(String
                        .format("- match fileName:[%s], pattern:[%s]", fileName, patternModel
                                .pattern()));
                ZipCycleInputStream zipCycleInputStream = new ZipCycleInputStream(stream);
                return new BufferedReader(new InputStreamReader(zipCycleInputStream, "UTF-8"), 8192);
            } else {
                 System.out.println(String.format(
                         "-- discard fileName:[%s], pattern:[%s]", fileName, patternModel.pattern()));
                continue;
            }
        }
        return null;
    }
```
本例中的 **patternModel** 为通过 **DataAttributes** 在建立外部表的时候从外部作为参数传入的正则规则。

写到这里可能有读者会问，如果一个文件夹下有很多文件，比如上万个文件，整个遍历一遍后只选择一小部分文件进行处理这样的方式会不会效率太低了？其实大可不必担心，因为相对于MaxCompute对外部表执行批量计算的过程，循环遍历文件流的时间消耗是非常小的，通常情况下是不会影响批量计算任务的。
#### 完整代码参考
[ZipExtractor](https://github.com/liukaitj/lk-labs/blob/master/maxcompute-oss-example/src/main/java/lk/lab/maxcompute/oss/zip/ZipExtractor.java)

### 2.5 场景五：针对单个大文件进行拆分
#### 场景说明
在 **场景一** 中提到，要想提高计算效率，我们需要调整 **odps.sql.unstructured.data.split.size** 参数值来增加Mapper的并行度，但是对于单个大文件来讲，MaxCompute默认是不进行拆分的，也就是说OSS上的单个大文件只会被分配给一个Mapper任务进行处理，如果这个文件非常大的话，处理效率将会及其低下，我们需要一种方式来实现对单个文件进行拆分，使其可以被多个Mapper任务进行并行处理。
#### 实现思路
仍然是要依靠调整 **odps.sql.unstructured.data.split.size** 参数来增加Mapper的并行度，并且设定 **odps.sql.unstructured.data.single.file.split.enabled** 参数来允许拆分单个文件 *（同odps.sql.unstructured.data.split.size，该参数也可能需要提工单申请使用权限）* ，例如：
```sql
set odps.sql.unstructured.data.split.size=128;
set odps.sql.unstructured.data.single.file.split.enabled=true;
```
设置好这些参数后，就需要编写特定的Reader类来进行单个大文件的拆分了。

核心的思路是，根据 **odps.sql.unstructured.data.split.size** 所设定的值，大概将文件按照这个大小拆分开，但是拆分点极大可能会切在一条记录的中间，这时就需要调整字节数，向前或向后寻找换行符，来保证最终的切分点落在一整条记录的尾部。具体的实现细节相对来讲比较复杂，可以参考在 [《访问 OSS 非结构化数据》](https://help.aliyun.com/document_detail/45389.html?spm=5176.doc27810.6.576.DsP5cs)文档中提到的 [代码示例](https://github.com/aliyun/aliyun-odps-java-sdk/blob/master/odps-sdk-impl/odps-udf-example/src/main/java/com/aliyun/odps/udf/example/text/TextExtractor.java) 来进行分析。
#### 注意点
在计算字节数的过程中，可能会遇到非英文字符造成计算切分点的位置计算不准确，进而出现读取的字节流仍然没有把一整行覆盖到的情况。这需要针对含有非英文字符的文本数据做一些特殊处理。

代码示例：
```java
    @Override
    public int read(char[] cbuf, int off, int len) throws IOException {
        if (this.splitReadLen >= this.splitSize) {
            return -1;
        }
        if (this.splitReadLen + len >= this.splitSize) {
            len = (int) (this.splitSize - this.splitReadLen);
        }
        int readSize = this.internalReader.read(cbuf, off, len);
        int totalBytes = 0;
        for (char ch : cbuf) {
            String str = String.valueOf(ch);
            byte[] bytes = str.getBytes(charset);
            totalBytes += bytes.length;
        }
        this.splitReadLen += totalBytes;
        return readSize;
    }
```
#### 完整代码参考
[SplitReader](https://github.com/liukaitj/lk-labs/blob/master/maxcompute-oss-example/src/main/java/lk/lab/maxcompute/oss/split/SplitReader.java)
[SplitExtractor](https://github.com/liukaitj/lk-labs/blob/master/maxcompute-oss-example/src/main/java/lk/lab/maxcompute/oss/split/SplitExtractor.java)

## 3. 其他建议
1. 在编写自定义Extractor的程序中，适当加入System.out作为日志信息输出，这些日志信息会在MaxCompute执行时输出在LogView的视图中，对于调试过程和线上问题排查过程非常有帮助。
2. 上文中提到通过调整 **odps.sql.unstructured.data.split.size** 参数值来适当提高Mapper任务的并行度，但是并行度并不是越高越好，具体什么值最合适是与OSS上的文件大小、总数据量、MaxCompute产品自身的集群状态紧密联系在一起的，需要多次调试，并且可能需要与 **odps.stage.reducer.num**、**odps.sql.reshuffle.dynamicpt**、**odps.merge.smallfile.filesize.threshold** 等参数配合使用才能找到最优值。并且由于MaxCompute产品自身的集群状态也是很重要的因素，可能今天申请500个Mapper资源是很容易的事情，过几个月就变成经常需要等待很长时间才能申请到，这就需要持续关注任务的执行时间并及时调整参数设定。
3. 外部表的读取和解析是依靠Extractor对文本的解析来实现的，因此在执行效率上是远不能和MaxCompute的普通表相比的，所以在需要频繁读取和分析OSS上的文本文件的情况下，建议将OSS文件先 **INSERT OVERWRITE** 到MaxCompute中字段完全对等的一张普通表中，然后针对普通表进行分析计算，这样通常会获得更好的计算效率。
