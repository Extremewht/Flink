## Flink

4.26

### 第 1 章 Flink的概念

​	Flink是一个**框架**和**分布式处理**引擎，用于对**无界和有界数据流**进行**状态**计算

​	用于对无界和有界数据流进行有状态计算。Flink 被设计在所有常见的集群环境中运行，以内存执行速度和 任意规模来执行计算。

![image-20230426110335537](C:\Users\王海涛\AppData\Roaming\Typora\typora-user-images\image-20230426110335537.png)

##### 1.1 Flink的源起和设计理念

⚫ 2014 年 8 月，Flink 第一个版本 0.6 正式发布（至于 0.5 之前的版本，那就是在 Stratosphere 名下的了）。与此同时 Fink 的几位核心开发者创办了 Data Artisans 公司， 主要做 Fink 的商业应用，帮助企业部署大规模数据处理解决方案。 

⚫ 2014 年 12 月，Flink 项目完成了孵化，一跃成为 Apache 软件基金会的顶级项目。 

⚫ 2015 年 4 月，Flink 发布了里程碑式的重要版本 0.9.0，很多国内外大公司也正是从这 时开始关注、并参与到 Flink 社区建设的。 ⚫ 2019 年 1 月，长期对 Flink 投入研发的阿里巴巴，以 9000 万欧元的价格收购了 Data  Artisans 公司；之后又将自己的内部版本 Blink 开源，继而与 8 月份发布的 Flink 1.9.0 版本进行了合并。自此之后，Flink 被越来越多的人所熟知，成为当前最火的新一代 大数据处理框架。

##### 1.2 Flink的应用

1）电商和市场营销

举例：实时数据报表、广告投放、实时推荐

2）物联网

举例：传感器实时数据采集和显示，实时报警，交通运输业

3）物流配送和服务业

举例：订单状态实时更新、通知信息推送

4）银行和金融业

举例：实时结算和通知推送，实时检测异常行为

##### 1.3 Flink的特性

###### 1.3.1 核心特性

Flink 区别与传统数据处理框架的特性如下。 

1）**高吞吐和低延迟**。每秒处理数百万个事件，毫秒级延迟。

2）**结果的准确性**。Flink 提供了事件时间（event-time）和处理时间（processing-time） 语义。对于乱序事件流，事件时间语义仍然能提供一致且准确的结果。 

3）**精确一次（exactly-once）的状态一致性保证**。 

4）**可以连接到最常用的存储系统**，如 Apache Kafka、Apache Cassandra、Elasticsearch、 JDBC、Kinesis 和（分布式）文件系统，如 HDFS 和 S3。 

5）**高可用**。本身高可用的设置，加上与 K8s，YARN 和 Mesos 的紧密集成，再加上从故障中快速恢复和动态扩展任务的能力，Flink 能做到以极少的停机时间 7×24 全天候 运行。 

6）**能够更新应用程序代码并将作业（jobs）迁移到不同的 Flink 集群，而不会丢失应用程序的状态。**

###### 1.3.2 分层API

越顶层越抽象，表达含义越简明，使用越方便

越底层越具体，表达能力越丰富，使用越灵活

![image-20230426160007527](C:\Users\王海涛\AppData\Roaming\Typora\typora-user-images\image-20230426160007527.png)

​		1.最底层级的抽象仅仅提供了有状态流，它将处理函数（Process Function）嵌入到了 DataStream API 中。底层处理函数（Process Function）与 DataStream API 相集成，可以对某 些操作进行抽象，它允许用户可以使用自定义状态处理来自一个或多个数据流的事件，且状态具有一致性和容错保证。

​		2.大多数应用并不需要上述的底层抽象，而是**直接针对核心 API（Core APIs）进行编程，比如 DataStream API（用于处理有界或无界流数据）以及 DataSet API（用于处理有界 数据集）。这些 API 为数据处理提供了通用的构建模块，比如由用户定义的多种形式的转换 （transformations）、连接（joins）、聚合（aggregations）、窗口（windows）操作等。**

​		3.**Table API 是以表为中心的声明式编程，其中表在表达流数据时会动态变化。**Table API 遵 循关系模型：表有二维数据结构（schema）（类似于关系数据库中的表），同时 API 提供可比 较的操作，例如 select、join、group-by、aggregate 等。

​		4.**Flink 提供的最高层级的抽象是 SQL**。这一层抽象在语法与表达能力上与 Table API 类似， 但是是以 SQL 查询表达式的形式表现程序。SQL 抽象与 Table API 交互密切，同时 SQL 查询 可以直接在 Table API 定义的表上执行。

##### 1.4 Flink VS Spark

1）数据处理架构：

Spark：基于批处理

Flink：基于流处理

![image-20230426161024461](C:\Users\王海涛\AppData\Roaming\Typora\typora-user-images\image-20230426161024461.png)

2）数据模型

​	Spark采用RDD模型，spark steaming的Dstream实际上就是一组组小批数据RDD的集合

​	Flink基本数据模型是数据流，以及时间（Event）序列

3）运行时架构

​	Spark是批计算，将DAG划分不同的Stage，一个完成后才可以计算下一个

​	Flink是标准的流执行模型，一个事件在一个节点处理完后可以直接发往下一个节点进行处理

### 第 2 章 Flink快速上手

##### 2.1 创建项目

1）创建工程 

​	打开 IntelliJ IDEA，创建一个 Maven 工程

2）添加项目依赖

​		在项目的 pom 文件中，增加标签设置属性，然后增加标签引 入需要的依赖。我们需要添加的依赖最重要的就是 **Flink 的相关组件，包括 flink-java、 flink-streaming-java，以及 flink-clients（客户端，也可以省略）。**另外，为了方便查看运行日志， 我们引入 slf4j 和 log4j 进行日志管理。

```
<properties>
 <flink.version>1.13.0</flink.version>
 <java.version>1.8</java.version>
 <scala.binary.version>2.12</scala.binary.version>
 <slf4j.version>1.7.30</slf4j.version>
</properties>
<dependencies>
<!-- 引入 Flink 相关依赖-->
 <dependency>
 <groupId>org.apache.flink</groupId>
 <artifactId>flink-java</artifactId>
 <version>${flink.version}</version>
 </dependency>
 <dependency>
 <groupId>org.apache.flink</groupId>
 <artifactId>flink-streaming-java_${scala.binary.version}</artifactId>
 <version>${flink.version}</version>
 </dependency>
 <dependency>
 <groupId>org.apache.flink</groupId>
 <artifactId>flink-clients_${scala.binary.version}</artifactId>
 <version>${flink.version}</version>
</dependency>
<!-- 引入日志管理相关依赖-->
 <dependency>
 <groupId>org.slf4j</groupId>
 <artifactId>slf4j-api</artifactId>
 <version>${slf4j.version}</version>
 </dependency>
 <dependency>
 <groupId>org.slf4j</groupId>
 <artifactId>slf4j-log4j12</artifactId>
 <version>${slf4j.version}</version>
 </dependency>
 <dependency>
 <groupId>org.apache.logging.log4j</groupId>
 <artifactId>log4j-to-slf4j</artifactId>
 <version>2.14.0</version>
</dependency>
</dependencies>
```

3）配置日志管理

在目录 src/main/resources 下添加文件:log4j.properties，内容配置如下：

```
log4j.rootLogger=error, stdout
log4j.appender.stdout=org.apache.log4j.ConsoleAppender
log4j.appender.stdout.layout=org.apache.log4j.PatternLayout
log4j.appender.stdout.layout.ConversionPattern=%-4r [%t] %-5p %c %x - %m%n
```

##### 2.2 编写代码

尽管 Flink 自身的定位是流式处理引擎，**但它同样拥有批处理的能力**。

###### 2.2.1 批处理

​		对于批处理而言，输入的应该是收集好的数据集。这里我们可以将要统计的文字，写入一 个文本文档，然后读取这个文件处理数据就可以了。

​	（1）在工程根目录下新建一个input文件夹，并在下面创建文本文件words.txt

​	（2）在words.txt中输入一些文字，例如：

```
hello world
hello flink
hello java
```

​	（3）新建Java类BatchWordCount，在静态方法中编写测试代码

```
//1.创建执行环境
ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

//2.从文件中读取数据
DataSource<String> lineDataSource = env.readTextFile("input/word.txt");

//3.将每行数据进行分词，然后转换成二元组类型
FlatMapOperator<String, Tuple2<String, Long>> wordAndOneTuple = lineDataSource.flatMap((String line, Collector<Tuple2<String, Long>> out) -> {
    //将一行文本进行分词
    String[] words = line.split(" ");
    //将每个单词转换成二元组输出
    for (String word : words) {
        out.collect(Tuple2.of(word, 1L));
    }
})

.returns(Types.TUPLE(Types.STRING,Types.LONG));

//4.按照word进行分组
UnsortedGrouping<Tuple2<String, Long>> wordAndOneGroup = wordAndOneTuple.groupBy(0);

//5.分组内进行聚合统计
AggregateOperator<Tuple2<String, Long>> sum = wordAndOneGroup.sum(1);

//6.打印结果
sum.print();
```

###### 2.2.2 流处理

1.读取文件

```
//1. 创建流式执行环境
StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

//2.读取文件
DataStreamSource<String> lineDataStreamSource = env.readTextFile("input/word.txt");

//3.转换计算
SingleOutputStreamOperator<Tuple2<String, Long>> wordAndOneTuple = lineDataStreamSource.flatMap((String line, Collector<Tuple2<String, Long>> out) -> {
    String[] words = line.split(" ");
    for (String word : words) {
        out.collect(Tuple2.of(word, 1L));
    }
})

        .returns(Types.TUPLE(Types.STRING, Types.LONG));

//4.分组
KeyedStream<Tuple2<String, Long>, String> wordAndOneKeyedStream = wordAndOneTuple.keyBy(data -> data.f0);

//5.求和
SingleOutputStreamOperator<Tuple2<String, Long>> sum = wordAndOneKeyedStream.sum(1);

//6.打印结果
sum.print();

//7.启动执行
env.execute();
```

主要观察与批处理程序 BatchWordCount 的不同： 

⚫ 创建执行环境的不同，流处理程序使用的是 StreamExecutionEnvironment。 

⚫ 每一步处理转换之后，得到的数据对象类型不同。 

⚫ 分组操作调用的是 keyBy 方法，可以传入一个匿名函数作为键选择器 （KeySelector），指定当前分组的 key 是什么。 

⚫ 代码末尾需要调用 env 的 execute 方法，开始执行任务。

2.读取文本流

```
//1.创建流式环境
StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

//从参数中提取主机名和端口号
ParameterTool parameterTool = ParameterTool.fromArgs(args);
String hostNmae = parameterTool.get("host");
Integer port =parameterTool.getInt("port");

//2.读取文本流
DataStreamSource<String> lineStream = env.socketTextStream(hostNmae, port);

//3.转换计算
SingleOutputStreamOperator<Tuple2<String, Long>> wordAndOneTuple = lineStream.flatMap((String line, Collector<Tuple2<String, Long>> out) -> {
            String[] words = line.split(" ");
            for (String word : words) {
                out.collect(Tuple2.of(word, 1L));
            }
        })

        .returns(Types.TUPLE(Types.STRING, Types.LONG));

//4.分组
KeyedStream<Tuple2<String, Long>, String> wordAndOneKeyedStream = wordAndOneTuple.keyBy(data -> data.f0);

//5.求和
SingleOutputStreamOperator<Tuple2<String, Long>> sum = wordAndOneKeyedStream.sum(1);

//6.打印结果
sum.print();

//7.启动执行
env.execute();
```

代码说明和注意事项： 

socketTextStream

⚫ socket 文本流的读取需要配置两个参数：**发送端主机名和端口号**。这里代码中指定了主机“hadoop102”的 7777 端口作为发送数据的 socket 端口，读者可以根据 测试环境自行配置。 

⚫ 在实际项目应用中，主机名和端口号这类信息往往可以通过配置文件，或者 传入程序运行参数的方式来指定。 

⚫ socket文本流数据的发送，可以通过Linux系统自带的netcat工具进行模拟。

### 第 3 章 Flink部署

​		 Flink 中的几个关键组件：**客户端（Client）、作业管理器（JobManager）和 任务管理器（TaskManager）**。我们的代码，实际上是由客户端获取并做转换，之后提交给 JobManger 的。所以 JobManager 就是 Flink 集群里的“管事人”，对作业进行中央调度管理； 而它获取到要执行的作业后，会进一步处理转换，然后分发任务给众多的 TaskManager。这里 的 TaskManager，就是真正“干活的人”，数据的处理操作都是它们来做的。

![image-20230503135229607](C:\Users\王海涛\AppData\Roaming\Typora\typora-user-images\image-20230503135229607.png)

Flink是一个非常灵活的处理框架，他支持多种不同的部署场景，还可以和不同的资源管理平台方便地集成。

##### 3.1 快速启动一个Flink集群

###### 3.1.1 环境配置

Flink 是一个分布式的流处理框架，所以实际应用一般都需要搭建集群环境，需要准备 3 台 Linux 机器。具体要求如下： 

1）系统环境为 CentOS 7.5 版本。 

2）安装 Java 8。

3）安装 Hadoop 集群，Hadoop 建议选择 Hadoop 2.7.5 以上版本。 

4）配置集群节点服务器间时间同步以及免密登录，关闭防火墙。 本书中三台服务器的具体设置如下： 

节点服务器 1，IP 地址为 192.168.75.102，主机名为 hadoop102。 节点服务器 2，IP 地址为 192.168.75.103，主机名为 hadoop103。 节点服务器 3，IP 地址为 192.168.75.104，主机名为 hadoop104。

###### 3.1.2 本地启动

​		最简单的启动方式，其实是不搭建集群，直接本地启动。本地部署非常简单，直接解压安 装包就可以使用，不用进行任何配置；一般用来做一些简单的测试。

1. 解压

​	在 hadoop102 节点服务器上创建安装目录/opt/module，将 flink 安装包放在该目录下，并 执行解压命令，解压至当前目录。 

```
$ tar -zxvf flink-1.13.0-bin-scala_2.12.tgz -C /opt/module/ flink-1.13.0/ flink-1.13.0/log/ flink-1.13.0/LICENSE flink-1.13.0/lib/ ……
```

2. 启动

​	进入解压后的目录，执行启动命令，并查看进程

```
$ cd flink-1.13.0/
$ bin/start-cluster.sh 

Starting cluster.
Starting standalonesession daemon on host hadoop102.
Starting taskexecutor daemon on host hadoop102.
```

3. 访问Web UI

​	启动成功后，访问 http://hadoop102:8081，可以对flink集群和任务进行监控管理

![image-20230503161719693](C:\Users\王海涛\AppData\Roaming\Typora\typora-user-images\image-20230503161719693.png)

4. 关闭集群

如果想要让Flink集群停止运行，可以执行以下命令：

```
$ bin/stop-cluster.sh 

Stopping taskexecutor daemon (pid: 10680) on host hadoop102.
Stopping standalonesession daemon (pid: 10369) on host hadoop102.
```

###### 3.1.3 集群启动

​	 Flink 是典型的 Master-Slave 架构的分布式数据处理框架，其中 Master 角色对应着 JobManager，Slave 角色则对应TaskManager。我们对三台节点服务器的角色分配如表所示

| 节点服务器 | hadoop102  | hadoop103   | hadoop104   |
| ---------- | ---------- | ----------- | ----------- |
| 角色       | JobManager | TaskManager | TaskManager |

1. 修改集群配置

（1）进入 conf 目录下，修改 flink-conf.yaml 文件，修改 jobmanager.rpc.address 参数为 hadoop102，如下所示：

```
$ cd conf/
$ vim flink-conf.yaml
# JobManager 节点地址.
jobmanager.rpc.address: hadoop102
```

这就指定了 hadoop102 节点服务器为 JobManager 节点

（2）修改 workers 文件，将另外两台节点服务器添加为本 Flink 集群的 TaskManager 节点

```
$ vim workers 
hadoop103
hadoop104
```

这样就指定了 hadoop103 和 hadoop104 为 TaskManager 节点。

2. 分发安装目录

配置修改完毕后，将 Flink 安装目录发给另外两个节点服务器。

```
xsync  ./flink-1.13.0
```

3. 启动集群

（1）在 hadoop102 节点服务器上执行 start-cluster.sh 启动 Flink 集群：

```
$ bin/start-cluster.sh 
Starting cluster.

Starting standalonesession daemon on host hadoop102.
Starting taskexecutor daemon on host hadoop103.
Starting taskexecutor daemon on host hadoop104
```

（2）查看进程情况

```
[atguigu@hadoop102 flink-1.13.0]$ jps
13859 Jps
13782 StandaloneSessionClusterEntrypoint
[atguigu@hadoop103 flink-1.13.0]$ jps
12215 Jps
12124 TaskManagerRunner
[atguigu@hadoop104 flink-1.13.0]$ jps
11602 TaskManagerRunner
11694 Jps
```

4. 访问UI

启动成功后，同样可以访问 http://hadoop102:8081 对 flink 集群和任务进行监控管理

![image-20230503164832591](C:\Users\王海涛\AppData\Roaming\Typora\typora-user-images\image-20230503164832591.png)

###### 3.1.4 向集群提交作业

​		以流处理的程序为例，演示如何将任务提交到集群中进行执行。具体步骤如下。

1.程序打包

（1）为方便自定义结构和定制依赖，引入了插件 maven-assembly-plugin 进行打包。 在 Flink 项目的 pom.xml 文件中添加打包插件的配置

```
<build>
 <plugins>
 <plugin>
 <groupId>org.apache.maven.plugins</groupId>
 <artifactId>maven-assembly-plugin</artifactId>
 <version>3.0.0</version>
 <configuration>
 <descriptorRefs>
 <descriptorRef>jar-with-dependencies</descriptorRef>
 </descriptorRefs>
 </configuration>
 <executions>
 <execution>
 <id>make-assembly</id>
 <phase>package</phase>
 <goals>
 <goal>single</goal>
 </goals>
 </execution>
 </executions>
 </plugin>
 </plugins>
</build>
```

（2）插件配置完毕后，可以使用IDEA的Maven工具执行package命令，出现如下提示即表示打包成功

```
[INFO] ----------------------------------------------
[INFO] BUILD SUCCESS
[INFO] ----------------------------------------------
[INFO] Total time: 21.665 s
[INFO] Finished at: 2021-06-01T17:21:26+08:00
[INFO] Final Memory: 141M/770M
[INFO] ----------------------------------------------
```

​		打 包 完 成 后 ， 在 target 目 录 下 即 可 找 到 所 需 JAR 包 ， JAR 包 会 有 两 个 ， Flink-1.0-SNAPSHOT.jar 和 Flinkl-1.0-SNAPSHOT-jar-with-dependencies.jar，因为集群中已经具备任务运行所需的所有依赖，所以使用 Tutorial-1.0-SNAPSHOT.jar。

2.提交作业

​		任务打包完成后，可以选择在命令行启动任务，也可以直接在WebUI界面启动任务

1）WebUI页面启动任务

（1）打开 Flink 的 WEB UI 页面，在右侧导航栏点击“Submit New Job”，然后点击按钮“+ Add New”，选择要上传运行的 JAR 包

![image-20230503165226279](C:\Users\王海涛\AppData\Roaming\Typora\typora-user-images\image-20230503165226279.png)

（2）点击该 JAR 包，出现任务配置页面，进行相应配置。

​		主要配置程序入口主类的全类名，任务运行的并行度，任务运行所需的配置参数和保存点 路径等，配置完成后，即可点击按钮“Submit”，将任务提交到集群运行。

![image-20230503165316927](C:\Users\王海涛\AppData\Roaming\Typora\typora-user-images\image-20230503165316927.png)

（3）任务提交成功之后，可点击左侧导航栏的“Running Jobs”查看程序运行列表情况

![image-20230503165340400](C:\Users\王海涛\AppData\Roaming\Typora\typora-user-images\image-20230503165340400.png)

（4）点击该任务，可以查看任务运行的具体情况，也可以通过点击“Cancel Job”结束任务运行

![image-20230503165401885](C:\Users\王海涛\AppData\Roaming\Typora\typora-user-images\image-20230503165401885.png)

2）命令行提交作业

（1）首先需要启动集群。 

```
$ bin/start-cluster.sh 
```

（2）在 hadoop102 中执行以下命令启动 netcat。 

```
$ nc -lk 7777 
```

（3）进入到 Flink 的安装路径下，在命令行使用 flink run 命令提交作业。

```
 $ bin/flink run -m hadoop102:8081 -c  com.atguigu.wc.StreamWordCount ./FlinkTutorial-1.0-SNAPSHOT.jar 
```

这里的参数 –m 指定了提交到的 JobManager，-c 指定了入口类。 （4）在浏览器中打开 Web UI，http://hadoop102:8081 查看应用执行情况

![image-20230503165516066](C:\Users\王海涛\AppData\Roaming\Typora\typora-user-images\image-20230503165516066.png)

（5）在 log 日志中，也可以查看执行结果，需要找到执行该数据任务的 TaskManager 节点 查看日志

##### 3.2 部署模式

在一些应用场景中，对于集群资源分配和占用的方式，可能会有特定的需求。Flink 为各 种场景提供了不同的部署模式，主要有以下三种： 

 会话模式（Session Mode） 

单作业模式（Per-Job Mode） 

 应用模式（Application Mode） 

它们的区别主要在于：**集群的生命周期以及资源的分配方式；以及应用的 main 方法到底在哪里执行——客户端（Client）还是 JobManager。**

###### 3.2.1 会话模式

我们需要先启动一个集群，保持一个会话，在这个会话中 通过客户端提交作业，集群启动时所有资源就都已经确定，所以所有提交的 作业会竞争集群中的资源。

![image-20230503191213304](C:\Users\王海涛\AppData\Roaming\Typora\typora-user-images\image-20230503191213304.png)

会话模式比较适合于单个规模小、执行时间短的大量作业。

###### 3.2.2 单作业模式

​		会话模式因为资源共享会导致很多问题，所以为了更好地隔离资源，可以考虑为每个 提交的作业启动一个集群，这就是 单作业（Per-Job）模式

![image-20230503191457903](C:\Users\王海涛\AppData\Roaming\Typora\typora-user-images\image-20230503191457903.png)

​		单作业模式也很好理解，就是严格的一对一，集群只为这个作业而生。同样由客户端运行应用程序，然后启动集群，作业被提交给 JobManager，进而分发给 TaskManager 执行。

​		作业完成后，集群就会关闭，所有资源也会释放。这样一来，每个作业都有它自己的 JobManager 管理，占用独享的资源，即使发生故障，它的 TaskManager 宕机也不会影响其他作业。

​	 **这些特性使得单作业模式在生产环境运行更加稳定，所以是实际应用的首选模式。**

###### 3.2.3 应用模式

​		不要客户端，直接把应用提交到 JobManger 上运行。需要为每一个提交的应用单独启动一个 JobManager，也就是创建一个集群。这 个 JobManager 只为执行这一个应用而存在，执行结束之后 JobManager 也就关闭了，这就是应用模式

![image-20230503192951433](C:\Users\王海涛\AppData\Roaming\Typora\typora-user-images\image-20230503192951433.png)

##### 3.3 独立模式（Standalone）

​		独立模式（Standalone）是部署 Flink 最基本也是最简单的方式：所需要的所有 Flink 组件， 都只是操作系统上运行的一个 JVM 进程

​		独立模式是独立运行的，不依赖任何外部的资源管理平台；当然独立也是有代价的：如果 资源不足，或者出现故障，没有自动扩展或重分配资源的保证，必须手动处理。**所以独立模式 一般只用在开发测试或作业非常少的场景下。**

​		另外，我们也可以将独立模式的集群放在容器中运行。Flink 提供了独立模式的容器化部 署方式，可以在 Docker 或者 Kubernetes 上进行部署。

###### 3.3.1 会话模式部署

​		独立模式的特点是不依赖外部资源管理平台，而会话模式的特点是先启动集群、 后提交作业

###### 3.3.2 单作业模式部署

​		Flink 本身无法直接以单作业方式启动集群，一般需要借助一些资 源管理平台。所以 Flink 的独立（Standalone）集群并不支持单作业模式部署。

###### 3.3.3 应用模式部署

​		应用模式下不会提前创建集群，所以不能调用 start-cluster.sh 脚本。可以使用同样在 bin 目录下的 standalone-job.sh 来创建一个 JobManager。 

具体步骤如下： 

（1）进入到 Flink 的安装路径下，将应用程序的 jar 包放到 lib/目录下。

```
 $ cp ./Flink-1.0-SNAPSHOT.jar lib/ 
```

（2）执行以下命令，启动 JobManager。

```
 $ ./bin/standalone-job.sh start --job-classname com.wanghaitao.wc.StreamWordCount 
```

这里我们直接指定作业入口类，脚本会到 lib 目录扫描所有的 jar 包。 （3）同样是使用 bin 目录下的脚本，启动 TaskManager。 

```
$ ./bin/taskmanager.sh start 
```

（4）如果希望停掉集群，同样可以使用脚本，命令如下。 

```
$ ./bin/standalone-job.sh stop 

$ ./bin/taskmanager.sh stop
```

##### 3.4 YARN模式

​		YARN 上部署的过程是：**客户端把 Flink 应用提交给 Yarn 的 ResourceManager,  Yarn 的 ResourceManager 会向 Yarn 的 NodeManager 申请容器。在这些容器上，Flink 会部署 JobManager 和 TaskManager 的实例，从而启动集群。Flink 会根据运行在 JobManger 上的作业 所需要的 Slot 数量动态分配 TaskManager 资源。**

###### 3.4.1 相关准备和配置

配置环境变量，增加环境变量配置如下： 

```
$ sudo vim /etc/profile.d/my_env.sh HADOOP_HOME=/opt/module/hadoop-2.7.5 export PATH=$PATH:$HADOOP_HOME/bin:$HADOOP_HOME/sbin 43 export HADOOP_CONF_DIR=${HADOOP_HOME}/etc/hadoop export HADOOP_CLASSPATH=`hadoop classpath`
```

###### 3.4.2 会话模式部署

​		YARN 的会话模式与独立集群略有不同，需要首先申请一个 YARN 会话（YARN session） 来启动 Flink 集群。具体步骤如下：

1.启动集群

（1）启动hadoop集群（HDFS，YARN）

（2）执行脚本命令向YARN集群申请资源，开启一个YARN会话，启动Flink集群

```
$ bin/yarn-session.sh -nm test
```

可用参数解读： 

⚫ -d：分离模式，如果你不想让 Flink YARN 客户端一直前台运行，可以使用这个参数， 44 即使关掉当前对话窗口，YARN session 也可以后台运行。 

⚫ -jm(--jobManagerMemory)：配置 JobManager 所需内存，默认单位 MB。 

⚫ -nm(--name)：配置在 YARN UI 界面上显示的任务名。 

⚫ -qu(--queue)：指定 YARN 队列名。 

⚫ -tm(--taskManager)：配置每个 TaskManager 所使用内存。

​		YARN Session 启动之后会给出一个 web UI 地址以及一个 YARN application ID， 用户可以通过 web UI 或者命令行两种方式提交作业。

YARN Session 启动之后会给出一个 web UI 地址以及一个 YARN application ID，如下所示， 用户可以通过 web UI 或者命令行两种方式提交作业。

1）命令行提交任务方式：/bin/flink run -c 入口类   [jar包]

```
./bin/flink run -c com.wanghaitao.wc.StreamWordCount ./Flink-1.0-SNAPSHOT.jar
```

2）WebUI提交任务：

![image-20230503203211177](C:\Users\王海涛\AppData\Roaming\Typora\typora-user-images\image-20230503203211177.png)

![image-20230503203331848](C:\Users\王海涛\AppData\Roaming\Typora\typora-user-images\image-20230503203331848.png)

###### 3.4.3 单作业模式部署

​		在 YARN 环境中，由于有了外部平台做资源调度，所以也可以直接向 YARN 提交一 个单独的作业，从而启动一个 Flink 集群。

（1）执行命令提交作业

​		  bin/flink run yarn-per-job -c 入口类 jar包

```
$ bin/flink run -d -t yarn-per-job -c com.atguigu.wc.StreamWordCount  FlinkTutorial-1.0-SNAPSHOT.jar
```

（2）在YARN的ResourceManager界面查看执行情况

![image-20230503203551233](C:\Users\王海涛\AppData\Roaming\Typora\typora-user-images\image-20230503203551233.png)

点击可以打开 Flink Web UI 页面进行监控

![image-20230503203604959](C:\Users\王海涛\AppData\Roaming\Typora\typora-user-images\image-20230503203604959.png)

如果取消作 业，整个 Flink 集群也会停掉。

###### 3.4.4 应用模式部署

应用模式同样非常简单，与单作业模式类似，直接执行 flink run-application 命令即可。 

（1）执行命令提交作业。

 $ bin/flink run-application -t yarn-application -c 入口类 jar包

```
 $ bin/flink run-application -t yarn-application -c com.atguigu.wc.StreamWordCount  FlinkTutorial-1.0-SNAPSHOT.jar 
```

（2）在命令行中查看或取消作业。

```
 $ ./bin/flink list -t yarn-application -Dyarn.application.id=application_XXXX_YY 
 
 $ ./bin/flink cancel -t yarn-application  -Dyarn.application.id=application_XXXX_YY
```

  （3）也可以通过 yarn.provided.lib.dirs 配置选项指定位置，将 jar 上传到远程。 

```
$ ./bin/flink run-application -t yarn-application -Dyarn.provided.lib.dirs="hdfs://myhdfs/my-remote-flink-dist-dir" hdfs://myhdfs/jars/my-application.jar
```

​	 	这种方式下 jar 可以预先上传到 HDFS，而不需要单独发送到集群，这就使得作业提交更加轻量了。

### 第 4 章 Flink运行时架构

##### 4.1 系统架构

​		对于数据处理系统的架构，最简单的实现方式当然就是单节点。当数据量增大、处理计算 更加复杂时，我们可以考虑增加 CPU 数量、加大内存，也就是让这一台机器变得性能更强大， 从而提高吞吐量——这就是所谓的 SMP（Symmetrical Multi-Processing，对称多处理）架构。 但是这样做问题非常明显：所有 CPU 是完全平等、共享内存和总线资源的，这就势必造成资源竞争；而且随着 CPU 核心数量的增加，机器的成本会指数增长，所以 SMP 的可扩展性是比 较差的，无法应对海量数据的处理场景。

###### 4.1.1 整体构成

​		Flink 的运行时架构中，最重要的就是两大组件：**作业管理器（JobManger）和任务管理器 （TaskManager）**。对于一个提交执行的作业，JobManager 是真正意义上的“管理者”（Master）， 负责管理调度，所以在不考虑高可用的情况下只能有一个；而 TaskManager 是“工作者” （Worker、Slave），负责执行任务处理数据，所以可以有一个或多个。Flink 的作业提交和任务处理时的系统如图所示。

![image-20230504143706360](C:\Users\王海涛\AppData\Roaming\Typora\typora-user-images\image-20230504143706360.png)

###### 4.1.2 作业管理器（JobManager）

​		**JobManager 是一个 Flink 集群中任务管理和调度的核心，是控制应用执行的主进程。**每个应用都应该被唯一的 JobManager 所控制执行。当然，在高可用（HA）的场景下，可能会出现多个 JobManager；这时只有一个是正在运行的领导节点（leader），其他都是备用 节点（standby）。

​		JobManger 又包含 3 个不同的组件

1. JobMaster

​		JobMaster 是 JobManager 中最核心的组件，负责处理单独的作业（Job）。所以 JobMaster 和具体的 Job 是一一对应的，多个 Job 可以同时运行在一个 Flink 集群中, 每个 Job 都有一个 自己的 JobMaster。

2. 资源管理器（ResourceManager）

​		**要把 Flink 内置的 ResourceManager 和其他资源管理平台（比如 YARN）的 ResourceManager 区分开。**

​		ResourceManager 主要负责资源的分配和管理，在 Flink 集群中只有一个。所谓“资源”， 主要是指 TaskManager 的任务槽（task slots）。

3. 分发器（Dispatcher）

​		Dispatcher 主要负责提供一个 REST 接口，用来提交应用，并且负责为每一个新提交的作 业启动一个新的 JobMaster 组件。Dispatcher 也会启动一个 Web UI，用来方便地展示和监控作 业执行的信息。Dispatcher 在架构中并不是必需的，在不同的部署模式下可能会被忽略掉。

###### 4.1.3 任务管理器（TaskManager）

​		TaskManager 是 Flink 中的工作进程，数据流的具体计算就是它来做的，所以也被称为 “Worker”。Flink 集群中必须至少有一个 TaskManager；

​		由于分布式计算的考虑，通常会有多个 TaskManager 运行，每一个 TaskManager 都包含了一定数量的任务槽（task slots）。

​		Slot 是资源调度的最小单位，slot 的数量限制了 TaskManager 能够并行处理的任务数量。 启动之后，TaskManager 会向资源管理器注册它的 slots；收到资源管理器的指令后， TaskManager 就会将一个或者多个槽位提供给 JobMaster 调用，JobMaster就可以分配任务来执行了。 

​		在执行过程中，TaskManager 可以缓冲数据，还可以跟其他运行同一应用的 TaskManager 交换数据。

##### 4.2 作业提交流程

###### 4.2.1 高层级抽象视角

![image-20230504145339717](C:\Users\王海涛\AppData\Roaming\Typora\typora-user-images\image-20230504145339717.png)

（1） 一般情况下，由客户端（App）通过分发器提供的 REST 接口，将作业提交给 JobManager。 

（2）由分发器启动 JobMaster，并将作业（包含 JobGraph）提交给 JobMaster。 

（3）JobMaster 将 JobGraph 解析为可执行的 ExecutionGraph，得到所需的资源数量，然后 向资源管理器请求资源（slots）。 

（4）资源管理器判断当前是否由足够的可用资源；如果没有，启动新的 TaskManager。 

（5）TaskManager 启动之后，向 ResourceManager 注册自己的可用任务槽（slots）。 

（6）资源管理器通知 TaskManager 为新的作业提供 slots。

（7）TaskManager 连接到对应的 JobMaster，提供 slots。 

（8）JobMaster 将需要执行的任务分发给 TaskManager。 

（9）TaskManager 执行任务，互相之间可以交换数据。

###### 4.2.2 独立模式（Standalone）

![image-20230504150208983](C:\Users\王海涛\AppData\Roaming\Typora\typora-user-images\image-20230504150208983.png)

###### 4.2.3 YARN集群

1. **会话（Session）模式**

![image-20230504150302295](C:\Users\王海涛\AppData\Roaming\Typora\typora-user-images\image-20230504150302295.png)

（1）客户端通过 REST 接口，将作业提交给分发器。 

（2）分发器启动 JobMaster，并将作业（包含 JobGraph）提交给 JobMaster。 

（3）JobMaster 向资源管理器请求资源（slots）。 

（4）资源管理器向 YARN 的资源管理器请求 container 资源。 （5）YARN 启动新的 TaskManager 容器。 

（6）TaskManager 启动之后，向 Flink 的资源管理器注册自己的可用任务槽。 

（7）资源管理器通知 TaskManager 为新的作业提供 slots。 

（8）TaskManager 连接到对应的 JobMaster，提供 slots。 

（9）JobMaster 将需要执行的任务分发给 TaskManager，执行任务。

2. **单作业（Per-Job）模式**

​		在单作业模式下，Flink 集群不会预先启动，而是在提交作业时，才启动新的 JobManager

![image-20230504150451139](C:\Users\王海涛\AppData\Roaming\Typora\typora-user-images\image-20230504150451139.png)

（1）客户端将作业提交给 YARN 的资源管理器，这一步中会同时将 Flink 的 Jar 包和配置 上传到 HDFS，以便后续启动 Flink 相关组件的容器。 

（2）YARN 的资源管理器分配 Container 资源，启动 Flink JobManager，并将作业提交给 JobMaster。这里省略了 Dispatcher 组件。 

（3）JobMaster 向资源管理器请求资源（slots）。 

（4）资源管理器向 YARN 的资源管理器请求 container 资源。 （5）YARN 启动新的 TaskManager 容器。 

（6）TaskManager 启动之后，向 Flink 的资源管理器注册自己的可用任务槽。 

（7）资源管理器通知 TaskManager 为新的作业提供 slots。 

（8）TaskManager 连接到对应的 JobMaster，提供 slots。 

（9）JobMaster 将需要执行的任务分发给 TaskManager，执行任务。

3. **应用模式**

​		应用模式与单作业模式的提交流程非常相似，只是初始提交给 YARN 资源管理器的不再是具体的作业，而是整个应用。一个应用中可能包含了多个作业，这些作业都将在 Flink 集群 中启动各自对应的 JobMaster。

##### 4.3 一些重要的概念

###### 4.3.1 数据流图

​		Flink 是流式计算框架。它的程序结构，其实就是定义了一连串的处理操作，每一个数据 输入之后都会依次调用每一步计算。在 Flink 代码中，我们定义的每一个处理转换操作都叫作 “算子”，所以我们的程序可以看作是一串算子构成的管道，数据则像水流一样有序地流过。

​		在运行时，Flink 程序会被映射成所有算子按照逻辑顺序连接在一起的一张图，这被称为 “逻辑数据流”（logical dataflow），或者叫“数据流图”（dataflow graph）。	

​		数据流图类似于任意的有向无环图（DAG），这一点与 Spark 等其他框架是一致的。图中 的每一条数据流（dataflow）以一个或多个 source 算子开始，以一个或多个 sink 算子结束。

![image-20230504162637353](C:\Users\王海涛\AppData\Roaming\Typora\typora-user-images\image-20230504162637353.png)

###### 4.3.2 并行度

​		把一个算子操作，“复制”多份到多个节点， 数据来了之后就可以到其中任意一个执行。这样一来，一个算子任务就被拆分成了多个并行的 “子任务”（subtasks），再将它们分发到不同节点，就真正实现了并行计算。 在 Flink 执行过程中，每一个算子（operator）可以包含一个或多个子任务（operator subtask）， 这些子任务在不同的线程、不同的物理机或不同的容器中完全独立地执行。

![image-20230504162813567](C:\Users\王海涛\AppData\Roaming\Typora\typora-user-images\image-20230504162813567.png)

​		一个特定算子的子任务（subtask）的个数被称之为其并行度（parallelism）。这样，包含并行子任务的数据流，就是并行数据流，它需要多个分区（stream partition）来分配并行任务。 一般情况下，一个流程序的并行度，可以认为就是其所有算子中最大的并行度。一个程序中， 不同的算子可能具有不同的并行度。

**并行度的设置**

（1）代码中设置

​		可以在算子后跟着调用setParallelism(）方法，来设置当前算子的并行度

```
stream.map(word -> Tuple2.of(word,1L)).setParallelism(2);
```

​	这种方式设置的并行度，只针对当前算子有效。

```
env.setParallelism(2);
```

（2）提交应用时设置

```
bin/flink run –p 2 –c com.atguigu.wc.StreamWordCount 
./FlinkTutorial-1.0-SNAPSHOT.jar
```

（3）配置文件中设置

```
parallelism.default: 2
```

###### 4.3.3 算子链

![image-20230504163847477](C:\Users\王海涛\AppData\Roaming\Typora\typora-user-images\image-20230504163847477.png)

1. 算子间的数据传输

​		一个数据流在算子之间传输数据的形式可以是一对一（one-to-one）的直 通 (forwarding)模式，也可以是打乱的重分区（redistributing）模式

（1）一对一（One-to-one，forwarding） 

​		这种模式下，数据流维护着分区以及元素的顺序。比如图中的 source 和 map 算子，source 算子读取数据之后，可以直接发送给 map 算子做处理，它们之间不需要重新分区，也不需要 调整数据的顺序。这就意味着 map 算子的子任务，看到的元素个数和顺序跟 source 算子的子 任务产生的完全一样，保证着“一对一”的关系。map、filter、flatMap 等算子都是这种 one-to-one 的对应关系。 这种关系类似于 Spark 中的窄依赖。 

（2）重分区（Redistributing） 

​		在这种模式下，数据流的分区会发生改变。比图中的 map 和后面的 keyBy/window 算子之 间（这里的 keyBy 是数据传输算子，后面的 window、apply 方法共同构成了 window 算子）, 以及 keyBy/window 算子和 Sink 算子之间，都是这样的关系。 

​		每一个算子的子任务，会根据数据传输的策略，把数据发送到不同的下游目标任务。例如， keyBy()是分组操作，本质上基于键（key）的哈希值（hashCode）进行了重分区；而当并行度 改变时，比如从并行度为 2 的 window 算子，要传递到并行度为 1 的 Sink 算子，这时的数据 传输方式是再平衡（rebalance），会把数据均匀地向下游子任务分发出去。这些传输方式都会 引起重分区（redistribute）的过程，这一过程类似于 Spark 中的 shuffle。 

​		总体说来，这种算子间的关系类似于 Spark 中的宽依赖。

2. 合并算子链

在 Flink 中，**并行度相同的一对一（one to one）算子操作，可以直接链接在一起形成一个 “大”的任务（task）**，这样原来的算子就成为了真正任务里的一部分。每个 task会被一个线程执行。这样的技术被称为“算子链”（Operator Chain）。

###### 4.3.4 作业图和执行图

Flink 中任务调度执行的图，按照生成顺序可以分成四层： 

​	逻辑流图（StreamGraph）→ 作业图（JobGraph）→ 执行图（ExecutionGraph）→ 物理 图（Physical Graph）

![image-20230504164138196](C:\Users\王海涛\AppData\Roaming\Typora\typora-user-images\image-20230504164138196.png)

1. 逻辑流图（StreamGraph）

​		这是**根据用户通过 DataStream API 编写的代码生成的最初的 DAG 图，用来表示程序的拓扑结构**。这一步一般在客户端完成

​		**逻辑流图中的节点，完全对应着代码中的四步算子操作**：

​	 源算子 Source（socketTextStream()）→扁平映射算子 Flat Map(flatMap()) →分组聚合算子 Keyed Aggregation(keyBy/sum()) →输出算子 Sink(print())。

2. 作业图（JobGraph）

​		StreamGraph 经过优化后生成的就是作业图（JobGraph），这是提交给 JobManager 的数据 结构，确定了当前作业中所有任务的划分。**主要的优化为: 将多个符合条件的节点链接在一起 合并成一个任务节点，形成算子链，这样可以减少数据交换的消耗。**

3. 执行图（ExecutionGraph）

​		JobMaster 收到 JobGraph 后，会根据它来生成执行图（ExecutionGraph）。ExecutionGraph 是 JobGraph 的并行化版本，是调度层最核心的数据结构。

​		**与 JobGraph 最大的区别就是按照并行度对并行子任务进行了拆分， 并明确了任务间数据传输的方式。**

4. 物理图（Physical Graph）

​		JobMaster 生成执行图后， 会将它分发给 TaskManager；各个 TaskManager 会根据执行图 部署任务，最终的物理执行过程也会形成一张“图”，一般就叫作物理图（Physical Graph）。 这只是具体执行层面的图，并不是一个具体的数据结构。

### 第 5 章 DataStream API（基础篇）

​		Flink 有非常灵活的分层 API 设计，其中的核心层就是 DataStream/DataSet API。

一个 Flink 程序，其实就是对 DataStream 的各种转换。具体来说，代码基本上都由以下几 部分构成，

（1）获取执行环境（execution environment） 

（2） 读取数据源（source） 

（3）定义基于数据的转换操作（transformations） 

（4）定义计算结果的输出位置（sink） 

（5）触发程序执行（execute） 其中，获取环境和触发执行，都可以认为是针对执行环境的操作。

![image-20230504165559594](C:\Users\王海涛\AppData\Roaming\Typora\typora-user-images\image-20230504165559594.png)

##### 5.1 执行环境（Execution Environment）

​		不同的环境，代码的提交运行的过程会有所不同。这就要求我们在提交作业执行计算时， 首先必须获取当前 Flink 的运行环境，从而建立起与 Flink 框架之间的联系。只有获取了环境 上下文信息，才能将具体的任务调度到不同的 TaskManager 执行。

###### 5.1.1 创建执行环境

1. getExecutionEnvironment

​		直接调用 getExecutionEnvironment 方法。它会根据当前运行的上下文 直接得到正确的结果：如果程序是独立运行的，就返回一个本地执行环境；如果是创建了 jar 包，然后从命令行调用它并提交到集群执行，那么就返回集群的执行环境。相当于自适应的效果

```
StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
```

2. createLocalEnvironment

​		这个方法返回一个本地执行环境。可以在调用时传入一个参数，指定默认的并行度；如果 不传入，则默认并行度就是本地的 CPU 核心数。

```
StreamExecutionEnvironment localEnv = StreamExecutionEnvironment.createLocalExecutionEnvironment();
```

3. createRemoteEnvironment

​		这个方法返回集群执行环境，需要在调用时指定JobManager的主机名和端口号，并指定要在集群中运行的jar包

```
StreamExecutionEnvironment remoteEnv = 		StreamExecutionEnvironment
 .createRemoteEnvironment(
 "host", // JobManager 主机名
 1234, // JobManager 进程端口号
 "path/to/jarFile.jar" // 提交给 JobManager 的 JAR 包
); 
```

###### 5.1.2 执行模式（Execution Mode）

​		批处理的执行环境与流处理类似，是调用类 ExecutionEnvironment 的静态方法，返回它的对象：

```
// 批处理环境
ExecutionEnvironment batchEnv = ExecutionEnvironment.getExecutionEnvironment();
// 流处理环境
StreamExecutionEnvironment env = 
StreamExecutionEnvironment.getExecutionEnvironment();
```

​		而从 1.12.0 版本起，Flink 实现了 API 上的流批统一。DataStream API 新增了一个重要特性：**可以支持不同的“执行模式”（execution mode），通过简单的设置就可以让一段 Flink 程序 在流处理和批处理之间切换。**

（1）流执行模式（Streaming）

​		这是 DataStream API 最经典的模式，一般用于需要持续实时处理的无界数据流。默认情况下，程序使用的就是 STREAMING 执行模式。

（2）批执行模式（Batch）

​		专门用于批处理的执行模式, 这种模式下，Flink 处理作业的方式类似于 MapReduce 框架。 对于不会持续计算的有界数据，我们用这种模式处理会更方便。

（3）自动模式（Automatic）

​		在这种模式下，将由程序根据输入数据源是否有界，来自动选择执行模式。

1. BATCH模式的配置方法

（1）通过命令行配置

```
bin/flink run -Dexecution.runtime-mode=BATCH ... 
```

在提交作业时，增加 execution.runtime-mode 参数，指定值为 BATCH。 

（2）通过代码配置 

```
StreamExecutionEnvironment env =  StreamExecutionEnvironment.getExecutionEnvironment(); env.setRuntimeMode(RuntimeExecutionMode.BATCH);
```

 		在代码中，直接基于执行环境调用 setRuntimeMode 方法，传入 BATCH 模式。

​		建议: 不要在代码中配置，而是使用命令行。这同设置并行度是类似的：在提交作业时指 定参数可以更加灵活，同一段应用程序写好之后，既可以用于批处理也可以用于流处理。而在 代码中硬编码（hard code）的方式可扩展性比较差，一般都不推荐。

###### 5.1.3 触发程序执行

​		写完输出（sink）操作并不代表程序已经结束。因为当 main()方法被调用 时，其实只是定义了作业的每个执行操作，然后添加到数据流图中；这时并没有真正处理数据 ——因为数据可能还没来。

​		**Flink 是由事件驱动的，只有等到数据到来，才会触发真正的计算， 这也被称为“延迟执行”或“懒执行”（lazy execution）。**

​		 所以我们需要显式地调用执行环境的 execute()方法，来触发程序执行。execute()方法将一 直等待作业完成，然后返回一个执行结果（JobExecutionResult）。

```
env.execute();
```

##### 5.2 源算子（Source）

​		创建环境之后，就可以构建数据处理的业务逻辑了，想要处理数据，先得有数据，所以首要任务就是把数据读进来。

​		Flink 可以从各种来源获取数据，然后构建 DataStream 进行转换处理。一般将数据的输入来源称为数据源(data source)，而读取数据的算子就是源算子（source operator）。所以，source 就是我们整个处理程序的输入端。

​		Flink 代码中通用的添加 source 的方式，是调用执行环境的 addSource()方法：

``` 
DataStream<String> stream = env.addSource();
```

###### 5.2.1 准备工作

​		为了更好地理解，我们先构建一个实际应用场景。比如网站的访问操作，可以抽象成一个三元组（用户名，用户访问的 urrl，用户访问 url 的时间戳），所以在这里，我们可以创建一个 类 Event，将用户行为包装成它的一个对象。Event 包含了以下一些字段

![image-20230505104341104](C:\Users\王海涛\AppData\Roaming\Typora\typora-user-images\image-20230505104341104.png)

```
public class Event {
    public String user;
    public String url;
    public  Long timestamp;

    public Event() {
    }

    public Event(String user, String url, Long timestamp) {
        this.user = user;
        this.url = url;
        this.timestamp = timestamp;
    }

    @Override
    public String toString() {
        return "Event{" +
                "user='" + user + '\'' +
                ", url='" + url + '\'' +
                ", timestamp=" + new Timestamp(timestamp) +
                '}';
    }
}
```

###### 5.2.2 从集合中读取数据

​		最简单的读取数据的方式，就是在代码中直接创建一个 Java 集合，然后调用执行环境的 fromCollection 方法进行读取。这相当于将数据临时存储到内存中，形成特殊的数据结构后， 作为数据源使用，一般用于测试。

​		**调用方法：fromCollection（）**

```
ArrayList<Integer> nums = new ArrayList<>();
nums.add(2);
nums.add(5);
DataStreamSource<Integer> numStream = env.fromCollection(nums);

ArrayList<Event> events = new ArrayList<>();
events.add(new Event("Mary","./home",1000L));
events.add(new Event("Bob","./cart",2000L));
DataStreamSource<Event> stream2 = env.fromCollection(events);
```

###### 5.2.3 从文件中读取数据

```
DataStreamSource<String> stream1 = env.readTextFile("input/clicks.txt");
```

###### 5.2.4 从元素中读取数据

```
DataStreamSource<Boolean> stream3 = env.fromElements(
        events.add(new Event("Mary", "./home", 1000L)),
        events.add(new Event("Bob", "./cart", 2000L))
);

stream3.print("3");
```

###### 5.2.5 从Socket中读取数据

```
DataStreamSource<String> stream4 = env.socketTextStream("hadoop102", 7777);

stream4.print("4");
```

###### 5.2.6 从Kafka中读取数据

​		Kafka 作为分布式消息传输队列，是一个高吞吐、易于扩展的消息系统。而消息队列的传 输方式，恰恰和流处理是完全一致的。所以可以说 Kafka 和 Flink 天生一对，是当前处理流式 数据的双子星。在如今的实时流处理应用中，由 Kafka 进行数据的收集和传输，Flink 进行分析计算，这样的架构已经成为众多企业的首选

![image-20230505145422146](C:\Users\王海涛\AppData\Roaming\Typora\typora-user-images\image-20230505145422146.png)

引入 Kafka 连接器的依赖：

```
<dependency>
 <groupId>org.apache.flink</groupId>
 <artifactId>flink-connector-kafka_${scala.binary.version}</artifactId>
 <version>${flink.version}</version>
</dependency>
```

然后调用env.addSource(),传入FlinkKafkaConsumer的对象实例

```
Properties properties = new Properties();
properties.setProperty("bootstrap.servers","hadoop102:9092");
properties.setProperty("group.id","consumer-group");
properties.setProperty("key.deserializer","org.apache.kafka.common.serialization.StringDeserializer");
properties.setProperty("value.deserializer","org.apache.kafka.common.serialization.StringDeserializer");
properties.setProperty("auto.offset.reset","latest");

DataStreamSource<String> kafkaStream = env.addSource(new FlinkKafkaConsumer<String>("clicks", new SimpleStringSchema(), properties));

kafkaStream.print();
```

其中，创建FlinkKafkaConsumer时需要传入的三个参数：

1. 第一个参数 topic，定义了从哪些主题中读取数据。可以是一个 topic，也可以是 topic 列表，还可以是匹配所有想要读取的 topic 的正则表达式。当从多个 topic 中读取数据 时，Kafka 连接器将会处理所有 topic 的分区，将这些分区的数据放到一条流中去。

2. 第二个参数是一个 DeserializationSchema 或者 KeyedDeserializationSchema。Kafka 消 息被存储为原始的字节数据，所以需要反序列化成 Java 或者 Scala 对象。上面代码中 使用的 SimpleStringSchema，是一个内置的 DeserializationSchema，它只是将字节数 组简单地反序列化成字符串。DeserializationSchema 和 KeyedDeserializationSchema 是 公共接口，所以我们也可以自定义反序列化逻辑。

3. 第三个参数是一个 Properties 对象，设置了 Kafka 客户端的一些属性。

###### 5.2.7 自定义Source

​		想要读取的数据源来自某个外部系统，而 flink 既没有预实现的方法，只能通过自定义实现 SourceFunction 。

​		创建一个自定义的数据源，实现 SourceFunction 接口。主要重写两个关键方法： run()和 cancel()。 

⚫ run()方法：使用运行时上下文对象（SourceContext）向下游发送数据； 

⚫ cancel()方法：通过标识位控制退出循环，来达到中断数据源的效果。

自定义数据源：

```
public class ClickSource implements SourceFunction<Event> {
    //声明标志位
    private Boolean running = true;
    @Override
    public void run(SourceContext<Event> ctx) throws Exception {
        //随机生成数据
        Random random = new Random();
        //定义字段选取的数据集
        String[] users = {"Mary","Alice","Bob","Cary"};
        String[] urls = {"./home","./cart","./fav","./prod?id=100","./prod?id=10"};

        //循环生成数据
        while (running){
            String user = users[random.nextInt(users.length)];
            String url = urls[random.nextInt(users.length)];
            Long timestamp = Calendar.getInstance().getTimeInMillis();
            ctx.collect(new Event(user,url,timestamp));
            Thread.sleep(1000L);
        }
    }

    @Override
    public void cancel() {
            running = false;
    }
```

主方法调用addSource，传入ClickSource实例

```
public class SourceCustom {
 public static void main(String[] args) throws Exception {
 StreamExecutionEnvironment env = 
StreamExecutionEnvironment.getExecutionEnvironment();
 env.setParallelism(1);
//有了自定义的 source function，调用 addSource 方法
 DataStreamSource<Event> stream = env.addSource(new ClickSource());
 stream.print("SourceCustom");
 env.execute();
 }
 
}
```

5.2.8 Flink支持的数据类型

1. Flink的类型系统

​		为了方便地处理数据，Flink 有自己一整套类型系统。Flink 使用“类型信息” （TypeInformation）来统一表示数据类型。				   		TypeInformation 类是 Flink 中所有类型描述符的基类。 它涵盖了类型的一些基本属性，并为每个数据类型生成特定的序列化器、反序列化器和比较器。

2. Fli支持的数据类型

（1）基本类型 

​		所有 Java 基本类型及其包装类，再加上 Void、String、Date、BigDecimal 和 BigInteger。

（2）数组类型

​		包括基本类型数组（PRIMITIVE_ARRAY）和对象数组(OBJECT_ARRAY)

（3）复合数据类型

​	①Java 元组类型（TUPLE）：这是 Flink 内置的元组类型，是 Java API 的一部分。最多 25 个字段，也就是从 Tuple0~Tuple25，不支持空字段 

​	②Scala 样例类及 Scala 元组：不支持空字段 

​	③行类型（ROW）：可以认为是具有任意个字段的元组,并支持空字段 

​	④POJO：Flink 自定义的类似于 Java bean 模式的类

（4）辅助类型

​	Option、Either、List、Map 等

（5）泛型类型

​		Flink 支持所有的 Java 类和 Scala 类。不过如果没有按照上面 POJO 类型的要求来定义， 就会被 Flink 当作泛型类来处理。Flink 会把泛型类型当作黑盒，无法获取它们内部的属性；它 们也不是由 Flink 本身序列化的，而是由 Kryo 序列化的。

3. 类型提示（Type Hints）

​		Flink 还具有一个类型提取系统，可以分析函数的输入和返回类型，自动获取类型信息， 从而获得对应的序列化器和反序列化器。

​		Flink 专门提供了 TypeHint 类，它可以捕获泛型的类型信息，并且一直记录下来，为运行时提供足够的信息。我们同样可以通过.returns()方法，明确地指定转换之后的 DataStream 里元素的类型。

```
returns(new TypeHint<Tuple2<Integer, SomeType>>(){})
```

##### 5.3 转换算子

![image-20230505162343342](C:\Users\王海涛\AppData\Roaming\Typora\typora-user-images\image-20230505162343342.png)

​		数据源读入数据之后，我们就可以使用各种转换算子，将一个或多个 DataStream 转换为 新的 DataStream，一个 Flink 程序的核心，其实就是所有的转换操作，它们决定了处理的业务逻辑。

​		我们可以针对一条流进行转换处理，也可以进行分流、合流等多流转换操作，从而组合成 复杂的数据流拓扑。

###### 5.3.1 基本转换算子

1. 映射（map）

​		map 是大家非常熟悉的大数据操作算子，主要用于将数据流中的数据进行转换，形成新的 数据流。简单来说，就是一个“一一映射”，消费一个元素就产出一个元素

![image-20230506105107338](C:\Users\王海涛\AppData\Roaming\Typora\typora-user-images\image-20230506105107338.png)

```
StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
    env.setParallelism(1);

    // 从元素中读取数据
    DataStreamSource<Event> stream = env.fromElements(new Event("Mary","./home",1000L),
            new Event("Bob","./cart",2000L),
            new Event("Alice","./prod?id=100",3000L));

    //进行转换计算，提取user字段
    //1.使用自定义类，实现MapFunction接口
    SingleOutputStreamOperator<String> result = stream.map(new MyMapper());

    //2.使用匿名类实现MapFUnction接口
    SingleOutputStreamOperator<String> stream2 = stream.map(new MapFunction<Event, String>() {
        @Override
        public String map(Event value) throws Exception {
            return value.user;
        }
    });

    //3.传入Lambda表达式
    SingleOutputStreamOperator<String> result3 = stream.map(data -> data.user);
    result3.print();
    result.print();

    env.execute();
}
//自定义MapFunction
public static class MyMapper implements MapFunction<Event,String>{
    @Override
    public String map(Event value) throws Exception {
        return value.user;
    }
}
```

2. 过滤（filter）

​		filter转换操作，就是对数据流执行一个过滤，通过一个布尔条件表达式设置过滤条件，对于每一个流内元素进行判断，若为true则元素正常输出，若为false则元素被过滤掉

![image-20230506105444373](C:\Users\王海涛\AppData\Roaming\Typora\typora-user-images\image-20230506105444373.png)

```
package com.wanghaitao.chapter05;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class TransformFilterTest {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // 从元素中读取数据
        DataStreamSource<Event> stream = env.fromElements(
                new Event("Mary","./home",1000L),
                new Event("Bob","./cart",2000L),
                new Event("Alice","./prod?id=100",3000L));

        //1.传入一个实现filterFunction的自定义对象
        SingleOutputStreamOperator<Event> result1 = stream.filter(new MyFilter());

        //2.传入一个匿名类实现FilterFunction接口
        SingleOutputStreamOperator<Event> result2 = stream.filter(new FilterFunction<Event>() {
            @Override
            public boolean filter(Event event) throws Exception {
                return event.user.equals("Bob");
            }
        });

        //3. 传入Lambda表达式
        SingleOutputStreamOperator<Event> result3 = stream.filter(data -> data.user.equals("Mary"));


        result1.print();
        result2.print();
        result3.print();

        env.execute();
    }

    //实现一个自定义的FilterFunction
    public static class MyFilter implements FilterFunction<Event>{
        @Override
        public boolean filter(Event event) throws Exception {
            return  event.user.equals("Mary");
        }
    }
}
```

3. 扁平映射（flatMap）

​		flatMap 操作又称为扁平映射，主要是将数据流中的整体（一般是集合类型）拆分成一个一个的个体使用。消费一个元素，可以产生 0 到多个元素。flatMap 可以认为是“扁平化”（flatten） 和“映射”（map）两步操作的结合，也就是先按照某种规则对数据进行打散拆分，再对拆分后的元素做转换处理，我们此前 WordCount 程序的第一步分词操作，就用到了 flatMap。

![image-20230506155740208](C:\Users\王海涛\AppData\Roaming\Typora\typora-user-images\image-20230506155740208.png)

​		flatMap 操作会应用在每一个输入事件上面，FlatMapFunction 接口中定义了 flatMap 方法， 用户可以重写这个方法，在这个方法中对输入数据进行处理，并决定是返回 0 个、1 个或多个 结果数据。因此 flatMap 并没有直接定义返回值类型，而是通过一个“收集器”（Collector）来 指定输出。希望输出结果时，只要调用收集器的.collect()方法就可以了；这个方法可以多次调 用，也可以不调用。所以 flatMap 方法也可以实现 map 方法和 filter 方法的功能，当返回结果是0个的时候，就相当于对数据进行了过滤，当返回结果是 1 个的时候，相当于对数据进行了 简单的转换操作。

###### 5.3.2 聚合算子

​		之前 word count 程序中，要对每个词出现的频次进行叠加统计。这种操作，计算 的结果不仅依赖当前数据，还跟之前的数据有关，相当于要把所有数据聚在一起进行汇总合并 ——这就是所谓的“聚合”（Aggregation），也对应着 MapReduce 中的 reduce 操作。

1. 按键分区（keyBy）

​		对于 Flink 而言，DataStream 是没有直接进行聚合的 API 的。因为我们对海量数据做聚合 肯定要进行分区并行处理，这样才能提高效率。所以在 Flink 中，**要做聚合，需要先进行分区**； 这个操作就是通过 keyBy 来完成的

​		keyBy 是聚合前必须要用到的一个算子。keyBy 通过指定键（key），可以将一条流从逻辑 上划分成不同的分区（partitions）。这里所说的分区，其实就是并行处理的子任务，也就对应 着任务槽（task slot）。

​		![image-20230506190116138](C:\Users\王海涛\AppData\Roaming\Typora\typora-user-images\image-20230506190116138.png)

​		keyBy()方法需要传入一个参数，这个参数指定了一个或一组 key。有很多不同的方法来指定 key：比如对于 Tuple 数据类型，可以指定字段的位置或者多个位置的组合；对于 POJO 类 型，可以指定字段的名称（String）；另外，还可以传入 Lambda 表达式或者实现一个键选择器 （KeySelector），用于说明从数据中提取 key 的逻辑。

​		keyBy 得到的结果将不再是 DataStream，而是会将 DataStream 转换为 KeyedStream。KeyedStream 可以认为是“分区流”或者“键控流”，它是对 DataStream 按照 key 的一个逻辑分区，所以泛型有两个类型：除去当前流中的元素类型外，还需要指定 key 的 类型。	

2. 简单聚合

Flink 为我们 内置实现了一些最基本、最简单的聚合 API，主要有以下几种： 

⚫ sum()：在输入流上，对指定的字段做叠加求和的操作。 

⚫ min()：在输入流上，对指定的字段求最小值。 

⚫ max()：在输入流上，对指定的字段求最大值。 

⚫ minBy()：与 min()类似，在输入流上针对指定字段求最小值。不同的是，min()只计 算指定字段的最小值，其他字段会保留最初第一个数据的值；而 **minBy()则会返回包含字段最小值的整条数据。** 

⚫ maxBy()：与 max()类似，在输入流上针对指定字段求最大值。**两者区别与 min()/minBy()完全一致。**

```
// 按键分区之后进行聚合，提取当前用户最后一次访问的数据
//1.传入匿名类
stream.keyBy(new KeySelector<Event, String>() {
    @Override
    public String getKey(Event event) throws Exception {
        return event.user;
    }
}).max("timestamp")
                .print("max: ");

//2.Lambda表达式
stream.keyBy(data -> data.user)
                .maxBy("timestamp")
                        .print("maxBy: ");
```

3. 归约聚合（reduce）

​		

```
package com.wanghaitao.chapter05;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class TransformReduceTest {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(1);

        // 从元素中读取数据
        DataStreamSource<Event> stream = env.fromElements(
                new Event("Mary","./home",1000L),
                new Event("Bob","./cart",2000L),
                new Event("Alice","./prod?id=100",3000L),
                new Event("Bob","./prod?id=1",3300L),
                new Event("Bob","./home",3500L),
                new Event("Bob","./prod?id=1",3800L),
                new Event("Bob","./prod?id=1",4200L)
        );

        //1.统计每个用户的访问频次
        SingleOutputStreamOperator<Tuple2<String, Long>> clicksByUser = stream.map(new MapFunction<Event, Tuple2<String, Long>>() {
                    @Override
                    public Tuple2<String, Long> map(Event event) throws Exception {
                        return Tuple2.of(event.user,1L);
                    }
                }).keyBy(data -> data.f0)
                .reduce(new ReduceFunction<Tuple2<String, Long>>() {
                    @Override
                    public Tuple2<String, Long> reduce(Tuple2<String, Long> value1, Tuple2<String, Long> value2) throws Exception {
                        return Tuple2.of(value1.f0, value1.f1 + value2.f1);
                    }
                });

        //2.选取当前最活跃的用户
        SingleOutputStreamOperator<Tuple2<String, Long>> result = clicksByUser.keyBy(data -> "key")
                .reduce(new ReduceFunction<Tuple2<String, Long>>() {
                    @Override
                    public Tuple2<String, Long> reduce(Tuple2<String, Long> value1, Tuple2<String, Long> value2) throws Exception {
                        return value1.f1 > value2.f1 ? value1 : value2;
                    }
                });

        result.print();

        env.execute();
    }
}
```

###### 5.3.3 用户自定义函数（UDF）

​		Flink 的 DataStream API 编程风格其实是一致的：基本上都是基于 DataStream 调用一个方法，表示要做一个转换操作；方法需要传入一个参数，这个参数都是需要实现一个接口。

​		这些接口有一个共同特点：全部都以算子操作名称 + Function 命名，例如 源算子需要实现 SourceFunction 接口，map 算子需要实现 MapFunction 接口，reduce 算子需要 实现 ReduceFunction 接口。它们都继承自 Function 接口;

​		接下来我们就对这几种编程方式做一个梳理总结。

1. 函数类（Function Classes）

​		自定义一个函数类，实现对应的接口

```
//1.使用自定义类，实现MapFunction接口
SingleOutputStreamOperator<String> result = stream.map(new MyMapper());


    //自定义MapFunction
    public static class MyMapper implements MapFunction<Event,String>{
        @Override
        public String map(Event value) throws Exception {
            return value.user;
        }
    }
```

2. 匿名函数

   ```
   DataStream stream = clicks.filter(new FilterFunction() { @Override public boolean filter(Event value) throws Exception { return value.url.contains("home"); } }); 
   ```

3. Lambda

​		Lambda 表达式允许以简洁的方式实现函数，以及将函数作为参数来进行传递，而不必声明额外的（匿名）类。 Flink 的所有算子都可以使用 Lambda 表达式的方式来进行编码，但是，**当 Lambda 表 达式使用 Java 的泛型时，我们需要显式的声明类型信息**。

```
SingleOutputStreamOperator<String> result3 = stream.map(data -> data.user);
```

​		由于 OUT是STring类型不是泛型，所以Flink可以从函数签名中自动提取出结果的类型

​		但对于像flapMap（）这样的函数，它的函数签名void flatMap(IN value, Collector  out) 被 Java 编译器编译成了 void flatMap(IN value, Collector out)，也就是说将 Collector 的泛 型信息擦除掉了。这样 Flink 就无法自动推断输出的类型信息了。

​		在这种情况下，我们需要**显式地指定类型信息**，否则输出将被视为 Object 类型，这会导致低效的序列化

1)直接声明具体的类型

```
.returns(Types.STRING);
```

2）通过类型提示获取类型

```
.returns(new TypeHint<String>() {})
```

###### 5.3.4 富函数

​		“富函数类”也是 DataStream API 提供的一个函数类的接口，所有的 Flink 函数类都有其 Rich 版本。富函数类一般是以抽象类的形式出现的。例如：RichMapFunction、RichFilterFunction、 RichReduceFunction 等。 

​		既然“富”，那么它一定会比常规的函数类提供更多、更丰富的功能。与常规函数类的不同主要在于，**富函数类可以获取运行环境的上下文，并拥有一些生命周期方法，所以可以实现更复杂的功能。**

​		注：生命周期的概念在编程中其实非常重要，到处都有体现。例如：对于 C 语言来说， 我们需要手动管理内存的分配和回收，也就是手动管理内存的生命周期。分配内存而不回收， 会造成内存泄漏，回收没有分配过的内存，会造成空指针异常。而在 JVM 中，虚拟机会自动 97 帮助我们管理对象的生命周期。对于前端来说，一个页面也会有生命周期。数据库连接、网络 连接以及文件描述符的创建和关闭，也都形成了生命周期。所以生命周期的概念在编程中是无 处不在的，需要我们多加注意。	

​		Rich Function 有生命周期的概念。典型的生命周期方法有： 

⚫ open()方法，是 Rich Function 的初始化方法，也就是会开启一个算子的生命周期。当 一个算子的实际工作方法例如 map()或者 filter()方法被调用之前，open()会首先被调 用。所以像文件 IO 的创建，数据库连接的创建，配置文件的读取等等这样一次性的工作，都适合在 open()方法中完成。 

⚫ close()方法，是生命周期中的最后一个调用的方法，类似于解构方法。一般用来做一 些清理工作。 

​		需要注意的是，这里的生命周期方法，对于一个并行子任务来说只会调用一次；而对应的， 实际工作方法，例如 RichMapFunction 中的 map()，在每条数据到来后都会触发一次调用。

```
    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
    env.setParallelism(1);

    // 从元素中读取数据
    DataStreamSource<Event> stream = env.fromElements(
            new Event("Mary","./home",1000L),
            new Event("Bob","./cart",2000L),
            new Event("Alice","./prod?id=100",3000L));

    stream.map(new MyRichMapper()).print();

    env.execute();
}
//实现一个自定义的富函数类
public static class MyRichMapper extends RichMapFunction<Event,Integer>{
    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        System.out.println("open生命周期被调用" + getRuntimeContext().getIndexOfThisSubtask() +"号任务启动了");
    }

    @Override
    public void close() throws Exception {
        super.close();
        System.out.println("close生命周期被调用" + getRuntimeContext().getIndexOfThisSubtask() +"号任务启动了");

    }

    @Override
    public Integer map(Event event) throws Exception {
        return event.url.length();
    }
```

###### 5.3.5 物理分区（Physical Partitioning）

​		“分区”（partitioning）操作就是要将数据进行重新分布，传递到不同的流分区 去进行下一步处理。

​		keyBy， 它就是一种按照键的哈希值来进行重新分区的操作。只不过这种分区操作只能保证把数据按 key“分开”，至于分得均不均匀、每个 key 的数据具体会分到哪一区去，这些是完全无从控制 的

​		有些时候，我们还需要手动控制数据分区分配策略。比如当发生数据倾斜的时候，系统无 法自动调整，这时就需要我们重新进行负载均衡，将数据流较为平均地发送到下游任务操作分 区中去。Flink 对于经过转换操作之后的 DataStream，提供了一系列的底层操作接口，能够帮 我们实现数据流的手动重分区。为了同 keyBy 相区别，我们把这些操作统称为“物理分区” 操作。

1. 随机分区

​		最简单的重分区方式就是直接“洗牌”。通过调用 DataStream 的.shuffle()方法，将数据随机地分配到下游算子的并行任务中去。 1 随机分区服从均匀分布（uniform distribution），所以可以把流中的数据随机打乱，均匀地传递到下游任务分区，如图所示。因为是完全随机的，所以对于同样的输入数据, 每次执 行得到的结果也不会相同。

![image-20230507105735936](C:\Users\王海涛\AppData\Roaming\Typora\typora-user-images\image-20230507105735936.png)

```
stream.shuffle().print().setParallelism(4);
```

2. 轮询分区

​		轮询也是一种常见的重分区方式。简单来说就是“发牌”，按照先后顺序将数据做依次分发，如图所示。通过调用 DataStream的.rebalance()方法，就可以实现轮询重分区。rebalance 使用的是 Round-Robin 负载均衡算法，可以将输入流数据平均分配到下游的并行任务中去。

![image-20230507140953345](C:\Users\王海涛\AppData\Roaming\Typora\typora-user-images\image-20230507140953345.png)

```
stream.rebalance().print().setParallelism(4);
```

3. 重缩放分区（rescale）

​		重缩放分区和轮询分区非常相似。当调用 rescale()方法时，其实底层也是使用 Round-Robin 算法进行轮询，但是只会将数据轮询发送到下游并行任务的一部分中，也就是说，“发牌人”如果有多个，那么 rebalance 的方式是每个发牌人都面向所有人发牌；而 rescale 的做法是分成小团体，发牌人只给自己团体内的所有人轮流发牌。

![image-20230507141115336](C:\Users\王海涛\AppData\Roaming\Typora\typora-user-images\image-20230507141115336.png)

```
env.addSource(new RichParallelSourceFunction<Integer>() {
            @Override
            public void run(SourceContext<Integer> ctx) throws Exception {
                for (int i = 1; i <= 8; i++) {
                    //将奇偶数分别发送到0号和1号分区
                    if (i % 2 == getRuntimeContext().getIndexOfThisSubtask()) {
                        ctx.collect(i);
                    }
                }
            }

            @Override
            public void cancel() {

            }
        }).setParallelism(2)
                        .rescale()
                                .print()
                                        .setParallelism(4);
```

4. 广播（broadcast）

​		这种方式其实不应该叫做“重分区”，**因为经过广播之后，数据会在不同的分区都保留一份，可能进行重复处理**。可以通过调用 DataStream 的 broadcast()方法，将输入数据复制并发送 到下游算子的所有并行任务中去。

```
stream.broadcast().print().setParallelism(4);
```

5. 全局分区（global）

​		全局分区也是一种特殊的分区方式。这种做法非常极端，通过调用.global()方法，会将所有的输入流数据都发送到下游算子的第一个并行子任务中去。这就相当于强行让下游任务并行 度变成了 1，所以使用这个操作需要非常谨慎，可能对程序造成很大的压力。

```
stream.global().print().setParallelism(4);
```

6. 自定义分区（Custom）

​		当 Flink 提 供 的 所 有 分 区 策 略 都 不 能 满 足 用 户 的 需 求 时 ， 我 们 可 以 通 过 使 用 partitionCustom()方法来自定义分区策略。 

​		在调用时，方法需要传入两个参数，第一个是自定义分区器（Partitioner）对象，第二个是应用分区器的字段，它的指定方式与 keyBy 指定 key 基本一样：可以通过字段名称指定，也可以通过字段位置索引来指定，还可以实现一个KeySelector。	

```
  env.fromElements(1,2,3,4,5,6,7,8)  	  .partitionCustom(new Partitioner<Integer>() {
                            @Override
  public int partition(Integer key, int i) {
     return key % 2 ;
  }
    }, new KeySelector<Integer, Integer>() {
 @Override
  public Integer getKey(Integer integer) throws Exception {
  return integer;
  }
  })                              			  .print().setParallelism(4);
        env.execute();
```

##### 5.4 输出算子（Sink）

![image-20230507143248756](C:\Users\王海涛\AppData\Roaming\Typora\typora-user-images\image-20230507143248756.png)

​		Flink作为数据处理框架，最终还是要把计算处理的结果写入外部存储中，为外部应用提供支持

###### 5.4.1 连接到外部系统

​		Flink 的 DataStream API 专门提供了向外部写入数据的方法： addSink。与 addSource 类似，addSink 方法对应着一个“Sink”算子，主要就是用来实现与外 部系统连接、并将数据提交写入的；Flink 程序中所有对外的输出操作，一般都是利用 Sink 算子完成的。

​		之前我们一直在使用的 print 方法其实就是一种 Sink，它表示将数据流写入标准控制台打 印输出。

​		与 Source 算子非常类似，除去一些 Flink 预实现的 Sink，一般情况下 Sink 算子的创建是 通过调用 DataStream 的.addSink()方法实现的。

```
stream.addSink(new SinkFunction(…));
```

​		addSource 的参数需要实现一个 SourceFunction 接口；类似地，addSink 方法同样需要传入 一个参数，实现的是 SinkFunction 接口。在这个接口中只需要重写一个方法 invoke(),**用来将指定的值写入到外部系统中。这个方法在每条数据记录到来时都会调用：**

```
default void invoke(IN value, Context context) throws Exception
```

###### 5.4.2 输出到文件

​		Flink 为此专门提供了一个流式文件系统的连接器：StreamingFileSink，它继承自抽象类 RichSinkFunction，而且集成了 Flink 的检查点（checkpoint）机制，用来保证精确一次（exactly  once）的一致性语义。

​		StreamingFileSink 为批处理和流处理提供了一个统一的 Sink，它可以将分区文件写入 Flink 支持的文件系统。它可以保证精确一次的状态一致性，大大改进了之前流式文件 Sink 的方式。 它的主要操作是将数据写入桶（buckets），每个桶中的数据都可以分割成一个个大小有限的分 区文件，这样一来就实现真正意义上的分布式文件存储。

​		StreamingFileSink 支持行编码（Row-encoded）和批量编码（Bulk-encoded，比如 Parquet） 格式。这两种不同的方式都有各自的构建器（builder），调用方法也非常简单，可以直接调用 StreamingFileSink 的静态方法： 

⚫ 行编码：

```
StreamingFileSink.forRowFormat（basePath，rowEncoder）
```

⚫ 批量编码：

```
StreamingFileSink.forBulkFormat（basePath，bulkWriterFactory）
```

​		在创建行或批量编码 Sink 时，我们需要传入两个参数，用来指定存储桶的基本路径 （basePath）和数据的编码逻辑（rowEncoder 或 bulkWriterFactory）。

**案例实操：new 一个StreamingFileSink并实现里面的forRowFormat方法，然后添加到addSink中**

```
StreamingFileSink<String> streamingFileSink = StreamingFileSink.<String>forRowFormat(new Path("./output"),
                new SimpleStringEncoder<>("UTF-8"))
        .withRollingPolicy(
                DefaultRollingPolicy.builder()
                        .withMaxPartSize(1024 * 1024)
                        .withRolloverInterval(TimeUnit.MINUTES.toMillis(15))
                        .withInactivityInterval(TimeUnit.MINUTES.toMillis(5))
                        .build()
        )
        .build();
stream.map(data ->data.toString())
        .addSink(streamingFileSink);
```

​		通过.withRollingPolicy()方法指定了一个“滚动策略”。“滚动”的概念在日志文件的写入中经常遇到：因为文件会有内容持续不断地写入，所以 我们应该给一个标准，到什么时候就开启新的文件，将之前的内容归档保存。也就是说，上面的代码设置了在以下3 种情况下，我们就会滚动分区文件： 

⚫ 至少包含 15 分钟的数据 

⚫ 最近 5 分钟没有收到新的数据 

⚫ 文件大小已达到 1 GB

###### 5.4.3 输出到Kafka

​		Kafka 是一个分布式的基于发布/订阅的消息系统，本身处理的也是流式数据，所以跟 110 Flink“天生一对”，经常会作为 Flink 的输入数据源和输出系统。Flink 官方为 Kafka 提供了 Source 和 Sink 的连接器，我们可以用它方便地从 Kafka 读写数据。如果仅仅是支持读写，那还说明 不了 Kafka 和 Flink 关系的亲密；真正让它们密不可分的是，Flink 与 Kafka 的连接器提供了端 到端的精确一次（exactly once）语义保证，这在实际项目中是最高级别的一致性保证。

​		**在addSink中传入一个FlinkKafkaProducer实例，并传入主机号端口号等参数**

案例实操：

从Kafka读取数据，再输出到Kafka

```
// 1.从Kafka中读取数据
Properties properties = new Properties();
properties.setProperty("bootstrap.servers","hadoop102:9092");

DataStreamSource<String> kafkaStream = env.addSource(new FlinkKafkaConsumer<String>("clicks", new SimpleStringSchema(), properties));

// 2.用Flink进行转换处理
SingleOutputStreamOperator<String> result = kafkaStream.map(new MapFunction<String, String>() {
    @Override
    public String map(String value) throws Exception {
        String[] fields = value.split(",");
        return new Event(fields[0].trim(), fields[1].trim(), Long.valueOf(fields[2].trim())).toString();
    }
});

// 3.结果数据写入Kafka
result.addSink(new FlinkKafkaProducer<String>("hadoop102:9092","events",new SimpleStringSchema()));

env.execute();
```

###### 5.4.4 输出到Redis

​		Redis 是一个开源的内存式的数据存储，提供了像字符串（string）、哈希表（hash）、列表 （list）、集合（set）、排序集合（sorted set）、位图（bitmap）、地理索引和流（stream）等一系 列常用的数据结构。因为它运行速度快、支持的数据类型丰富，在实际项目中已经成为了架构 优化必不可少的一员，一般用作数据库、缓存，也可以作为消息代理。

​		具体步骤如下：

​		（1）导入的Redis连接器依赖

```
<dependency>
 <groupId>org.apache.bahir</groupId>
 <artifactId>flink-connector-redis_2.11</artifactId>
 <version>1.0</version>
</dependency>
```

​		（2）启动Redis集群

​		（3）编写输出到Redis的示例代码

​		连接器为我们提供了一个 **RedisSink**，它继承了抽象类 RichSinkFunction，这就是已经实现 好的向 Redis 写入数据的 SinkFunction。我们可以直接将 Event 数据输出到 Redis：

```
 // 创建一个到 redis 连接的配置
 FlinkJedisPoolConfig conf = new 
FlinkJedisPoolConfig.Builder().setHost("hadoop102").build();
 env.addSource(new ClickSource())
 .addSink(new RedisSink<Event>(conf, new MyRedisMapper()));
 
 env.execute();
```

这里 RedisSink 的构造方法需要传入两个参数： 

⚫ JFlinkJedisConfigBase：Jedis 的连接配置 

⚫ RedisMapper：Redis 映射类接口，说明怎样将数据转换成可以写入 Redis 的类型

定义一个 Redis 的映射类，实现 RedisMapper 接口。

```
public static class MyRedisMapper implements RedisMapper<Event> {
 @Override
 public String getKeyFromData(Event e) {
 return e.user;
 }
 @Override
 public String getValueFromData(Event e) {
 return e.url;
 }
 @Override
 public RedisCommandDescription getCommandDescription() {
 return new RedisCommandDescription(RedisCommand.HSET, "clicks");
 }
}

```

###### 5.4.5 输出到Elasticsearch

​		ElasticSearch 是一个分布式的开源搜索和分析引擎，适用于所有类型的数据。ElasticSearch 有着简洁的 REST 风格的 API，以良好的分布式特性、速度和可扩展性而闻名，在大数据领域 应用非常广泛。 

​		Flink 为 ElasticSearch 专门提供了官方的 Sink 连接器，Flink 1.13 支持当前最新版本的 ElasticSearch。

具体步骤如下：

（1）添加 Elasticsearch 连接器依赖  

```
org.apache.flink  flink-connector-elasticsearch7_${scala.binary.version} ${flink.version} 
```

（2）启动 Elasticsearch 集群 

（3）编写输出到 Elasticsearch 的示例代码

###### 5.4.6 输出到MySQL（JDBC）

​		关系型数据库有着非常好的结构化数据设计、方便的 SQL 查询，是很多企业中业务数据 存储的主要形式。MySQL 就是其中的典型代表。尽管在大数据处理中直接与 MySQL 交互的 场景不多，但最终处理的计算结果是要给外部应用消费使用的，而外部应用读取的数据存储往往就是 MySQL。

​		步骤如下：

（1）添加依赖

```
<dependency>
 <groupId>org.apache.flink</groupId>
 <artifactId>flink-connector-jdbc_${scala.binary.version}</artifactId>
 <version>${flink.version}</version>
</dependency>
<dependency>
 <groupId>mysql</groupId>
 <artifactId>mysql-connector-java</artifactId>
 <version>5.1.47</version>
</dependency>
```

（2）启动MySQL，在数据库下建表clicks

```
mysql> create table clicks(
 -> user varchar(20) not null,
 -> url varchar(100) not null);
```

（3）编写输出到MySQL的实例代码

```
stream.addSink(JdbcSink.sink(
                "insert into clicks (user,url) values (?,?)",
                ((statement, event) -> {
                    statement.setString(1,event.user);
                    statement.setString(2,event.url);
                }),
                new JdbcConnectionOptions.JdbcConnectionOptionsBuilder()
                        .withUrl("jdbc:mysql://localhost:3306/test?useSSL=false")
                        .withDriverName("com.mysql.jdbc.Driver")
                        .withUsername("root")
                        .withPassword("123456")
                        .build()
        ));

        env.execute();
```

###### 5.4.7 自定义Sink输出

​		如果我们想将数据存储到我们自己的存储设备中，而 Flink 并没有提供可以直接使用的连接器，与 Source 类似，Flink 为我们提供了通用的 SinkFunction 接口和对应的 RichSinkDunction 抽象类，只要实现它，通过简单地调用 DataStream 的.addSink()方法就可以自定义写入任何外部存储。

​		之前与外部系统的连接，其实都是连接器帮我们实现了 SinkFunction，现在既然没有现成的，我们就只好自力更生了。例如，Flink 并没有提供 HBase 的连接器，所以需要我们自己写。 **在实现 SinkFunction 的时候，需要重写的一个关键方法 invoke()，在这个方法中我们就可以实现将流里的数据发送出去的逻辑。**

​		使用SinkFunction 的富函数版本，因为有时会使用到生命周期的概念，例如创建 HBase 的连接以及关闭 HBase 的连接就需要分别放在 open()方法和 close()方法中。

### 第 6 章 Flink中的时间和窗口

​		在流数据处理应用中，一个很重要，也很常见的操作就是窗口计算。**所谓的"窗口"，一般就是划定的一段时间范围，也就是"时间窗"，对在这范围内的数据进行处理，就是所谓的窗口计算。**所以窗口和时间往往是分不开的。

##### 6.1 时间语义

###### 6.1.1 Flink中的时间语义

1. 处理时间

​	处理时间的概念非常简单，就是指执行处理操作的机器的系统时间。这种方法非常简单粗暴，不需要各个节点之间进行协调同步，也不需要考虑数据在流中的 位置，简单来说就是“我的地盘听我的”。所以处理时间是最简单的时间语义。

2. 事件时间

​		事件时间，是指每个事件在对应的设备上发生的时间，也就是数据生成的时间。

​		数据一旦产生，这个时间自然就确定了，所以它可以作为一个属性嵌入到数据中。这其实就是这条数据记录的“时间戳”（Timestamp）。

##### 6.2 水位线（Watermark）

###### 6.2.1 什么是水位线

​		在 Flink 中，这种用来衡量事件时间（Event Time）进展的标记，就被称作“水位线”（Watermark）。 

​		具体实现上，水位线可以看作一条特殊的数据记录，它是插入到数据流中的一个标记点， 主要内容就是一个时间戳，用来指示当前的事件时间。而它插入流中的位置，就应该是在某个数据到来之后；这样就可以从这个数据中提取时间戳，作为当前水位线的时间戳了。

![image-20230507202503271](C:\Users\王海涛\AppData\Roaming\Typora\typora-user-images\image-20230507202503271.png)

1. 有序流中的水位线

​		在理想状态下，数据应该按照它们生成的先后顺序、排好队进入流中；也就是说，它们处 理的过程会保持原先的顺序不变，遵守先来后到的原则。这样的话我们从每个数据中提取时间 戳，就可以保证总是从小到大增长的，从而插入的水位线也会不断增长、事件时钟不断向前推进。

​		实际应用中，如果当前数据量非常大，可能会有很多数据的时间戳是相同的，这时每来一条数据就提取时间戳、插入水位线就做了大量的无用功。而且即使时间戳不同，同时涌来的数 据时间差会非常小（比如几毫秒），往往对处理计算也没什么影响。**所以为了提高效率，一般 会每隔一段时间生成一个水位线，这个水位线的时间戳，就是当前最新数据的时间戳**，如图所示。所以这时的水位线，**其实就是有序流中的一个周期性出现的时间标记。**

![image-20230507204126219](C:\Users\王海涛\AppData\Roaming\Typora\typora-user-images\image-20230507204126219.png)

2. **乱序流中的水位线**

​		在分布式系统中，数据在节点间传输，会因为网络传输延迟的不确定性， 导致顺序发生改变，这就是所谓的“乱序数据”。

​		“乱序”（out-of-order），是指数据的先后顺序不一致，比如一个 7 秒时产生的数据，生成时间自然要比 9 秒的数据早；但 是经过数据缓存和传输之后，处理任务可能先收到了 9 秒的数据，之后 7 秒的数据才姗姗来迟。

![image-20230507204305603](C:\Users\王海涛\AppData\Roaming\Typora\typora-user-images\image-20230507204305603.png)

​		因为水位线必须是递增的，没有时光回溯这一说法，所以解决思路也很简单：**我们插入新的水位线时，要先判断一下时间戳是否比之前的大，否则 就不再生成新的水位线，只有数据的时间戳比当前时钟大，才能推动时钟前进，这时才插入水位线**。

![image-20230507204505988](C:\Users\王海涛\AppData\Roaming\Typora\typora-user-images\image-20230507204505988.png)

​		如果考虑到大量数据同时到来的处理效率，我们同样可以**周期性地生成水位线。这时只需要保存一下之前所有数据中的最大时间戳，需要插入水位线时，就直接以它作为时间戳生成新的水位线**，如图 所示。

![image-20230507204535741](C:\Users\王海涛\AppData\Roaming\Typora\typora-user-images\image-20230507204535741.png)

3. 水位线的特性

​		水位线代表了当前的事件时间时钟，而且可以在数据的时间戳基础上加一些延迟来保证不丢数据，这一点对于乱序流的正确处理非常重要

总结一下水位线的特性：

（1）**水位线是插入到数据流中的一个标记，可以认为是一个特殊的数据** 

（2）**水位线主要的内容是一个时间戳，用来表示当前事件时间的进展** 

（3）**水位线是基于数据的时间戳生成的** 

（4）**水位线的时间戳必须单调递增，以确保任务的事件时间时钟一直向前推进** 

（5）**水位线可以通过设置延迟，来保证正确处理乱序数据** 

（6）**一个水位线 Watermark(t)，表示在当前流中事件时间已经达到了时间戳 t, 这代表 t 之 前的所有数据都到齐了，之后流中不会出现时间戳 t’ ≤ t 的数据**

​		水位线是 Flink 流处理中保证结果正确性的核心机制，它往往会跟窗口一起配合，完成对乱序数据的正确处理。

###### 6.2.2 如何生成水位线

1. 生成水位线的总体原则

​		一个字，等。由于网络传输的延迟不确定，为了获取所有迟到数据，我们只能等待更长的时间。那到底等多久呢？这就需要对相关领域有一定的了解了。比如，如果我们知道当前业务中事件的迟到时间不会超过 5 秒， 那就可以将水位线的时间戳设为当前已有数据的最大时间戳减去 5 秒，相当于设置了 5 秒的延 迟等待。		

​		Flink 中的水位线，其实是流处理中对低延迟和结果正确性的一个权衡机制，而且把 控制的权力交给了程序员，我们可以在代码中定义水位线的生成策略。

2. 水位线生成策略（Watermark Strategies）

​		在 Flink 的 DataStream API 中 ， 有 一个单 独 用 于 生 成 水 位 线 的 方 法：**.assignTimestampsAndWatermarks()，它主要用来为流中的数据分配时间戳，并生成水位线来指示事件时间：**	

​		具体使用时，直接用 DataStream 调用该方法即可，与普通的 transform 方法完全一样。

```
DataStreamSource<String> kafkaStream = env.addSource(new FlinkKafkaConsumer<String>("clicks", new SimpleStringSchema(), properties))
                .assignTimestampsAndWatermarks(new WatermarkStrategy<String>() {
                    @Override
                    public WatermarkGenerator<String> createWatermarkGenerator(WatermarkGeneratorSupplier.Context context) {
                        return null;
                    }
                });
```

​		**.assignTimestampsAndWatermarks()方法需要传入一个 WatermarkStrategy 作为参数，这就 是 所 谓 的 “ 水 位 线 生 成 策 略 ” 。 **

​		**WatermarkStrategy 中 包 含 了 一 个 “ 时 间 戳 分 配 器”TimestampAssigner 和一个“水位线生成器”WatermarkGenerator。**

⚫ TimestampAssigner：主要负责从流中数据元素的某个字段中提取时间戳，并分配给元素。时间戳的分配是生成水位线的基础。 

⚫ WatermarkGenerator：主要负责按照既定的方式，基于时间戳生成水位线。在 WatermarkGenerator 接口中，主要又有两个方法：onEvent()和 onPeriodicEmit()。 

⚫ onEvent：每个事件（数据）到来都会调用的方法，它的参数有当前事件、时间戳， 以及允许发出水位线的一个 WatermarkOutput，可以基于事件做各种操作

⚫ onPeriodicEmit：周期性调用的方法，可以由 WatermarkOutput 发出水位线。周期时间 为处理时间，可以调用环境配置的.setAutoWatermarkInterval()方法来设置，默认为 200ms。

3. Flink内置水位线生成器

​		Flink 提供了内置的水位线生成器（WatermarkGenerator），可以通过调用 WatermarkStrategy 的静态辅助方法来创建。它们都是周期性生成水位线的，分别对应着处理有序流和乱序流的场景。

（1）有序流

对于有序流，主要特点就是时间戳单调增长（Monotonously Increasing Timestamps），所以 永远不会出现迟到数据的问题。这是周期性生成水位线的最简单的场景，直接调用 **WatermarkStrategy.forMonotonousTimestamps()**方法就可以实现。

```
.assignTimestampsAndWatermarks(WatermarkStrategy.<Event>forMonotonousTimestamps()
        .withTimestampAssigner(new SerializableTimestampAssigner<Event>() {
            @Override
            public long extractTimestamp(Event element, long recordTimestamp) {
                return element.timestamp;
            }
        }))
```

​		通过调用.withTimestampAssigner()方法，将数据中的 timestamp 字段提取出来， 作为时间戳分配给数据元素；然后用内置的有序流水位线生成器构造出了生成策略。这样，提取出的数据时间戳，就是我们处理计算的事件时间。

（2）乱序流

​		由于乱序流中需要等待迟到数据到齐，所以必须设置一个固定量的延迟时间（Fixed  Amount of Lateness）。调用 **WatermarkStrategy.  forBoundedOutOfOrderness()**方法就可以实现。这个方法需要传入一个 maxOutOfOrderness 参 数，表示“最大乱序程度”，它表示数据流中乱序数据时间戳的最大差值；

```
                  .assignTimestampsAndWatermarks(WatermarkStrategy.forBoundedOutOfOrderness(Duration.ofSeconds(2))
                                .withTimestampAssigner(new SerializableTimestampAssigner<Event>() {
                                    @Override
                                    public long extractTimestamp(Event event, long recordTimestamp) {
                                        return event.timestamp;
                                    }
                                })
                        )
```

​		通过调用.withTimestampAssigner()方法，将数据中的 timestamp 字段提取出来， 作为时间戳分配给数据元素；然后用内置的有序流水位线生成器构造出了生成策略。这样，提取出的数据时间戳，就是我们处理计算的事件时间。

4. 自定义水位线策略

​		有时我们的业务逻辑可能非常复杂，这时对水位线生成的逻辑也有更高的要求，我们就必须自定义实现水位线策略 WatermarkStrategy 了。

​		WatermarkGenerator 接口中的两个方法——onEvent()和 onPeriodicEmit()，前者是在每个事件到来时调用，而后者由框架周期性调用。周期性调用的方法中发出水位线，自然就是周期性生成水位线；而在事件触发的方法中发出水位线，自然就是断点式生成了。两种方式的不同就集中体现在这两个方法的实现上。

（1）周期性水位线生成器（Periodic Generator）

​		**周期性生成器一般是通过 onEvent()观察判断输入的事件，而在 onPeriodicEmit()里发出水位线。**

（2）断点式水位线生成器（Punctuated Generator）

​		**断点式生成器会不停地检测 onEvent()中的事件，当发现带有水位线信息的特殊事件时， 就立即发出水位线。**断点式生成器不会通过 onPeriodicEmit()发出水位线

###### 6.2.3 水位线的传递

​		水位线是数据流中插入的一个标记，用来表示事件时间的进展，它会随着数据一起在任务间传递。如果只是直通式（forward）的传输，那很简单，数据和水位线都是按照本身的顺序依次传递、依次处理的；**一旦水位线到达了算子任务, 那么这个任务就会将它内部的时钟设为这个水位线的时间戳**。

​		还有另外一个问题，那就是在“重分区”（redistributing）的传输模式下，一个任务有可能会收到来自不同分区上游子任务的数据。而不同分区的子任务时钟并不同步，所以同一时刻发给下游任务的水位线可能并不相同。

​		**水位线的本质是表示当前时间戳之前的数据都已经到齐了，不会再有比当前时间更前的数据到来了，所以要以当前时间的数据全部到齐这个标准，那么水位线就是以最晚到来数据的时间戳为准，也就是类似于木桶原理，短板决定了当前的水位线**![image-20230508135753372](C:\Users\王海涛\AppData\Roaming\Typora\typora-user-images\image-20230508135753372.png)

​		如图所示，图1当前上游最小的水位线为2，那么下游任务则以最小水位线为准，也是2。图2 当前上游最小水位线为3，那么下游任务水位线为3，并广播出去；图3上游最小水位线仍是3，则下游任务不更新时钟；图4上游最小水位线是4，则下游任务更新水位线为4。

###### 6.2.4 水位线的总结

​		水位线的默认计算公式：水位线 = 观察到的最大事件时间 – 最大延迟时间 – 1 毫秒。

##### 6.3 窗口

###### 6.3.1 窗口的概念

​		Flink 是一种流式计算引擎，主要是来处理无界数据流的，数据源源不断、无穷无尽。想 要更加方便高效地处理无界流，**一种方式就是将无限数据切割成有限的“数据块”进行处理，这 就是所谓的“窗口”（Window）。**

​		**在 Flink 中, 窗口就是用来处理无界流的核心**。我们很容易把窗口想象成一个固定位置的 “框”，数据源源不断地流过来，到某个时间点窗口该关闭了，就停止收集数据、触发计算并输 出结果。

​		但是当处理乱序的数据时，为了正确处理迟到的数据，有可能会将本不属于该窗口的数据包含进去，导致结果错误，如下图所示：

![image-20230508143657129](C:\Users\王海涛\AppData\Roaming\Typora\typora-user-images\image-20230508143657129.png)

​		由于有乱序数据，我们需要设置一个延迟时间来等所有数据到齐，如该图就设置了延迟时间为2秒，这样迟到的9秒数据就能包含到了，但是0~10 秒的窗口不光包含了迟到的 9 秒数据，连 11 秒和 12 秒的数据也包含进去了。

​		所以在 Flink 中，窗口其实并不是一个“框”，流进来的数据被框住了就只能进这一个窗口。相比之下，我们应该把窗口理解成一个“桶”，如图所示。**在 Flink 中，窗口可以把 流切割成有限大小的多个“存储桶”（bucket)；每个数据都会分发到对应的桶中，当到达窗口 结束时间时，就对每个桶中收集的数据进行计算处理。**

![image-20230508144010975](C:\Users\王海涛\AppData\Roaming\Typora\typora-user-images\image-20230508144010975.png)

###### 6.3.2 窗口的分类

1. 按照驱动类型分类

​		窗口本身是截取有界数据的一种方式，所以窗口一个非常重要的信息其实就是“怎样截取数据”。就是以什么标准来开始和结束数据的截取，我们把它叫作窗口的“驱动类型”。

（1）时间窗口（Time Window）

​		时间窗口以时间点来定义窗口的开始（start）和结束（end），所以截取出的就是某一时间 段的数据。到达结束时间时，窗口不再收集数据，触发计算输出结果，并将窗口关闭销毁。

​		Flink 中有一个专门的类来表示**时间窗口，名称就叫作 TimeWindow。这个类只有两个私有属性：start 和 end，表示窗口的开始和结束的时间戳，单位为毫秒。**

```
private final long start;
private final long end;
```

​		我们可以调用**公有的 getStart()和 getEnd()方法直接获取这两个时间戳**。另外，**TimeWindow 还提供了一个 maxTimestamp()方法，用来获取窗口中能够包含数据的最大时间戳**。

```
public long maxTimestamp() {
 return end - 1;
}
```

窗口中的数据，最大允许的时间戳就是 end - 1

（2）计数窗口（Count Window）

​		计数窗口基于元素的个数来截取数据，到达固定的个数时就触发计算并关闭窗口。相当于“人满就发车”，是否发车与时间无关。每个窗口截取数据的个数，就是窗口的大小。

2. 按照窗口分配数据的规则分类

​		根据分配数据的规则，窗口的具体实现可以分为 4 类：滚动窗口（Tumbling Window）、 滑动窗口（Sliding Window）、会话窗口（Session Window），以及全局窗口（Global Window）。

（1）滚动窗口（Tumbling Windows）

​		滚动窗口有固定的大小，是一种对数据进行“均匀切片”的划分方式。窗口之间没有重叠， 也不会有间隔，是“首尾相接”的状态。所以每个数据都会被分配到一个窗口，而且只会属于一个窗口。

​		**滚动窗口可以基于时间定义，也可以基于数据个数定义；需要的参数只有一个，就是窗口的大小（window size）**

![image-20230508150749677](C:\Users\王海涛\AppData\Roaming\Typora\typora-user-images\image-20230508150749677.png)

（2）滑动窗口（Sliding Windows）

​		与滚动窗口类似，滑动窗口的大小也是固定的。区别在于，窗口之间并不是首尾相接的， 而是可以“错开”一定的位置。如果看作一个窗口的运动，那么就像是向前小步“滑动”一样。

​		定义滑动窗口的参数有两个：**除去窗口大小（window size）之外，还有一个“滑动步长”（window slide）**

​	![image-20230508150930976](C:\Users\王海涛\AppData\Roaming\Typora\typora-user-images\image-20230508150930976.png)

（3）会话窗口（Session Windows）

​		会话窗口顾名思义，是基于“会话”（session）来来对数据进行分组的。数据来了之后就开启一个会话窗口，如果接下来还有数据陆续到来， 那么就一直保持会话；如果一段时间一直没收到数据，那就认为会话超时失效，窗口自动关闭。

​		会话窗口只能基于时间来定义

![image-20230508151109268](C:\Users\王海涛\AppData\Roaming\Typora\typora-user-images\image-20230508151109268.png)

（4）全局窗口（Global Windows）

​		还有一类比较通用的窗口，就是“全局窗口”。这种窗口全局有效，会把相同 key 的所有数据都分配到同一个窗口中；

​		所以 这种窗口也没有结束的时候，默认是不会做触发计算的。如果希望它能对数据进行计算处理， 还需要自定义“触发器”（Trigger）。

![image-20230508151212918](C:\Users\王海涛\AppData\Roaming\Typora\typora-user-images\image-20230508151212918.png)

###### 6.3.3 窗口API概览

1. **按键分区（Keyed）和非按键分区（No-Keyed）**

（1）按键分区窗口（Keyed Windows）

​		经过按键分区 keyBy 操作后，数据流会按照 key 被分为多条逻辑流（logical streams），这 就是 KeyedStream。**基于 KeyedStream 进行窗口操作时, 窗口计算会在多个并行子任务上同时执行。相同 key 的数据会被发送到同一个并行子任务，而窗口操作会基于每个 key 进行单独的处理。**所以可以认为，每个 key 上都定义了一组窗口，各自独立地进行统计计算。

​		在代码实现上，我们需要先对 DataStream 调用.keyBy()进行按键分区，然后再调用.window()定义窗口。

```
 stream.keyBy(...) 
 	   .window(...)
```

（2）非按键分区（No-Keyed Windows）

​		**如果没有进行 keyBy，那么原始的 DataStream 就不会分成多条逻辑流。这时窗口逻辑只能在一个任务（task）上执行，就相当于并行度变成了 1。**所以在实际应用中一般不推荐使用这种方式。 在代码中，直接基于 DataStream 调用.windowAll()定义窗口。

```
stream.windowAll(...)
```

2. **代码中窗口API的调用**

​		对数据流进行窗口化以后，就可以实现窗口操作了，窗口操作有两部分：窗口分配器（Window  Assigners）和窗口函数（WindowFUnctions）

```
stream.keyBy(<key selector>)
 	  .window(<window assigner>)
 	  .aggregate(<window function>)
```

​	.window()方法需要传入一个窗口分配器，它指明了窗口的类型；

​	.aggregate() 方法传入一个窗口函数作为参数，它用来定义窗口具体的处理逻辑。

###### 6.3.4 窗口分配器（Window Assigners）

​		定义窗口分配器（Window Assigners）是构建窗口算子的第一步，它的作用就是定义数据 应该被“分配”到哪个窗口。窗口分配数据的规则，其实就对应着不同的窗口类型。所以可以说，**窗口分配器其实就是在指定窗口的类型。**

​		**窗口分配器最通用的定义方式，就是调用.window()方法。**这个方法需要传入一个 WindowAssigner 作为参数，返回 WindowedStream。如果是非按键分区窗口，那么直接调 用.windowAll()方法，同样传入一个 WindowAssigner，返回的是 AllWindowedStream。

1. 计数窗口

​		**直接调用.countWindow()方法**。根据分配规则的不同，又可以分为 滚动计数窗口和滑动计数窗口两类

（1）滚动计数窗口

滚动计数窗口只需要传入一个长整型的参数 size，表示窗口的大小。

```
stream.keyBy(...)
		.countWindow(10)
		//定义了一个长度为 10 的滚动计数窗口，当窗口中元素数量达到 10 的时候，就会触发计算执行并关闭窗口。
```

（2）滑动计数窗口

​		需要在.countWindow()调用时传入两个参数：size 和 slide，前 者表示窗口大小，后者表示滑动步长。

```
stream.keyBy(...)
.countWindow(10，3)
//定义了一个长度为 10、滑动步长为 3 的滑动计数窗口。每个窗口统计 10 个数据，每隔 3 个数据就统计输出一次结果。
```

2. 时间窗口

​		时间窗口是最常用的窗口类型，又可以细分为滚动、滑动和会话三种。

（1）滚动窗口

​		窗口分配器由类 TumblingProcessingTimeWindows 提供，需要调用它的静态方法.of()。

```
.window(TumblingEventTimeWindows.of(Time.hours(1))); //滚动事件时间窗口
```

（2）滑动窗口

​		窗口分配器由类 SlidingProcessingTimeWindows 提供，同样需要调用它的静态方法.of()

```
stream.keyBy(...)		.window(SlidingProcessingTimeWindows.of(Time.seconds(10), Time.seconds(5)))
.aggregate(...)
```

（3）会话窗口

​		窗口分配器由类 EventTimeSessionWindows 提供，需要调用它的静态方法.withGap() 或者.withDynamicGap()。

```
.window(EventTimeSessionWindows.withGap(Time.seconds(2))) //会话事件窗口
```

###### 6.3.5 窗口函数（Window Function）

​		定义了窗口分配器，我们只是知道了数据属于哪个窗口，可以将数据收集起来了；但是还没有给数据进行操作，所以在窗口分配器之后，必须再接上一个定义窗口如何进行计算的操作，这就是所谓的“窗口函数”（window functions）。

​		经窗口分配器处理之后，数据可以分配到对应的窗口中，而数据流经过转换得到的数据类型是 WindowedStream。这个类型并不是 DataStream，所以并不能直接进行其他转换，而**必须进一步调用窗口函数，对收集到的数据进行处理计算之后，才能最终再次得到 DataStream**

![image-20230508163034307](C:\Users\王海涛\AppData\Roaming\Typora\typora-user-images\image-20230508163034307.png)

​		窗口函数定义了要对窗口中收集的数据做的计算操作，根据处理的方式可以分为两类：**增量聚合函数和全窗口函数**。

1. 增量聚合函数

​		为了提高实时性，要贯彻流处理的思想：**就像 DataStream 的简单聚合一样，每来一条数据就立即进行计算，中间只要保持一个简单的聚合状态就可以了；区别只是在于不立即输出结果，而是要等到窗口结束时间。**等到窗口到了结束时间需要输出计算结果的时候，只需要拿出之前聚合的状态直接输出，大大提高了程序运行的效率和实时性。 

​		典型的增量聚合函数有两个：**ReduceFunction 和 AggregateFunction。**

（1）归约函数（ReduceFunction）

​		最基本的聚合方式就是归约（reduce）。，窗口的归约聚合和作基本转换的聚合算子非常类似，就是将窗口中收集到的数据两两进行归约。当进行流处理时，就是要保存一个状态；每来一个新的数据，就和之前的聚合状态做归约，这样就实现了增量式的聚合。

​		 窗口函数中也提供了 ReduceFunction：**只要基于 WindowedStream 调用.reduce()方法，然 后传入 ReduceFunction 作为参数，就可以指定以归约两个元素的方式去对窗口中数据进行聚 合了。**这里的 ReduceFunction 其实与简单聚合时用到的 ReduceFunction 是同一个函数类接口， 所以使用方式也是完全一样的。 

​		ReduceFunction 中需要重写一个 reduce 方法，它的两个参数代表输入的两个元素，而归约最终输出结果的数据类型，与输入的数据类型必须保持一致。也就是说，中间 聚合的状态和输出的结果，都和输入的数据类型是一样的。

```
SingleOutputStreamOperator<Event> stream = env.addSource(new ClickSource())
        .assignTimestampsAndWatermarks(WatermarkStrategy.<Event>forBoundedOutOfOrderness(Duration.ZERO)
                .withTimestampAssigner(new SerializableTimestampAssigner<Event>() {
                    @Override
                    public long extractTimestamp(Event element, long recordTimestamp) {
                        return element.timestamp;
                    }
                }));
stream.map(new MapFunction<Event, Tuple2<String,Long>>() {
    @Override
    public Tuple2<String, Long> map(Event event) throws Exception {
        return Tuple2.of(event.user, 1L);
    }
})
    .keyBy(data -> data.f0)
        //  .countWindow(10,2) //滑动计数窗口
        //  .window(EventTimeSessionWindows.withGap(Time.seconds(2))) //会话事件窗口
        //  .window(SlidingEventTimeWindows.of(Time.hours(1),Time.minutes(5))) //滑动事件窗口
        .window(TumblingEventTimeWindows.of(Time.seconds(10))) //滚动事件时间窗口
        .reduce(new ReduceFunction<Tuple2<String, Long>>() {
            @Override
            public Tuple2<String, Long> reduce(Tuple2<String, Long> value1, Tuple2<String, Long> value2) throws Exception {
                return Tuple2.of(value1.f0, value1.f1+value2.f1);
            }
        })
                .print();
```

（2）聚合函数（AggregateFunction）

​		AggregateFunction 可以看作是 ReduceFunction 的通用版本，这里有三种类型：**输入类型 （IN）、累加器类型（ACC）和输出类型（OUT）**。输入类型 IN 就是输入流中元素的数据类型； 累加器类型 ACC 则是我们进行聚合的中间状态类型；而输出类型当然就是最终计算结果的类型

接口中有四个方法：

① createAccumulator()：创建一个累加器，这就是为聚合创建了一个初始状态，每个聚合任务只会调用一次。 

② add()：将输入的元素添加到累加器中。这就是基于聚合状态，对新来的数据进行进一步聚合的过程。**方法传入两个参数：当前新到的数据 value，和当前的累加器 accumulator；返回一个新的累加器值，也就是对聚合状态进行更新。每条数据到来之后都会调用这个方法。** 

③ getResult()：**从累加器中提取聚合的输出结果**。也就是说，我们可以定义多个状态， 然后再基于这些聚合的状态计算出一个结果进行输出。比如之前我们提到的计算平均值，就可以把 sum 和 count 作为状态放入累加器，而在调用这个方法时相除得到最终 结果。这个方法只在窗口要输出结果时调用。 

④ merge()：**合并两个累加器，并将合并后的状态作为一个累加器返回。这个方法只在需要合并窗口的场景下才会被调用**；最常见的合并窗口（Merging Window）的场景就是会话窗口（Session Windows）。

​		AggregateFunction 的工作原理是：**首先调用 createAccumulator()为任务初始化一个状态(累加器)；而后每来一个数据就调用一次 add()方法，对数据进行聚合，得到的 结果保存在状态中；等到了窗口需要输出时，再调用 getResult()方法得到计算结果。**很明显， 与 ReduceFunction 相同，AggregateFunction 也是增量式的聚合；而由于输入、中间状态、输出的类型可以不同，使得应用更加灵活方便。

​		一个具体例子：在电商网站中，PV（页面浏览量）和 UV（独立访客 数）是非常重要的两个流量指标。一般来说，PV 统计的是所有的点击量；而对用户 id 进行去重之后，得到的就是UV。所以有时我们会用 PV/UV 这个比值，来表示“人均重复访问量”， 也就是平均每个用户会访问多少次页面，这在一定程度上代表了用户的粘度

代码如下：

```
SingleOutputStreamOperator<Event> stream = env.addSource(new ClickSource())
            	.assignTimestampsAndWatermarks(WatermarkStrategy.<Event>forBoundedOutOfOrderness(Duration.ZERO)
                    .withTimestampAssigner(new SerializableTimestampAssigner<Event>() {
                        @Override
                        public long extractTimestamp(Event element, long recordTimestamp) {
                            return element.timestamp;
                        }
                    })
            );
    stream.print("data");
    // 所有数据放在一起统计pv和uv
    stream.keyBy(data -> true)
                    .window(TumblingEventTimeWindows.of(Time.seconds(10),Time.seconds(2)))
                            .aggregate(new AvgPv())
                                    .print();


    env.execute();
}

//自定义一个AggregateFunction,用Long保存pv个数，用HashSet做uv去重
public static class AvgPv implements AggregateFunction<Event, Tuple2<Long, HashSet<String>>,Double>{
    @Override
    public Tuple2<Long, HashSet<String>> createAccumulator() {
        return Tuple2.of(0L,new HashSet<>());
    }

    @Override
    public Tuple2<Long, HashSet<String>> add(Event value, Tuple2<Long, HashSet<String>> accumulator) {
        //每来一条数据，pv个数加一，将user放入HashSet
        accumulator.f1.add(value.user);
        return Tuple2.of(accumulator.f0 + 1, accumulator.f1);
}

    @Override
    public Double getResult(Tuple2<Long, HashSet<String>> accumulator) {
        //窗口触发时，输出pv和uv的比值
        return (double)accumulator.f0 / accumulator.f1.size();
}

    @Override
    public Tuple2<Long, HashSet<String>> merge(Tuple2<Long, HashSet<String>> longHashSetTuple2, Tuple2<Long, HashSet<String>> acc1) {
        return null;
}
```

2. 全窗口函数（full window functions）

​		窗口操作中的另一大类就是全窗口函数。与增量聚合函数不同，全窗口函数需要先收集窗口中的数据，并在内部缓存起来，等到窗口要输出结果的时候再取出数据进行计算。

（1）窗口函数（WindowFuncation）

​		可以基于 WindowedStream 调用.apply()方法，传入一个 WindowFunction 的实现类，这个类中可以获取到包含窗口所有数据的可迭代集合（Iterable），还可以拿到窗口 （Window）本身的信息。

```
stream
 .keyBy(<key selector>)
 .window(<window assigner>)
 .apply(new MyWindowFunction());

```

​		WindowFunction 能提供的上下文信息较少，也没有更高级的功能。 事实上，它的作用可以被 ProcessWindowFunction 全覆盖，**所以之后可能会逐渐弃用。一般在 实际应用，直接使用 ProcessWindowFunction 就可以了。**

（2）处理窗口函数（ProcessWindowFunction）

​		ProcessWindowFunction 是 Window API 中最底层的通用窗口函数接口,除了可以拿到窗口中的所有数据之外，								    	    ProcessWindowFunction 还可以获取到一个  “上下文对象”（Context）。这个上下文对象非常强大，不仅能够获取窗口信息，还可以访问当 前的时间和状态信息。这里的时间就包括了处理时间（processing time）和事件时间水位线（event  time watermark）。这就使得 ProcessWindowFunction 更加灵活、功能更加丰富。

​		这 些 好 处 是 以 牺 牲 性 能 和 资 源 为 代 价 的 。 作 为 一 个 全 窗 口 函 数 ， ProcessWindowFunction 同样需要将所有数据缓存下来、等到窗口触发计算时才使用。它其实 就是一个增强版的 WindowFunction。

```
//使用ProcessWindowFunction计算UV
    stream.keyBy(data -> true)
                    .window(TumblingEventTimeWindows.of(Time.seconds(10)))
                            .process(new UvCountByWindow())
                                    .print();
    env.execute();
}

//实现自定义的ProcessWindowFunction，输出一条统计信息
public static class UvCountByWindow extends ProcessWindowFunction<Event,String,Boolean, TimeWindow>{
    @Override
    public void process(Boolean aBoolean, ProcessWindowFunction<Event, String, Boolean, TimeWindow>.Context context, Iterable<Event> elements, Collector<String> out) throws Exception {
        // 用一个HashSet保存user
        HashSet<String> userSet = new HashSet<>();
        // 从elements中遍历数据，放到set中去重
        for (Event event : elements) {
            userSet.add(event.user);
        }
        Integer uv = userSet.size();
        //结合窗口信息
        Long start = context.window().getStart();
        Long end = context.window().getEnd();
        out.collect("窗口 " + new Timestamp(start) + "~" + new Timestamp(end) + "UV值为 " + uv);
    }
}
```

3. 增量聚合和全窗口函数的结合使用	

​		全窗口函数只是把它们收集缓存起来，并没有处理；到了窗口要关闭、输出结果 的时候，再遍历所有数据依次叠加，得到最终结果。		增量聚合函数处理计算会更高效。

​		而全一起使用窗口函数的优势在于提供了更多的信息，可以认为是更加“通用”的窗口操作。它只负责收集数据、提供上下文相关信息

​		所以在实际应用中，往往将这两者结合起来

​		之前在调用 WindowedStream 的.reduce()和.aggregate()方法时，只是简单地直接传入 了一个 ReduceFunction 或 AggregateFunction 进行增量聚合。除此之外，其实还可以传入第二 个参数：一个全窗口函数，可以是 WindowFunction 或者 ProcessWindowFunction。

实际案例：

```
//使用AggregateFunction和ProcessWindowFunction计算UV
    stream.keyBy(data -> true)
            .window(TumblingEventTimeWindows.of(Time.seconds(10)))
            .aggregate(new UvAgg(),new UvCountResult())
            .print();
    env.execute();
}

//自定义AggregateFunction，增量聚合UV值
public static class UvAgg implements AggregateFunction<Event, HashSet<String>,Long>{
    @Override
    public HashSet<String> createAccumulator() {
        return new HashSet<>();
    }

    @Override
    public HashSet<String> add(Event event, HashSet<String> accumulator) {
        accumulator.add(event.user);
        return accumulator;
    }

    @Override
    public Long getResult(HashSet<String> accumulator) {
        return (long) accumulator.size();
    }

    @Override
    public HashSet<String> merge(HashSet<String> strings, HashSet<String> acc1) {
        return null;
    }
}

//自定义实现ProcessWindowFunction，包装窗口信息输出
public static class UvCountResult extends ProcessWindowFunction<Long,String,Boolean, TimeWindow>{
    @Override
    public void process(Boolean aBoolean, ProcessWindowFunction<Long, String, Boolean, TimeWindow>.Context context, Iterable<Long> elements, Collector<String> out) throws Exception {
        //结合窗口信息
        Long start = context.window().getStart();
        Long end = context.window().getEnd();
        Long uv =elements.iterator().next();
        out.collect("窗口: " + new Timestamp(start) + "~" + new Timestamp(end) + "UV值为 " + uv);

    }
}
```

###### 6.3.5 其他API

​	对于一个窗口算子而言，窗口分配器和窗口函数是必不可少的。此外，Flink还提供了一些其他可选的API，**并且这些API只有写在窗口分配器和窗口函数的中间才有效**

1. **触发器（Trigger）**

​		触发器主要是用来控制窗口什么时候触发计算。所谓的触发计算，本质上就是执行窗口函数，所以可以认为是计算得到结果并输出的过程。

​		基于WindowedStream调用.trigger()方法，就可以传入一个自定义的窗口触发器（Trigger）

```
stream.keyBy(...)
      .window(...)
 	  .trigger(new MyTrigger())
```

​	Trigger 是一个抽象类，自定义时必须实现下面四个抽象方法：

⚫ onElement()：窗口中每到来一个元素，都会调用这个方法。 

⚫ onEventTime()：当注册的事件时间定时器触发时，将调用这个方法。 

⚫ onProcessingTime ()：当注册的处理时间定时器触发时，将调用这个方法。 

⚫ clear()：当窗口关闭销毁时，调用这个方法。一般用来清除自定义的状态。

前三个方法的返回值都是TriggerResult，这是一个枚举类型（enum），定义了对窗口操作的四种类型

⚫ CONTINUE（继续）：什么都不做 

⚫ FIRE（触发）：触发计算，输出结果 

⚫ PURGE（清除）：清空窗口中的所有数据，销毁窗口 

⚫ FIRE_AND_PURGE（触发并清除）：触发计算输出结果，并清除窗口

2. 移除器

移除器主要用来定义移除某些数据的逻辑，基于 WindowedStream 调用.evictor()方法，就可以传入一个自定义的移除器（Evictor）。Evictor 是一个接口，不同的窗口类型都有各自预实现的移除器。

```
stream.keyBy(...)
 .window(...)
 .evictor(new MyEvictor())
```

Evictor 接口定义了两个方法： 

⚫ evictBefore()：定义执行窗口函数之前的移除数据操作 

⚫ evictAfter()：定义执行窗口函数之后的以处数据操作 默认情况下，预实现的移除器都是在执行窗口函数（window fucntions）之前移除数据的。

3. 允许延迟

​		在事件时间语义下，窗口中可能会出现数据迟到的情况。为了解决迟到数据的问题，Flink 提供了一个特殊的接口，可以为窗口算子设置一个 “允许的最大延迟”（Allowed Lateness）。

​		基于 WindowedStream 调用.allowedLateness()方法，传入一个 Time 类型的延迟时间，就可 以表示允许这段时间内的延迟数据。

```
stream.keyBy(...)
 .window(TumblingEventTimeWindows.of(Time.hours(1)))
 .allowedLateness(Time.minutes(1))
 .aggregate(...)
```

4. 将迟到的数据放入侧输出流

​		Flink 还提供了另外一种方式处理迟到数据。我们可以将未收入窗口的迟到数据，放入“侧输出流”（side output）进行另外的处理。所谓的侧输出流，相当于是数据流的一个“分支”， 这个流中单独放置那些错过了该上的车、本该被丢弃的数据。

​		**基于 WindowedStream 调用.sideOutputLateData() 方法，就可以实现这个功能。**

​		将迟到数据放入侧输出流之后，还应该可以将它提取出来。基于窗口处理完成之后的 DataStream，调用.getSideOutput()方法，传入对应的输出标签，就可以获取到迟到数据所在的流了。

案例：处理迟到的数据

```
 StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.getConfig().setAutoWatermarkInterval(100);

//        SingleOutputStreamOperator<Event> stream = env.addSource(new ClickSource())
          SingleOutputStreamOperator<Event> stream = env.socketTextStream("hadoop102",7777)
                  .map(new MapFunction<String, Event>() {
                      @Override
                      public Event map(String s) throws Exception {
                          String[] fields = s.split(" ");
                          return new Event(fields[0].trim(),fields[1].trim(),Long.valueOf(fields[2].trim()));
                      }
                  })
                  .assignTimestampsAndWatermarks(WatermarkStrategy.<Event>forBoundedOutOfOrderness(Duration.ofSeconds(2))
                        .withTimestampAssigner(new SerializableTimestampAssigner<Event>() {
                            @Override
                            public long extractTimestamp(Event element, long recordTimestamp) {
                                return element.timestamp;
                            }
                        })
                );
        stream.print("input");

        //定义一个输出标签
        OutputTag<Event> late = new OutputTag<Event>("lates"){};

        // 统计每个URL的访问量
        SingleOutputStreamOperator<UrlViewCount> result = stream.keyBy(data -> data.url)
                .window(TumblingEventTimeWindows.of(Time.seconds(10)))
                .allowedLateness(Time.minutes(1)) //将窗口允许延迟1分钟
                .sideOutputLateData(late)  //设置测输出流，传入一个输出标签，将迟到的数据放入侧输出流中
                .aggregate(new UrlCountViewExample.UrlViewCountAgg(), new UrlCountViewExample.UrlViewCountResult());

        result.print("result");

        result.getSideOutput(late).print("late");//获取侧输出流

        env.execute();
```

### 第 7 章 处理函数

![image-20230510162804173](C:\Users\王海涛\AppData\Roaming\Typora\typora-user-images\image-20230510162804173.png)

​		在更底层，我们可以不定义任何具体的算子（比如 map，filter，或者 window），而只是提 炼出一个统一的“处理”（process）操作——它是所有转换算子的一个概括性的表达，可以自 定义处理逻辑，所以这一层接口就被叫作“处理函数”（process function）。

##### 7.1 基本处理函数（ProcessFunction）

​		处理函数主要是定义数据流的转换操作，所以也可以把它归到转换算子中。我们知道在 Flink 中几乎所有转换算子都提供了对应的函数类接口，处理函数也不例外；它所对应的函数类，就叫作 ProcessFunction。

###### 7.1.1 处理函数的功能和使用

​		前面提到的转换算子，都是针对某种具体操作进行定义的，能拿到的信息有限，而而像窗口聚合这样的复杂操作，AggregateFunction 中除数据外，还可以获取到当前的状态。另外还有富函数类，可以调用生命周期的函数，比如 RichMapFunction， 它提供了获取运行时上下文的方法 getRuntimeContext()，可以拿到状态，还有并行度、任务名称之类的运行时信息。

​		但以上算子都无法访问到一些信息比如：事件的时间戳、当前水位线信息。

​		**处理函数可以做到。处理函数提供了一个“定时服务” （TimerService），我们可以通过它访问流中的事件（event）、时间戳（timestamp）、水位线 （watermark），甚至可以注册“定时事件”。而且处理函数继承了 AbstractRichFunction 抽象类， 所以拥有富函数类的所有特性，**同样可以访问状态（state）和其他运行时信息。此外，处理函数还可以直接将数据输出到侧输出流（side output）中。所以，**处理函数是最为灵活的处理方法，可以实现各种自定义的业务逻辑；同时也是整个 DataStream API 的底层基础。**

​			处理函数的使用与基本的转换操作类似，只需要直接基于 DataStream **调用.process()方法** 就可以了。**方法需要传入一个 ProcessFunction 作为参数，用来定义处理逻辑。**

```
stream.process(new MyProcessFunction())
```

###### 7.1.2 ProcessFunction 解析

​	ProcessFunction 是一个抽象类，继承了 AbstractRichFunction；

​	有两个泛型类型参数：I 表示 Input，也就是输入的数据类型；O 表示 Output，也就是处理完成之后输出 的数据类型。

​	内部单独定义了两个方法：**一个是必须要实现的抽象方法.processElement()；另一个是非 抽象方法.onTimer()。**

```
public abstract class ProcessFunction<I, O> extends AbstractRichFunction {
 ...
	public abstract void processElement(I value, Context ctx, Collector<O> out) 
throws Exception;

	public void onTimer(long timestamp, OnTimerContext ctx, Collector<O> out) 
throws Exception {}
...
}

```

1. 抽象方法processElement（）

​	用于“处理元素”，定义了处理的核心逻辑。这个方法对于流中的每个元素都会调用一次， 参数包括三个：输入数据值 value，上下文 ctx，以及“收集器”（Collector）out。方法没有返回值，处理之后的输出数据是通过收集器 out 来定义的。

传入的参数：

​	1）value：当前流中的输入元素，也就是正在处理的数据，类型与流中数据类 型一致。

​	2）ctx：类型是 ProcessFunction 中定义的内部抽象类 Context，表示当前运行的上下文，可以获取到当前的时间戳，并提供了用于查询时间和注册定时器的“定时服务”(TimerService)，以及可以将数据发送到“侧输出流”（side output）的方法.output()。

​	3）out：“收集器”（类型为 Collector），用于返回输出数据。使用方式与 flatMap 算子中的收集器完全一样，直接调用 out.collect()方法就可以向下游发出一个数据。

2. 非抽象方法 .onTimer()

​		用于定义定时触发的操作，这个方法只有在注册好的定时器触发的时候才会调用，而定时器是通过“定时服务”TimerService 来注册的。

​		注册定时器（timer）就是设了一个闹钟，到了设定时间就会响；而 .onTimer()中定义的， 就是闹钟响的时候要做的事。

```
StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
env.setParallelism(1);

SingleOutputStreamOperator<Event> stream = env.addSource(new ClickSource())
        .assignTimestampsAndWatermarks(WatermarkStrategy.<Event>forBoundedOutOfOrderness(Duration.ZERO)
                .withTimestampAssigner(new SerializableTimestampAssigner<Event>() {
                    @Override
                    public long extractTimestamp(Event element, long l) {
                        return element.timestamp;
                    }
                })
        );

stream.process(new ProcessFunction<Event, String>() {
    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
    }

    @Override
    public void close() throws Exception {
        super.close();
    }

    @Override
    public void processElement(Event value, ProcessFunction<Event, String>.Context ctx, Collector<String> out) throws Exception {
        if (value.user.equals("Mary")){
            out.collect(value.user + "clicks" + value.url);
        } else if (value.user.equals("Bob")){
            out.collect(value.user);
            out.collect(value.user);
        }

        out.collect(value.toString());
        System.out.println("timestamp: " + ctx.timestamp());
        System.out.println("watermark: " + ctx.timerService().currentWatermark());

        System.out.println(getRuntimeContext().getIndexOfThisSubtask());


    }
}).print();

env.execute();
```

###### 7.1.3 处理函数的分类

​		Flink 中的处理函数其实是一个大家族，ProcessFunction 只是其中一员。

​		Flink 提供了 8 个不同的处理函数：

（1）ProcessFunction 

​		最基本的处理函数，基于 DataStream 直接调用.process()时作为参数传入。 

（2）KeyedProcessFunction 

​		**对流按键分区后的处理函数，基于 KeyedStream 调用.process()时作为参数传入。**要想使用定时器，比如基于 KeyedStream。 

（3）ProcessWindowFunction 

​		**开窗之后的处理函数，也是全窗口函数的代表。基于 WindowedStream 调用.process()时作为参数传入**。 

（4）ProcessAllWindowFunction 

​		**同样是开窗之后的处理函数，基于 AllWindowedStream 调用.process()时作为参数传入。** 

（5）CoProcessFunction

​		合并（connect）两条流之后的处理函数，基于 ConnectedStreams 调用.process()时作为参数传入。

（6）ProcessJoinFunction 

​		间隔连接（interval join）两条流之后的处理函数，基于 IntervalJoined 调用.process()时作为 参数传入。 

（7）BroadcastProcessFunction 

​		广播连接流处理函数，基于 BroadcastConnectedStream 调用.process()时作为参数传入。这里的“广播连接流”BroadcastConnectedStream，是一个未 keyBy 的普通 DataStream 与一个广播流（BroadcastStream）做连接（conncet）之后的产物。

（8）KeyedBroadcastProcessFunction 

​		按键分区的广播连接流处理函数，同样是基于 BroadcastConnectedStream 调用.process()时作为参数传入。与 BroadcastProcessFunction 不同的是，这时的广播连接流，是一个 KeyedStream 与广播流（BroadcastStream）做连接之后的产物。

##### 7.2 按键分区处理函数（KeyedProcessFunction）

​		为了实现数据的聚合统计，或者开窗计算之类的功能，我们一般都要先用 keyBy 算子对数据流进行“按键分区”，得到一个 KeyedStream。

​		**只有在 KeyedStream 中才支持使用 TimerService 设置定时器的操作。所以一般情况下，都是先做了 keyBy 分区之后，再去定义处理操作；**

###### 7.2.1 定时器（Timer）和定时服务（TimeService）

KeyedProcessFunction 的一个特色，就是可以灵活地使用定时器。

定时器（timers）是处理函数中进行时间相关操作的主要机制。		   		**在.onTimer()方法中可以实现定时处理的逻辑，而它能触发的前提，就是之前曾经注册过定时器、并且现在已经到了触发时间。**注册定时器的功能，是通过上下文中提供的“定时服务”（TimerService）来实现的。

​		定时服务与当前运行的环境有关。ProcessFunction 的上下文（Context） 中提供了.timerService()方法，可以直接返回一个 TimerService 对象

​		TimerService 是 Flink 关于时间和定时器的基础服务接口，包含以下六个方法：

```
// 获取当前的处理时间
long currentProcessingTime();
// 获取当前的水位线（事件时间）
long currentWatermark();
// 注册处理时间定时器，当处理时间超过 time 时触发
void registerProcessingTimeTimer(long time);
// 注册事件时间定时器，当水位线超过 time 时触发
void registerEventTimeTimer(long time);
// 删除触发时间为 time 的处理时间定时器
void deleteProcessingTimeTimer(long time);
// 删除触发时间为 time 的处理时间定时器
void deleteEventTimeTimer(long time);
```

案例实操：

基于时间的定时器

```
 StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        SingleOutputStreamOperator<Event> stream = env.addSource(new ClickSource());

        stream.keyBy(data -> data.user)
                        .process(new KeyedProcessFunction<String, Event, String>() {
                            @Override
                            public void processElement(Event value, KeyedProcessFunction<String, Event, String>.Context ctx, Collector<String> out) throws Exception {
                                //获取当前时间戳
                                Long currTs = ctx.timerService().currentProcessingTime();
                                //收集数据到控制台
                                out.collect(ctx.getCurrentKey()+" 数据到达，时间： " + new Timestamp(currTs));

                                //注册一个10s的定时器
                                ctx.timerService().registerProcessingTimeTimer(currTs+ 10000L);
                            }
                            @Override
                            public void onTimer(long timestamp, KeyedProcessFunction<String, Event, String>.OnTimerContext ctx, Collector<String> out) throws Exception {
                                out.collect(ctx.getCurrentKey() + "定时器触发，触发时间： " + new Timestamp(timestamp));
                            }
                        }).print();

env.execute();
```

基于事件的定时器：

```
    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
    env.setParallelism(1);

    SingleOutputStreamOperator<Event> stream = env.addSource(new CustomSource())
            .assignTimestampsAndWatermarks(WatermarkStrategy.<Event>forBoundedOutOfOrderness(Duration.ZERO)
                    .withTimestampAssigner(new SerializableTimestampAssigner<Event>() {
                        @Override
                        public long extractTimestamp(Event element, long l) {
                            return element.timestamp;
                        }
                    })
            );
    //事件时间定时器
    stream.keyBy(data -> data.user)
            .process(new KeyedProcessFunction<String, Event, String>() {
                @Override
                public void processElement(Event value, KeyedProcessFunction<String, Event, String>.Context ctx, Collector<String> out) throws Exception {
                    //获取当前时间戳
                    Long currTs = ctx.timestamp();
                    //收集数据到控制台
                    out.collect(ctx.getCurrentKey()+" 数据到达，到达时间： " + new Timestamp(currTs)+ "watermark: " + ctx.timerService().currentWatermark());
                    //注册一个10s的定时器
                    ctx.timerService().registerProcessingTimeTimer(currTs+ 10000L);
                }
                @Override
                public void onTimer(long timestamp, KeyedProcessFunction<String, Event, String>.OnTimerContext ctx, Collector<String> out) throws Exception {
                    out.collect(ctx.getCurrentKey() + "定时器触发，触发时间： " + new Timestamp(timestamp));
                }
            }).print();
    env.execute();
}

//自定义测试数据源
public static class CustomSource implements SourceFunction<Event>{
    @Override
    public void run(SourceContext<Event> ctx) throws Exception {
        // 直接发出测试数据
        ctx.collect(new Event("Mary","./home",1000L));

        Thread.sleep(5000L);

        ctx.collect(new Event("Alice","./home",11000L));
        Thread.sleep(5000L);

        ctx.collect(new Event("Bob","./home",11001L));
        Thread.sleep(5000L);


    }

    @Override
    public void cancel() {

    }
}
```

###### 7.2.2 KeyedProcessFunction的使用

​		我们只要基于 keyBy 之后的 KeyedStream，直接调用.process()方法，这时需要传入 的参数就是 KeyedProcessFunction 的实现类。

```
stream.keyBy( t -> t.f0 )
		.process(new MyKeyedProcessFunction())
```

##### 7.3 窗口处理函数

​			除 了 KeyedProcessFunction ， 另 外 一 大 类 常 用 的 处 理 函 数 ， 就 是 基 于 窗 口 的 ProcessWindowFunction 和 ProcessAllWindowFunction 

###### 7.3.1 窗口处理函数的使用

​		窗 口 处 理 函 数 ProcessWindowFunction 的 使 用 与 其 他 窗 口 函 数 类 似 ， 也 是 基 于 WindowedStream 直接调用方法就可以，只不过这时调用的是.process()。

```
stream.keyBy( t -> t.f0 )
 .window(TumblingEventTimeWindows.of(Time.seconds(10)) )
 .process(new MyProcessWindowFunction())
```

##### 7.4 应用案例—Top N

​		窗口的计算处理，在实际应用中非常常见。对于一些比较复杂的需求，如果增量聚合函数 无法满足，我们就需要考虑使用窗口处理函数

​		网站中一个非常经典的例子，就是实时统计一段时间内的热门 url。例如，需要统计最近 10 秒钟内最热门的两个 url 链接，并且每 5 秒钟更新一次。这可以用一个滑动窗口来实现，而“热门度”一般可以直接用访问量来表示。于是就需要开滑动窗口收集 url 的访问数据，按照不同的 url 进行统计，而后汇总排序并最终输出前两名。这其实就是著名的“Top N” 问题。

###### 7.4.1 使用ProcessAllWindowFunction

​		一种最简单的想法是，干脆不区分 url 链接，而是将所有访问数据都收集起来，统一 进行统计计算。所以可以不做 keyBy，直接基于 DataStream 开窗，然后使用全窗口函数 ProcessAllWindowFunction 来进行处理。 

​		在窗口中可以用一个 HashMap 来保存每个 url 的访问次数，只要遍历窗口中的所有数据， 自然就能得到所有 url 的热门度。最后把 HashMap 转成一个列表 ArrayList，然后进行排序、 取出前两名输出就可以了。

```
StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
    env.setParallelism(1);

    // 读取数据
    SingleOutputStreamOperator<Event> stream = env.addSource(new ClickSource())
            .assignTimestampsAndWatermarks(WatermarkStrategy.<Event>forBoundedOutOfOrderness(Duration.ZERO)
                    .withTimestampAssigner(new SerializableTimestampAssigner<Event>() {
                        @Override
                        public long extractTimestamp(Event event, long l) {
                            return event.timestamp;
                        }
                    })
            );
    //直接开窗收集所有数据排序
    stream.map(data -> data.user)
                    .windowAll(SlidingEventTimeWindows.of(Time.seconds(10),Time.seconds(5)))
                            .aggregate(new UrlHashMapCountAgg(),new UrlAllWindowResult())
                                    .print();

    env.execute();


}
//实现自定义的增量聚合函数
public static class UrlHashMapCountAgg implements AggregateFunction<String, HashMap<String,Long>, ArrayList<Tuple2<String,Long>>>{
    @Override
    public HashMap<String, Long> createAccumulator() {
        return new HashMap<>();
    }

    @Override
    public HashMap<String, Long> add(String value, HashMap<String, Long> accumulator) {
        if (accumulator.containsKey(value)){
            Long count = accumulator.get(value);
            accumulator.put(value, count + 1);
        } else {
            accumulator.put(value,1L);
        }
        return accumulator;
    }

    @Override
    public ArrayList<Tuple2<String, Long>> getResult(HashMap<String, Long> accumulator) {
        ArrayList<Tuple2<String,Long>> result = new ArrayList<>();
        for (String key : accumulator.keySet()){
            result.add(Tuple2.of(key, accumulator.get(key)));
        }
        result.sort(new Comparator<Tuple2<String, Long>>() {
            @Override
            public int compare(Tuple2<String, Long> o1, Tuple2<String, Long> o2) {
                return o2.f1.intValue() - o1.f1.intValue();
            }
        });
        return result;
    }

    @Override
    public HashMap<String, Long> merge(HashMap<String, Long> stringLongHashMap, HashMap<String, Long> acc1) {
        return null;
    }
}

//实现自定义全窗口函数 包装信息输出结果
public static class UrlAllWindowResult extends ProcessAllWindowFunction<ArrayList<Tuple2<String,Long>>,String, TimeWindow>{
    @Override
    public void process(ProcessAllWindowFunction<ArrayList<Tuple2<String, Long>>, String, TimeWindow>.Context context, Iterable<ArrayList<Tuple2<String, Long>>> elements, Collector<String> out) throws Exception {
        ArrayList<Tuple2<String, Long>> list = elements.iterator().next();


        StringBuilder result = new StringBuilder();
        result.append("==========================\n");
        result.append("窗口结束时间：" + new Timestamp(context.window().getEnd()) + "\n");

        //取list前两个，包装信息输出
        for (int i = 0; i < 2; i++) {
            Tuple2<String, Long> currTuple = list.get(i);
            String info = "No. "+ (i+1) + " " + "url: " + currTuple.f0 + " " +"访问量： " + currTuple.f1 + "\n";
            result.append(info);
        }
        result.append("===============================\n");

        out.collect(result.toString());
```

##### 7.5 侧输出流（Side Output）

​		处理函数还有另外一个特有功能，就是将自定义的数据放入“侧输出流”（side output）输出。

​		具体应用时，只要在处理函数的.processElement()或者.onTimer()方法中，调用上下文 的.output()方法就可以了。

```
DataStream<Integer> stream = env.addSource(...);
SingleOutputStreamOperator<Long> longStream = stream.process(new 
ProcessFunction<Integer, Long>() {
 @Override
 public void processElement( Integer value, Context ctx, Collector<Integer> 
out) throws Exception {
 // 转换成 Long，输出到主流中
 out.collect(Long.valueOf(value));
 // 转换成 String，输出到侧输出流中
 ctx.output(outputTag, "side-output: " + String.valueOf(value));
 }
});
```

​		这里 output()方法需要传入两个参数，**第一个是一个“输出标签”OutputTag，用来标识侧输出流，一般会在外部统一声明；第二个就是要输出的数据。**

​		可以在外部先将 OutputTag 声明出来：

```
OutputTag<String> outputTag = new OutputTag<String>("side-output") {};
```

### 第 8 章 多流转换

​		多流转换可以分为“分流”和“合流”两大类。目前分流的操作一般是通过侧输出流来实现，而合流的算子比较丰富，根据不同的需求可以调用union、connect、join、以及CoGroup等接口进行连接合并操作

##### 8.1 分流

​		分流就是将一条数据流拆分成完全独立的两条、甚至多条流。也就是**基于一个DataStream，得到完全平等的多个子DataStream**，如图所示。**会定义一些筛选条件，将符合条件的数据拣选出来放到对应的流里**

​		![image-20230511145424452](C:\Users\王海涛\AppData\Roaming\Typora\typora-user-images\image-20230511145424452.png)

```
StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
env.setParallelism(1);

SingleOutputStreamOperator<Event> stream = env.addSource(new ClickSource())
        .assignTimestampsAndWatermarks(WatermarkStrategy.<Event>forBoundedOutOfOrderness(Duration.ZERO)
                .withTimestampAssigner(new SerializableTimestampAssigner<Event>() {
                    @Override
                    public long extractTimestamp(Event element, long l) {
                        return element.timestamp;
                    }
                })
        );
//定义一个输出标签
OutputTag<Tuple3<String, String, Long>> maryTag = new OutputTag<Tuple3<String, String, Long>>("Mary") {};
OutputTag<Tuple3<String, String, Long>> bobTag = new OutputTag<Tuple3<String, String, Long>>("Mary") {};


SingleOutputStreamOperator<Event> processStream = stream.process(new ProcessFunction<Event, Event>() {
    @Override
    public void processElement(Event value, ProcessFunction<Event, Event>.Context ctx, Collector<Event> out) throws Exception {
        if (value.user.equals("Mary"))
            ctx.output(maryTag, Tuple3.of(value.user, value.url, value.timestamp));
        else if (value.user.equals("Bob")) {
            ctx.output(bobTag, Tuple3.of(value.user, value.url, value.timestamp));
        } else {
            out.collect(value);
        }
    }
});

processStream.print("else");
processStream.getSideOutput(maryTag).print("Mary");
processStream.getSideOutput(bobTag).print("Bob");

env.execute();
```

##### 8.2 基本合流操作

​		既然一条流可以分开，自然多条流就可以合并。在实际应用中，我们经常会遇到来源不同 的多条流，需要将它们的数据进行联合处理。

###### 8.2.1 联合（Union）

​		最简单的合流操作，就是直接将多条流合在一起，叫作流的“联合”（union），如图 8-2 所示。联合操作要求必须流中的数据类型必须相同，合并之后的新流会包括所有流中的元素， 数据类型不变。

![image-20230511153315401](C:\Users\王海涛\AppData\Roaming\Typora\typora-user-images\image-20230511153315401.png)

​		在代码中，只要基于 DataStream 直接调用.union()方法，传入其他 DataStream 作为参数，就可以实现流的联合了；得到的依然是一个 DataStream：

```
stream1.union(stream2, stream3, ...)
```

###### 8.2.2 连接（Connect）

​		流的联合虽然简单，**不过受限于数据类型不能改变，灵活性大打折扣，所以实际应用较少出现**。除了联合（union），Flink 还提供了另外一种方便的合流操作——连接（connect）。顾名 思义，这种操作就是直接把两条流像接线一样对接起来。

1. 连接流（ConnectedStreams）

​		为了处理更加灵活，连接操作允许流的数据类型不同。但我们知道一个 DataStream 中的 数据只能有唯一的类型，所以连接得到的并不是 DataStream，而是一个“连接流” （ConnectedStreams）。连接流可以看成是两条流形式上的“统一”，被放在了一个同一个流中； 事实上内部仍保持各自的数据形式不变，彼此之间是相互独立的。**要想得到新的 DataStream， 还需要进一步定义一个“同处理”（co-process）转换操作，用来说明对于不同来源、不同类型 的数据，怎样分别进行处理转换、得到统一的输出类型。**所以整体上来，两条流的连接就像是 “一国两制”，两条流可以保持各自的数据类型、处理方式也可以不同，不过最终还是会统一到同一个 DataStream中

![image-20230511154928469](C:\Users\王海涛\AppData\Roaming\Typora\typora-user-images\image-20230511154928469.png)

```
DataStreamSource<Integer> stream1 = env.fromElements(1, 2, 3);
DataStreamSource<Long> stream2 = env.fromElements(4L, 5L, 6L, 7L);

stream2.connect(stream1)
                .map(new CoMapFunction<Long, Integer, String>() {
                    @Override
                    public String map1(Long value) throws Exception {
                        return "Long: " + value.toString();
                    }

                    @Override
                    public String map2(Integer value) throws Exception {
                        return "Integer: " + value.toString();
                    }
                })
        .print();
```

​		ConnectedStreams 有两个类型参数，分别表示内部包含的两条流各自的数据类型；由于需要“一国两制”，**因此调用.map()方法时传入的不再是一个简单的 MapFunction， 而是一个 CoMapFunction，表示分别对两条流中的数据执行 map 操作。这个接口有三个类型参数，依次表示第一条流、第二条流，以及合并后的流中的数据类型。**需要实现的方法也非常直白：.map1()就是对第一条流中数据的 map 操作，.map2()则是针对第二条流。

2. CoProcessFunction

​		对于连接流 ConnectedStreams 的处理操作，需要分别定义对两条流的处理转换，因此接口 中就会有两个相同的方法需要实现，用数字“1”“2”区分，在两条流中的数据到来时分别调 用。我们把这种接口叫作“协同处理函数”（co-process function）。

​		CoProcessFunction 也是“处理函数”家族中的一员，用法非常相 似。**它需要实现的就是 processElement1()、processElement2()两个方法，在每个数据到来时， 会根据来源的流调用其中的一个方法进行处理。CoProcessFunction 同样可以通过上下文 ctx 来 访问 timestamp、水位线，并通过 TimerService 注册定时器；另外也提供了.onTimer()方法，用 于定义定时触发的处理操作。**

​		一个具体实例：实现实时对账的需求，app 的支付操作和第三方的支付操作的一个双流 Join。App 的支付事件和第三方的支付事件将 会互相等待 5 秒钟，如果等不来对应的支付事件，那么就输出报警信息。

```
StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
    env.setParallelism(1);

    //来自app的支付日志
    SingleOutputStreamOperator<Tuple3<String, String, Long>> appstream = env.fromElements(
            Tuple3.of("order-1", "app", 1000L),
            Tuple3.of("order-2", "app", 2000L),
            Tuple3.of("order-3", "app", 3500L)
    ).assignTimestampsAndWatermarks(WatermarkStrategy.<Tuple3<String, String, Long>>forBoundedOutOfOrderness(Duration.ZERO)
            .withTimestampAssigner(new SerializableTimestampAssigner<Tuple3<String, String, Long>>() {
                @Override
                public long extractTimestamp(Tuple3<String, String, Long> element, long l) {
                    return element.f2;
                }
            })
    );

    //来自第三方支付平台的支付日志
    SingleOutputStreamOperator<Tuple4<String, String, String, Long>> thirdpartstream = env.fromElements(
            Tuple4.of("order-1", "third-party", "success", 3000L),
            Tuple4.of("order-3", "third-party", "success", 4000L)
    ).assignTimestampsAndWatermarks(WatermarkStrategy.<Tuple4<String, String, String,Long>>forBoundedOutOfOrderness(Duration.ZERO)
            .withTimestampAssigner(new SerializableTimestampAssigner<Tuple4<String, String, String,Long>>() {
                @Override
                public long extractTimestamp(Tuple4<String, String, String,Long> element, long l) {
                    return element.f3;
                }
            })
    );

    // 检测统一支付单在两条流中是否匹配，不匹配就报警
    appstream.keyBy(data -> data.f0)
                    .connect(thirdpartstream.keyBy(data -> data.f0));

    appstream.connect(thirdpartstream)
                    .keyBy(data -> data.f0, data -> data.f0)
                            .process(new OrderMatchResult())
                                    .print();


    env.execute();
}

//自定义实现CoProcessFunc
public static class OrderMatchResult extends CoProcessFunction<Tuple3<String,String,Long>,Tuple4<String,String,String,Long>, String>{
    //定义状态，用来保存已经到达的事件
    private ValueState<Tuple3<String, String, Long>> appEventState;
    private ValueState<Tuple4<String, String, String, Long>> thirdPartyEventState;

    @Override
    public void open(Configuration parameters) throws Exception {
         appEventState = getRuntimeContext().getState(
                new ValueStateDescriptor<Tuple3<String, String, Long>>("app-event", Types.TUPLE(Types.STRING, Types.STRING, Types.LONG))
        );
         thirdPartyEventState = getRuntimeContext().getState(
                 new ValueStateDescriptor<Tuple4<String, String, String, Long>>("thirdParty-event",Types.TUPLE(Types.STRING, Types.STRING, Types.STRING,Types.LONG))
         );
    }

    @Override
    public void processElement1(Tuple3<String, String, Long> value, CoProcessFunction<Tuple3<String, String, Long>, Tuple4<String, String, String, Long>, String>.Context ctx, Collector<String> out) throws Exception {
        //来的是app event, 看另一条流中事件是否来过
        if (thirdPartyEventState.value() != null){
            out.collect("对账成功： " + value + " " + thirdPartyEventState.value());
            // 清空状态
            thirdPartyEventState.clear();
        }else {
            //更新状态
            appEventState.update(value);
            //定义注册一个5s定时器，等待另一条流的事件
            ctx.timerService().registerEventTimeTimer(value.f2 + 5000L);
        }
    }

    @Override
    public void processElement2(Tuple4<String, String, String, Long> value, CoProcessFunction<Tuple3<String, String, Long>, Tuple4<String, String, String, Long>, String>.Context ctx, Collector<String> out) throws Exception {
        //来的是app event, 看另一条流中事件是否来过
        if (appEventState.value() != null){
            out.collect("对账成功： " + appEventState.value() + " " + value);
            // 清空状态
            appEventState.clear();
        }else {
            //更新状态
            thirdPartyEventState.update(value);
            //定义注册一个5s定时器，等待另一条流的事件
            ctx.timerService().registerEventTimeTimer(value.f3);
        }
    }

    @Override
    public void onTimer(long timestamp, CoProcessFunction<Tuple3<String, String, Long>, Tuple4<String, String, String, Long>, String>.OnTimerContext ctx, Collector<String> out) throws Exception {
        // 定时器触发，判断状态，如果某个状态不为空，说明另一条流中事件没来
        if (appEventState.value() != null){
            out.collect("当前对账失败： " + appEventState.value() + " " + "第三方支付平台信息未到" );
        }

        if (thirdPartyEventState.value() != null){
            out.collect("当前对账失败： " + thirdPartyEventState.value() + " " + "app支付平台信息未到" );
        }
        appEventState.clear();
        thirdPartyEventState.clear();
    }
```

3. 广播连接流（BroadcastConnectedStream）

   ​	DataStream 调用.connect()方法时，传入的 参数也可以不是一个 DataStream，而是一个“广播流”（BroadcastStream），这时合并两条流得 到的就变成了一个“广播连接流”（BroadcastConnectedStream）。

​		这种连接方式往往用在需要动态定义某些规则或配置的场景。因为规则是实时变动的，所以我们可以用一个单独的流来获取规则数据；而这些规则或配置是对整个应用全局有效的，所 以不能只把这数据传递给一个下游并行子任务处理，而是要“广播”（broadcast）给所有的并行子任务。而下游子任务收到广播出来的规则，会把它保存成一个状态，这就是所谓的“广播 状态”（broadcast state）。

​		将要处理的数据流，与这条广播流进行连接（connect），得到的就是所谓的“广播连接流”（BroadcastConnectedStream）。基于 BroadcastConnectedStream 调用 .process() 方法，就可以同时获取规则和数据，进行动态处理了

```
DataStream<String> output = stream
 .connect(ruleBroadcastStream)
 .process( new BroadcastProcessFunction<>() {...} );
```

##### 8.3 基于时间的合流—双流联结（Join）

​		如果我们希望将两条流的数据进行 合并、且同样针对某段时间进行处理和统计，又该怎么做呢？		

###### 8.3.1 窗口联结（Window Join）

​		Flink 为这种场景专门提供了一个窗口联结（window join）算子，可以定义时间窗口，并 将两条流中共享一个公共键（key）的数据放在窗口中进行配对处理。

1. 窗口联结的调用

​		窗口联结在代码中的实现，首先需要调用 DataStream 的.join()方法来合并两条流，得到一 个 JoinedStreams；接着通过.where()和.equalTo()方法指定两条流中联结的 key；然后通 过.window()开窗口，并调用.apply()传入联结窗口函数进行处理计算。

```
stream1.join(stream2)
	   .where(<KeySelector>)
 	   .equalTo(<KeySelector>)
 	   .window(<WindowAssigner>)
	   .apply(<JoinFunction>)
```

2. 窗口联结的处理流程

​		两条流的数据到来之后，首先会按照 key 分组、进入对应的窗口中存储；**当到达窗口结束时间时，算子会先统计出窗口内两条流的数据的所有组合，也就是对两条流中的数据做一个笛卡尔积（相当于表的交叉连接，cross join），然后进行遍历，把每一对匹配的数据，作为参数 (first，second)传入 JoinFunction 的.join()方法进行计算处理，得到的结果直接输出如图 8-8 所 示。所以窗口中每有一对数据成功联结匹配，JoinFunction 的.join()方法就会被调用一次，并输出一个结果。**

![image-20230511202340517](C:\Users\王海涛\AppData\Roaming\Typora\typora-user-images\image-20230511202340517.png)

案例实操：

```
stream1.join(stream2)
                        .where(data -> data.f0)
                                .equalTo(data -> data.f0)
                                        .window(TumblingEventTimeWindows.of(Time.seconds(2)))
                                                .apply(new JoinFunction<Tuple2<String, Long>, Tuple2<String, Integer>, String>() {
                                                    @Override
                                                    public String join(Tuple2<String, Long> first, Tuple2<String, Integer> second) throws Exception {
                                                        return first + " -> " + second;
                                                    }
                                                }).print();

```

###### 8.3.2 间隔联结（Interval Join）

​		在有些场景下，我们要处理的时间间隔可能并不是固定的。比如，在交易系统中，需要实 时地对每一笔交易进行核验，保证两个账户转入转出数额相等，也就是所谓的“实时对账”。 两次转账的数据可能写入了不同的日志流，它们的时间戳应该相差不大，所以我们可以考虑只统计一段时间内是否有出账入账的数据匹配。这时显然不应该用滚动窗口或滑动窗口来处理— —因为匹配的两个数据有可能刚好“卡在”窗口边缘两侧，于是窗口内就都没有匹配了；会话窗口虽然时间不固定，但也明显不适合这个场景。 基于时间的窗口联结已经无能为力了。

​		为了应对这样的需求，Flink 提供了一种叫作“间隔联结”（interval join）的合流操作。顾名思义，**间隔联结的思路就是针对一条流的每个数据，开辟出其时间戳前后的一段时间间隔， 看这期间是否有来自另一条流的数据匹配。**

  		1. 间隔联结的调用

​		间隔联结在代码中，是基于 KeyedStream 的联结（join）操作。DataStream 在 keyBy 得到 KeyedStream 之后，可以调用.intervalJoin()来合并两条流，传入的参数同样是一个 KeyedStream， 两者的 key 类型应该一致；

​		先通过.between()方法指定间隔的上下界，再调用.process()方法，定义对匹配数据对的处理操 作。调用.process()需要传入一个处理函数

```
stream1 .keyBy() 
.intervalJoin(stream2.keyBy()) .between(Time.milliseconds(-2), Time.milliseconds(1)) .process (new ProcessJoinFunction out) { 			 
		out.collect(left + "," + right); 
	} 
});
```

2. 间隔联结实例

```
StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
env.setParallelism(1);

SingleOutputStreamOperator<Tuple2<String, Long>> stream1 = env.fromElements(
        Tuple2.of("a", 1000L),
        Tuple2.of("b", 1000L),
        Tuple2.of("a", 2000L),
        Tuple2.of("b", 2000L)
).assignTimestampsAndWatermarks(WatermarkStrategy.<Tuple2<String, Long>>forBoundedOutOfOrderness(Duration.ZERO)
        .withTimestampAssigner(new SerializableTimestampAssigner<Tuple2<String, Long>>() {
            @Override
            public long extractTimestamp(Tuple2<String, Long> element, long l) {
                return element.f1;
            }
        }));

SingleOutputStreamOperator<Tuple2<String, Integer>> stream2 = env.fromElements(
        Tuple2.of("a", 3000),
        Tuple2.of("b", 4000),
        Tuple2.of("a", 4500),
        Tuple2.of("b", 5500)
).assignTimestampsAndWatermarks(WatermarkStrategy.<Tuple2<String, Integer>>forBoundedOutOfOrderness(Duration.ZERO)
        .withTimestampAssigner(new SerializableTimestampAssigner<Tuple2<String, Integer>>() {
            @Override
            public long extractTimestamp(Tuple2<String, Integer> element, long l) {
                return element.f1;
            }
        }));

stream1.join(stream2)
                .where(data -> data.f0)
                        .equalTo(data -> data.f0)
                                .window(TumblingEventTimeWindows.of(Time.seconds(2)))
                                        .apply(new JoinFunction<Tuple2<String, Long>, Tuple2<String, Integer>, String>() {
                                            @Override
                                            public String join(Tuple2<String, Long> first, Tuple2<String, Integer> second) throws Exception {
                                                return first + " -> " + second;
                                            }
                                        }).print();
env.execute();
```

###### 8.3.3 窗口同组联结（Window CoGroup）

​		除窗口联结和间隔联结之外，Flink 还提供了一个“窗口同组联结”（window coGroup）操 作。它的用法跟 window join 非常类似，也是将两条流合并之后开窗处理匹配的元素，调用时 只需要将.join()换为.coGroup()就可以了。

```
stream1.coGroup(stream2)
 .where(<KeySelector>)
 .equalTo(<KeySelector>)
 .window(TumblingEventTimeWindows.of(Time.hours(1)))
 .apply(<CoGroupFunction>)
```

​	与 window join 的区别在于，调用.apply()方法定义具体操作时，传入的是一个 CoGroupFunction。

​		内部的.coGroup()方法，有些类似于 FlatJoinFunction 中.join()的形式，**同样有三个参数， 分别代表两条流中的数据以及用于输出的收集器（Collector）**。不同的是，这里的前两个参数 不再是单独的每一组“配对”数据了，而是传入了可遍历的数据集合。

​		CoGroup操作比窗口的join更加通用，不仅可以实现类似SQL中的“内连接（inner join）”，也可以实现左右外连接和全外连接。

```
stream1.coGroup(stream2)
        .where(data -> data.f0)
        .equalTo(data -> data.f0)
        .window(TumblingEventTimeWindows.of(Time.seconds(5)))
        .apply(new CoGroupFunction<Tuple2<String, Long>, Tuple2<String, Integer>, String>() {
            @Override
            public void coGroup(Iterable<Tuple2<String, Long>> first, Iterable<Tuple2<String, Integer>> second, Collector<String> out) throws Exception {
                out.collect(first + "=>" + second);
            }
        }).print();
```

### 第 9 章 状态编程

​		Flink 处理机制的核心，就是“有状态的流式计算”。不论是简单聚合、窗口聚合，还是处理函数的应用，都会有状态的身影出现。状态就如同事务处理时数据库中保存的 信息一样，是用来辅助进行任务计算的数据。

##### 9.1 Flink中的状态

​		在流处理中，数据是连续不断到来和处理的。每个任务进行计算处理时，可以基于当前数据直接转换得到输出结果；**也可以依赖一些其他数据。这些由一个任务维护，并且用来计算输出结果的所有数据，就叫作这个任务的状态。**

###### 9.1.1 有状态算子

​		在 Flink 中，算子任务可以分为无状态和有状态两种情况。

​		无状态的算子任务**只需要观察每个独立事件，根据当前输入的数据直接转换输出结果**，如图所示。例如，可以将一个字符串类型的数据拆分开作为元组输出；也可以对数据做一些 计算，比如每个代表数量的字段加 1。之前学的基本转换算子，如 map、filter、flatMap， **计算时不依赖其他数据，就都属于无状态的算子**。

![image-20230511215646631](C:\Users\王海涛\AppData\Roaming\Typora\typora-user-images\image-20230511215646631.png)

​		而有状态的算子任务，**则除当前数据之外，还需要一些其他数据来得到计算结果。这里的 “其他数据”，就是所谓的状态（state）**，最常见的就是之前到达的数据，或者由之前数据计算出的某个结果。比如，做求和（sum）计算时，需要保存之前所有数据的和，这就是状态；窗口算子中会保存已经到达的所有数据，这些也都是它的状态。另外，如果我们希望检索到某种 “事件模式”（event pattern），比如“先有下单行为，后有支付行为”，那么也应该把之前的行为保存下来，这同样属于状态。容易发现，之前学过的的聚合算子、窗口算子都属于有状态的算子。

![image-20230511215747462](C:\Users\王海涛\AppData\Roaming\Typora\typora-user-images\image-20230511215747462.png)

图为有状态算子的一般处理流程，具体步骤如下：

​	（1）算子任务接收到上游发来的数据； 

​	（2）获取当前状态； 

​	（3）根据业务逻辑进行计算，更新状态； 

​	（4）得到计算结果，输出发送到下游任务。

###### 9.1.2 状态的管理

​		在传统的事务型处理架构中，这种额外的状态数据是保存在数据库中的。而对于实时流处理来说，这样做需要频繁读写外部数据库，如果数据规模非常大肯定就达不到性能要求了。所以Flink的解决方案是，**将状态直接保存在内存中来保证性能，并通过分布式扩展来提高吞吐量**。

​		这样看来状态的管理似乎非常简单，我们直接把它作为一个对象交给 JVM 就可以了。然 而大数据的场景下，我们必须使用分布式架构来做扩展，在低延迟、高吞吐的基础上还要保证容错性，一系列复杂的问题就会随之而来了。

（1）状态的访问权限：Flink 上的聚合和窗口操作，一般都是基于 KeyedStream的，数据会按照 key 的哈希值进行分区，聚合处理的结果也应该是只对当前 key 有效。 然而**同一个分区（也就是 slot）上执行的任务实例，可能会包含多个 key 的数据，它们同时访问和更改本地变量，就会导致计算结果错误。所以这时状态并不是单纯的本地变量**。

（2）容错性：也就是故障后的恢复。**状态只保存在内存中显然是不够稳定的，我们需要将它持久化保存，做一个备份；在发生故障后可以从这个备份中恢复状态。**

（3）分布式应用的横向扩展性。比如**处理的数据量增大时，我们应该相应地对计算资源扩容，调大并行度**。这时就涉及到了状态的重组调整。当某个节点故障时，就会把当前节点的子任务和状态重组，并分配到其他节点上

###### 9.1.3 状态的分类

1. 托管状态（Managed State）和原始状态（Raw State）

​		Flink 的状态有两种：托管状态（Managed State）和原始状态（Raw State）。**托管状态就是由 Flink 统一管理的，状态的存储访问、故障恢复和重组等一系列问题都由 Flink 实现，我们只要调接口就可以；*****而原始状态则是自定义的，相当于就是开辟了一块内存，需要我们自己管理，实现状态的序列化和故障恢复。**

​		托管状态是由 Flink 的运行时（Runtime）来托管的；**在配置容错机制后，状态会自动持久化保存，并在发生故障时自动恢复。当应用发生横向扩展时，状态也会自动地重组分配到所有的子任务实例上**。对于具体的状态内容，Flink 也提供了值状态（ValueState）、 列表状态（ListState）、映射状态（MapState）、聚合状态（AggregateState）等多种结构，内部支持各种数据类型。聚合、窗口等算子中内置的状态，就都是托管状态；

​		原始状态需要自定义。Flink 不会对状态进行任何自动操作，也不知道状态的具体数据类型，只会把它当作最原始的字节（Byte）数组来存储。我们需要花费大 量的精力来处理状态的管理和维护。

2. 算子状态（Operator State）和按键分区状态（Keyed State）

​		在 Flink 中，一个算子任务会按照并行度分为多个并行子任务执行，而不同的子任务会占据不同的任务槽（task slot）。由于不同的 slot 在计算资源上是物理隔离的，所以 **Flink 能管理的状态在并行任务间是无法共享的，每个状态只能针对当前子任务的实例有效。**

​		（1）算子状态

​		状态作用范围限定为当前的算子任务实例，也就是**只对当前并行子任务实例有效**。这就意味着对于一个并行子任务，占据了一个“分区”，**它所处理的所有数据都会访问到相同的状态， 状态对于同一任务而言是共享的**

![image-20230514134757853](C:\Users\王海涛\AppData\Roaming\Typora\typora-user-images\image-20230514134757853.png)

​		（2）按键分区状态

​		状态是根据输入流中定义的键（key）来维护和访问的，所以只能定义在按键分区流 （KeyedStream）中，也就是keyBy 之后才可以使用

​		按键分区状态应用非常广泛。之前讲到的聚合算子必须在 keyBy 之后才能使用，就是因为聚合的结果是以 Keyed State 的形式保存的。另外，也可以通过富函数类（Rich Function） 来自定义Keyed State，所以只要提供了富函数类接口的算子，也都可以使用 Keyed State。

​		

![image-20230514134911956](C:\Users\王海涛\AppData\Roaming\Typora\typora-user-images\image-20230514134911956.png)

##### 9.2 按键分区状态（Key State）

​		在实际应用中，我们一般都需要将数据按照某个 key 进行分区，然后再进行计算处理；所以最为常见的状态类型就是 Keyed State。之前介绍到 keyBy 之后的聚合、窗口计算，算子所持有的状态，都是 Keyed State。 

​		另外，我们还可以通过富函数类（Rich Function）对转换算子进行扩展、实现自定义功能， 比如 RichMapFunction、RichFilterFunction。在富函数中，我们可以调用.getRuntimeContext() 获取当前的运行时上下文（RuntimeContext），进而获取到访问状态的句柄；这种富函数中自定义的状态也是 Keyed State。

###### 9.2.1 基本概念和特点

​		按键分区状态（Keyed State）顾名思义，是任务按照键（key）来访问和维护的状态。它 的特点非常鲜明，就是以 key 为作用范围进行隔离。

​		**在应用的并行度改变时，状态也需要随之进行重组。不同 key 对应的 Keyed State 可以进一步组成所谓的键组（key groups），每一组都对应着一个并行子任务。**键组是 Flink 重 新分配 Keyed State 的单元，键组的数量就等于定义的最大并行度。当算子并行度发生改变时， Keyed State 就会按照当前的并行度重新平均分配，保证运行时各个子任务的负载相同。

​		**使用 Keyed State 必须基于 KeyedStream。**没有进行 keyBy 分区的 DataStream， 即使转换算子实现了对应的富函数类，也不能通过运行时上下文访问 Keyed State。

###### 9.2.2 支持的结构类型

1. 值状态（ValueState）

状态中只保存一个“值”（value）。ValueState本身是一个接口，

```
public interface ValueState<T> extends State {
	
	T value() throws IOException;
	
	void update(T value) throws IOException;
}
```

可以在代码中读写值状态，实现对于状态的访问和更新。 

⚫ T value()：获取当前状态的值； 

⚫ update(T value)：对状态进行更新，传入的参数 value 就是要覆写的状态值。 

​		在具体使用时，为了让运行时上下文清楚到底是哪个状态，我们还需要创建一个“状态描述器”（StateDescriptor）来提供状态的基本信息。源码中，ValueState 的状态描述器构造 方法如下： 

```
public ValueStateDescriptor(String name, Class typeClass) { 
		super(name, typeClass, null); 
} 
```

这里需要传入状态的名称和类型——这跟我们声明一个变量时做的事情完全一样。有了这个描述器，运行时环境就可以获取到状态的控制句柄（handler）了。

2. 列表状态（ListState）

​		将需要保存的数据，以列表（List）的形式组织起来。在 ListState接口中同样有一个类型参数 T，表示列表中数据的类型。ListState也提供了一系列的方法来操作状态，使用方式与一般的 List 非常相似。

​		⚫ Iterable get()：获取当前的列表状态，返回的是一个可迭代类型 Iterable； 

​		⚫ update(List values)：传入一个列表 values，直接对状态进行覆盖； 

​		⚫ add(T value)：在状态列表中添加一个元素 value； 

​		⚫ addAll(List values)：向列表中添加多个元素，以列表 values 形式传入。 

​		类似地，ListState 的状态描述器就叫作 ListStateDescriptor，用法跟 ValueStateDescriptor 完全一致。

3. 映射状态（MapState）

​		把一些键值对（key-value）作为状态整体保存起来，可以认为就是一组 key-value 映射的 列表。对应的 MapState接口中，就会有 UK、UV 两个泛型，分别表示保存的 key 和 value 的类型。同样，MapState 提供了操作映射状态的方法，与 Map 的使用非常类似。

⚫ UV get(UK key)：传入一个 key 作为参数，查询对应的 value 值； 

⚫ put(UK key, UV value)：传入一个键值对，更新 key 对应的 value 值； 

⚫ putAll(Map map)：将传入的映射 map 中所有的键值对，全部添加到映射状 态中； 

⚫ remove(UK key)：将指定 key 对应的键值对删除； 

⚫ boolean contains(UK key)：判断是否存在指定的 key，返回一个 boolean 值。 另外，MapState 也提供了获取整个映射相关信息的方法： 

⚫ Iterable> entries()：获取映射状态中所有的键值对； 

⚫ Iterable keys()：获取映射状态中所有的键（key），返回一个可迭代 Iterable 类型； 

⚫ Iterable values()：获取映射状态中所有的值（value），返回一个可迭代 Iterable 类型； 

⚫ boolean isEmpty()：判断映射是否为空，返回一个 boolean 值。

4. 归约状态（ReducingState）

​		需要对添加进来的所有数据进行归约，将归约聚合之后的值 作为状态保存下来。ReducintState这个接口调用的方法类似于 ListState，只不过它保存的只是一个聚合值，所以**调用.add()方法时，不是在状态列表里添加元素，而是直接把新数据和 之前的状态进行归约，并用得到的结果更新状态。**

​		归约逻辑的定义，是在归约状态描述器（ReducingStateDescriptor）中，通过传入一个归 约函数（ReduceFunction）来实现的。

```
public ReducingStateDescriptor(
 String name, ReduceFunction<T> reduceFunction, Class<T> typeClass) {...}
```

​		这里的描述器有三个参数，其中第二个参数就是定义了归约聚合逻辑的 ReduceFunction， 另外两个参数则是状态的名称和类型。

5. 聚合状态（AggregatingState）

​		与归约状态非常类似，聚合状态也是一个值，用来保存添加进来的所有数据的聚合结果。 与 ReducingState 不同的是，它的聚合逻辑是由在描述器中传入一个更加一般化的聚合函数（AggregateFunction）来定义的；这也就是之前我们讲过的 AggregateFunction，里面通过一个累加器（Accumulator）来表示状态，所以聚合的状态类型可以跟添加进来的数据类型完全不 同，使用更加灵活。 

​		同样地，AggregatingState 接口调用方法也与 ReducingState 相同，调用.add()方法添加元素时，会直接使用指定的 AggregateFunction 进行聚合并更新状态。

###### 9.2.3 代码实现

​		状态始终是与特定算子相关联的；算子在使用状态前首先需要“注册”，其 实就是告诉 Flink 当前上下文中定义状态的信息。

​		**状态的注册，主要是通过“状态描述器”（StateDescriptor）来实现的。状态描述器中最重 要的内容，就是状态的名称（name）和类型（type）。**

​		值状态，列表状态，映射状态的状态描述器需要传入状态名称和类型，此外归约状态和聚合状态还需要传入实现运算逻辑的自定义函数。

​		代码中使用状态的完整步骤如下：

1. 在方法外先声明状态；

```
 	ValueState<Long> countState;
    ValueState<Long> timerTsState;
```

2. 重写生命周期方法open（），在open中获取状态对象，创建状态描述器

```
public void open(Configuration parameters) throws Exception {
            countState = getRuntimeContext().getState(new ValueStateDescriptor<Long>("count",Long.class));
        }
```

3. 使用状态进行存储中间值

```
public void processElement(Event value, KeyedProcessFunction<String, Event, String>.Context ctx, Collector<String> out) throws Exception {
    //每来一条数据，就更新对应的count值
    Long count = countState.value();
    countState.update(count == null ? 1 : count + 1);

    //注册定时器.
    if (timerTsState.value() == null){
        ctx.timerService().registerEventTimeTimer(value.timestamp + 10000L);
        timerTsState.update(value.timestamp + 10000L);
    }
}
```

接下来介绍不同类型状态的应用实例：

1. 值状态

​		使用用户 id 来进行分流，然后分别统计每个用户的 pv 数据，由于但是不能每次 pv 加一，就将统计结果发送到下游去，所以这里我们注册了一个定时器，用来隔一段时间发送 pv 的统计结果。

​		具体实现方式是定义一个用来保 存定时器时间戳的值状态变量。当定时器触发并向下游发送数据以后，便清空储存定时器时间 戳的状态变量，这样当新的数据到来时，发现并没有定时器存在，就可以注册新的定时器了， 注册完定时器之后将定时器的时间戳继续保存在状态变量中。

```
public static void main(String[] args) throws Exception {
    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
    env.setParallelism(1);

    SingleOutputStreamOperator<Event> stream = env.addSource(new ClickSource())
            .assignTimestampsAndWatermarks(WatermarkStrategy.<Event>forBoundedOutOfOrderness(Duration.ZERO)
                    .withTimestampAssigner(new SerializableTimestampAssigner<Event>() {
                        @Override
                        public long extractTimestamp(Event event, long timestamp) {
                            return event.timestamp;
                        }
                    }));
    stream.print("input");

    // 统计每个用户的pv
    stream.keyBy(data -> data.user)
                    .process(new PeridicPvResult())
                            .print();
    env.execute();
}

//实现自定义的KeyProcessFunction
public static class PeridicPvResult extends KeyedProcessFunction<String,Event,String>{
    //定义状态，保存当前pv统计值,以及有么有定时器
    ValueState<Long> countState;
    ValueState<Long> timerTsState;

    @Override
    public void open(Configuration parameters) throws Exception {
        countState = getRuntimeContext().getState(new ValueStateDescriptor<Long>("count",Long.class));
        timerTsState = getRuntimeContext().getState(new ValueStateDescriptor<Long>("timer-ts",Long.class));
    }

    @Override
    public void processElement(Event value, KeyedProcessFunction<String, Event, String>.Context ctx, Collector<String> out) throws Exception {
        //每来一条数据，就更新对应的count值
        Long count = countState.value();
        countState.update(count == null ? 1 : count + 1);

        //注册定时器.
        if (timerTsState.value() == null){
            ctx.timerService().registerEventTimeTimer(value.timestamp + 10000L);
            timerTsState.update(value.timestamp + 10000L);
        }
    }

    @Override
    public void onTimer(long timestamp, KeyedProcessFunction<String, Event, String>.OnTimerContext ctx, Collector<String> out) throws Exception {
        //定时器触发，输出一次统计结果
        out.collect(ctx.getCurrentKey() + "的pv: " + countState.value());
        //清空状态
        timerTsState.clear();
    }
}
```

2. 列表状态（ListState）

```
StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
env.setParallelism(1);

SingleOutputStreamOperator<Tuple3<String, String, Long>> stream1 = env.fromElements(
        Tuple3.of("a", "stream-1", 1000L),
        Tuple3.of("b", "stream-1", 2000L)
).assignTimestampsAndWatermarks(WatermarkStrategy.<Tuple3<String, String, Long>>forMonotonousTimestamps()
        .withTimestampAssigner(new SerializableTimestampAssigner<Tuple3<String, String, Long>>() {
            @Override
            public long extractTimestamp(Tuple3<String, String, Long> element, long recordtimestamp) {
                return element.f2;
            }
        }));

SingleOutputStreamOperator<Tuple3<String, String, Long>> stream2 = env.fromElements(
        Tuple3.of("a", "stream-2", 3000L),
        Tuple3.of("b", "stream-2", 4000L)
).assignTimestampsAndWatermarks(WatermarkStrategy.<Tuple3<String, String, Long>>forMonotonousTimestamps()
        .withTimestampAssigner(new SerializableTimestampAssigner<Tuple3<String, String, Long>>() {
            @Override
            public long extractTimestamp(Tuple3<String, String, Long> element, long recordtimestamp) {
                return element.f2;
            }
        }));
//自定义列表状态进行全外连接
stream1.keyBy(data -> data.f0)
        .connect(stream2.keyBy(data ->data.f0))
        .process(new CoProcessFunction<Tuple3<String, String, Long>, Tuple3<String, String, Long>, String>() {
            //定义列表状态用于保存两条流已经到达的所有数据
            private ListState<Tuple2<String, Long>> stream1ListState;
            private ListState<Tuple2<String, Long>> stream2ListState;

            @Override
            public void open(Configuration parameters) throws Exception {
                stream1ListState = getRuntimeContext().getListState(new ListStateDescriptor<Tuple2<String, Long>>("stream1-list", Types.TUPLE(Types.STRING,Types.LONG)));
                stream2ListState = getRuntimeContext().getListState(new ListStateDescriptor<Tuple2<String, Long>>("stream2-list", Types.TUPLE(Types.STRING,Types.LONG)));
            }

            @Override
            public void processElement1(Tuple3<String, String, Long> left, CoProcessFunction<Tuple3<String, String, Long>, Tuple3<String, String, Long>, String>.Context ctx, Collector<String> out) throws Exception {
                    //获取另一条流中所有数据，配对输出
                    for (Tuple2<String, Long> right : stream2ListState.get()){
                        out.collect(left + "=>" + right);
                    }
                    stream1ListState.add(Tuple2.of(left.f0, left.f2));
            }

            @Override
            public void processElement2(Tuple3<String, String, Long> right, CoProcessFunction<Tuple3<String, String, Long>, Tuple3<String, String, Long>, String>.Context ctx, Collector<String> out) throws Exception {
                    for (Tuple2<String, Long> left : stream1ListState.get()){
                        out.collect(left + "=>" + right);
                    }
                    stream2ListState.add(Tuple2.of(right.f0, right.f2));
            }
        })
        .print();
```

3. 映射状态

```
 StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
    env.setParallelism(1);
    env.getConfig().setAutoWatermarkInterval(100);

    SingleOutputStreamOperator<Event> stream = env.addSource(new ClickSource())
            .assignTimestampsAndWatermarks(WatermarkStrategy.<Event>forBoundedOutOfOrderness(Duration.ZERO)
                    .withTimestampAssigner(new SerializableTimestampAssigner<Event>() {
                        @Override
                        public long extractTimestamp(Event event, long l) {
                            return event.timestamp;
                        }
                    })
            );
    stream.print("input");

    stream.keyBy(data -> data.url)
                    .process(new FakeWindowResult(10000L))
                            .print();

    env.execute();
}

//实现自定义的KeyedProcessFunction
public static class FakeWindowResult extends KeyedProcessFunction<String,Event,String>{
    private Long windowSize;//窗口大小

    public FakeWindowResult(Long windowSize) {
        this.windowSize = windowSize;
    }

    //定义一个MapState，用来保存每个窗口中统计的count值
    MapState<Long,Long> windowURLCountMapState;

    @Override
    public void processElement(Event value, KeyedProcessFunction<String, Event, String>.Context ctx, Collector<String> out) throws Exception {
        //每来一条数据，根据时间戳判断属于哪个窗口（窗口分配器）
        Long windowStart = value.timestamp / windowSize *windowSize;
        Long windowEnd = windowStart + windowSize;

        //注册End-1 的定时器
        ctx.timerService().registerEventTimeTimer(windowEnd - 1);

        //更新状态进行增量聚合
        if (windowURLCountMapState.contains(windowStart)){
            Long count = windowURLCountMapState.get(windowStart);
            windowURLCountMapState.put(windowStart,count + 1);
        }else
            windowURLCountMapState.put(windowStart,1L);
    }

    //定时器触发时输出结果
    @Override
    public void onTimer(long timestamp, KeyedProcessFunction<String, Event, String>.OnTimerContext ctx, Collector<String> out) throws Exception {
        Long windowEnd = timestamp + 1;
        Long windowStart = windowEnd - windowSize;
        Long count =windowURLCountMapState.get(windowStart);

        out.collect("窗口" + new Timestamp(windowStart) + "~" + new Timestamp(windowEnd)
        + "url" + ctx.getCurrentKey()
        + "count: " + count
        );

        //模拟窗口的关闭，清除map对应的key-value
        windowURLCountMapState.remove(windowStart);
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        windowURLCountMapState = getRuntimeContext().getMapState(new MapStateDescriptor<Long, Long>("window-count",Long.class,Long.class));

    }
```

4. 聚合状态（Reducing State）

```
StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
    env.setParallelism(1);
    env.getConfig().setAutoWatermarkInterval(100);

    SingleOutputStreamOperator<Event> stream = env.addSource(new ClickSource())
            .assignTimestampsAndWatermarks(WatermarkStrategy.<Event>forBoundedOutOfOrderness(Duration.ZERO)
                    .withTimestampAssigner(new SerializableTimestampAssigner<Event>() {
                        @Override
                        public long extractTimestamp(Event event, long l) {
                            return event.timestamp;
                        }
                    })
            );
    stream.print("input");

    //自定义实现平均时间戳的统计
    stream.keyBy(data ->data.user)
                    .flatMap(new AvgTsResult(5L))
                            .print();

    env.execute();
}

//实现自定义的RichFlatmapFunction
public static class AvgTsResult extends RichFlatMapFunction<Event,String>{

    private Long count;

    public AvgTsResult(Long count) {
        this.count = count;
    }

    //定义一个聚合状态，用来保存平均时间戳
    AggregatingState<Event,Long> avgTsAggState;

    //定义一个值状态保存用户访问的次数
    ValueState<Long> countState;

    @Override
    public void open(Configuration parameters) throws Exception {
        avgTsAggState = getRuntimeContext().getAggregatingState(new AggregatingStateDescriptor<Event, Tuple2<Long,Long>, Long>(
                "avg-ts",
                new AggregateFunction<Event, Tuple2<Long, Long>, Long>() {
                    @Override
                    public Tuple2<Long, Long> createAccumulator() {
                        return Tuple2.of(0L,0L);
                    }

                    @Override
                    public Tuple2<Long, Long> add(Event value, Tuple2<Long, Long> accumulator) {
                        return Tuple2.of(accumulator.f0 + value.timestamp, accumulator.f1 + 1);
                    }

                    @Override
                    public Long getResult(Tuple2<Long, Long> longLongTuple2) {
                        return createAccumulator().f0 / createAccumulator().f1;
                    }

                    @Override
                    public Tuple2<Long, Long> merge(Tuple2<Long, Long> longLongTuple2, Tuple2<Long, Long> acc1) {
                        return null;
                    }
                },
                Types.TUPLE(Types.LONG, Types.LONG)
        ));
        countState = getRuntimeContext().getState(new ValueStateDescriptor<Long>("count",Long.class));
    }

    @Override
    public void flatMap(Event value, Collector<String> out) throws Exception {
        //每来一条数据，currcount + 1
        Long currcount = countState.value();
        if (currcount == null)
            currcount = 1L;
        else
            currcount ++;

        //更新状态
        countState.update(currcount);
        avgTsAggState.add(value);

        //如果达到count次数就输出结果
        if (currcount.equals(count)){
            out.collect(value.user + "过去" + count + "次访问平均时间戳为： " + avgTsAggState.get());
            //清理状态
            countState.clear();
            avgTsAggState.clear();
        }
    }
```

###### 9.2.4 状态生存时间（TTL）

​		在实际应用中，很多状态会随着时间的推移逐渐增长，如果不加以限制，最终就会导致存储空间的耗尽。一个优化的思路是**直接在代码中调用.clear()方法去清除状态**，但是有时候我们的逻辑要求不能直接清除。这时就需要配置一个状态的“生存时间”（time-to-live，TTL），当状态在内存中存在的时间超出这个值时，就将它清除。

​		状态创建的时候，设置失效时间 = 当前时间 + TTL；

​		配置状态的 TTL 时，需要创建一个 StateTtlConfig 配置对象，然后调用状态描述器 的.enableTimeToLive()方法启动 TTL 功能。

```
//配置状态的TTL
StateTtlConfig ttlConfig = StateTtlConfig.newBuilder(Time.hours(1))		.setUpdateType(StateTtlConfig.UpdateType.OnCreateAndWrite)
        .setStateVisibility(StateTtlConfig.StateVisibility.ReturnExpiredIfNotCleanedUp)
.build();

valueStateDescriptor.enableTimeToLive(ttlConfig);
```

创建StateTTLConfig配置对象用到的几个配置项：

**（1）.newBuilder()** 

​	状态 TTL 配置的构造器方法，必须调用，**返回一个 Builder 之后再调用.build()方法就可以得到 StateTtlConfig 了。方法需要传入一个 Time 作为参数，这就是设定的状态生存时间**。

**（2）.setUpdateType**

​		**设置更新类型。更新类型指定了什么时候更新状态失效时间**，

```
.setUpdateType(StateTtlConfig.UpdateType.OnCreateAndWrite)
```

有两种类型：

​	① OnCreateAndWrite 表示只有创建状态和更改状态时更新失效			时间

​	② OnReadAndWrite 表示无论读写操作都会更新失效时间

**（3）.setStateVisibility( )**

​		**设置状态的可见性。所谓的“状态可见性”，是指因为清除操作并不是实时的，所以当状态过期之后还有可能存在，这时如果对它进行访问，能否正常读取得到**

```
.setStateVisibility(StateTtlConfig.StateVisibility.ReturnExpiredIfNotCleanedUp)
```

​		①NeverReturnExpired 表示从不返回过期值

​		②ReturnExpireDefNotCleanedUp 表示如果过期状态还存在，			就返回它的值

##### 9.3 算子状态（Operator State）

​		除按键分区状态（Keyed State）之外，另一大类受控状态就是算子状态（Operator State）。它只针对当前算子并行任务有效，不需要考虑不同 key 的隔离。算子状态功能不如按键分区状态丰富，应用场景较少，它的调用方法也会有一些区别。

###### 9.3.1 基本概念和特点

​		算子状态跟数据的 key 无关，所以不同 key 的数据只要被分发到同一个并行子任务， 就会访问到同一个 Operator State

​		算子状态的实际应用场景不如 Keyed State 多，一般用在 Source 或 Sink 等与外部系统连接 的算子上，或者完全没有 key 定义的场景。

###### 9.3.2 状态类型

​		算子状态也支持不同的结构类型，主要有三种：ListState、UnionListState和BroadcastState

1. 列表状态（ListState）

​		与 Keyed State 中的 ListState 一样，将状态表示为一组数据的列表。与 Keyed State 中的列表状态的区别是：在算子状态的上下文中，不会按键（key）分别处 理状态，所以每一个并行子任务上只会保留一个“列表”（list），也就是当前并行子任务上所 有状态项的集合。

​		**当算子并行度进行缩放调整时，算子的列表状态中的所有元素项会被统一收集起来，相当于把多个分区的列表合并成了一个“大列表”，然后再均匀地分配给所有并行任务。这种“均匀分配”的具体方法就是“轮询”（round-robin）。**

2. 联合列表状态（UnionListState）

​		它与常规列表状态的区别在于，算子并行度进行缩放调整时对于状态的分配方式不同。

​		在并行度调整时，常规列表状态是轮询分 配状态项，而联合列表状态的算子则会直接广播状态的完整列表。这样，并行度缩放之后的并行子任务就获取到了联合后完整的“大列表”，可以自行选择要使用的状态项和要丢弃的状态 项。这种分配也叫作“联合重组”（union redistribution）。

3. 广播状态（BroadcastState）

​		有时希望算子并行子任务都保持同一份“全局”状态，用来做统一的配置和规则设定。 这时所有分区的所有数据都会访问到同一个状态，状态就像被“广播”到所有分区一样，这种特殊的算子状态，就叫作广播状态（BroadcastState）。

###### 9.3.3 代码实现

​		状态从本质上来说就是算子并行子任务实例上的一个特殊本地变量。它的特殊之处就在于 Flink 会提供完整的管理机制，来保证它的持久化保存，以便发生故障时进行状态恢复；

​		 当发生故障重启之后，我们不能保证某个数据跟之前一样，进入到同一个并行子任务、访问同一个状态。所以Flink 无法直接判断该怎样保存和恢复状态，而是**提供了接口，让我们根据业务需求自行设计状态的快照保存（snapshot）和恢复（restore）逻辑**。

1. **CheckpointedFunction 接口**

​	对状态进行持久化保存的快照机制叫作“检查点”（Checkpoint）。于是使用 算子状态时，就需要对检查点的相关操作进行定义，实现一个 CheckpointedFunction 接口。

```
public interface CheckpointedFunction {
// 保存状态快照到检查点时，调用这个方法
void snapshotState(FunctionSnapshotContext context) throws Exception
// 初始化状态时调用这个方法，也会在恢复状态时调用
 void initializeState(FunctionInitializationContext context) throws 
Exception;
}

```

1）每次应用保存检查点做快照时，都会调用.snapshotState()方法，将状态进行外部持久化。

```
@Override
public void snapshotState(FunctionSnapshotContext context) throws Exception {
    //清空状态
    checkpointedState.clear();

    //对状态进行持久化，复制缓存的列表到列表状态
    for (Event element : bufferedElements) {
        checkpointedState.add(element);
    }
```

2）在算子任务进行初始化时，会调用. initializeState()方法。

```
@Override
public void initializeState(FunctionInitializationContext context) throws Exception {
    //定义算子状态
    ListStateDescriptor<Event> descriptor = new ListStateDescriptor<>("buffered-elements", Event.class);

    checkpointedState = context.getOperatorStateStore().getListState(descriptor);

 //如果从故障恢复，需要将ListState中的所有元素复制到列表中
    
    if (context.isRestored()){
        for (Event element : checkpointedState.get()) 		  {		
            bufferedElements.add(element);
        }
    }
}
```

案例实操：

```
public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        SingleOutputStreamOperator<Event> stream = env.addSource(new ClickSource())
                .assignTimestampsAndWatermarks(WatermarkStrategy.<Event>forBoundedOutOfOrderness(Duration.ZERO)
                        .withTimestampAssigner(new SerializableTimestampAssigner<Event>() {
                            @Override
                            public long extractTimestamp(Event element, long recordTimestamp) {
                                return element.timestamp;
                            }
                        })
                );
        stream.print("input");

        //批量缓存输出
        stream.addSink(new BufferingSink(10));

        env.execute();
    }

    //自定义实现SinkFunction
    public static class BufferingSink implements SinkFunction<Event>, CheckpointedFunction {
        //定义当前类的属性，批量
        private final int threshold;

        public BufferingSink(int threshold) {
            this.threshold = threshold;
            this.bufferedElements = new ArrayList<>();
        }

        private List<Event> bufferedElements;

        //定义一个算子状态
        private ListState<Event> checkpointedState;

        @Override
        public void invoke(Event value, Context context) throws Exception {
            bufferedElements.add(value); //缓存到列表
            //判断如果达到阈值，就批量写入
            if (bufferedElements.size() == threshold){
                //用打印到控制台模拟写入外部系统
                for (Event element : bufferedElements) {
                    System.out.println(element);

                    System.out.println("=================输出完毕=========================");
                    bufferedElements.clear();
                }
            }
        }

        @Override
        public void snapshotState(FunctionSnapshotContext context) throws Exception {
            //清空状态
            checkpointedState.clear();

            //对状态进行持久化，复制缓存的列表到列表状态
            for (Event element : bufferedElements) {
                checkpointedState.add(element);
            }
        }

        @Override
        public void initializeState(FunctionInitializationContext context) throws Exception {
            //定义算子状态
            ListStateDescriptor<Event> descriptor = new ListStateDescriptor<>("buffered-elements", Event.class);

            checkpointedState = context.getOperatorStateStore().getListState(descriptor);

            //如果从故障恢复，需要将ListState中的所有元素复制到列表中
            if (context.isRestored()){
                for (Event element : checkpointedState.get()) {
                    bufferedElements.add(element);
            }
        }
    }
}
```

##### 9.4 广播状态（Broadcast State）

###### 9.4.1 基本用法

​		一个最为普遍的应用，就是“动态配置”或者“动态规则”。我们在处理流数据时，有时会基于一些配置（configuration）或者规则（rule）。简单的配置当然可以直接读取配置文件， 一次加载，永久有效；但数据流是连续不断的，如果这配置随着时间推移还会动态变化，那该如何解决？

​		**我们可以将这动态的配置数据看作一条流，将这条流和本身要处理的数据流进行连接（connect），就可以实时地更新配置进行计算了。**

​		代码上直接调用 DataStream 的.broadcast()方法，传入一个“映射状态描述器” （MapStateDescriptor）说明状态的名称和类型，就可以得到一个“广播流”（BroadcastStream）； 进而将要处理的数据流与这条广播流进行连接（connect），就会得到“广播连接流” （BroadcastConnectedStream）。对 于 广 播 连 接 流 调 用 .process() 方 法 ， 可 以 传 入 “ 广 播 处 理 函 数 ” KeyedBroadcastProcessFunction 或者 BroadcastProcessFunction 来进行处理计算。广播处理函数 里面有两个方法.processElement()和.processBroadcastElement()

```
//行为模式流，构建广播流
        DataStreamSource<Pattern> patternStream = env.fromElements(
                new Pattern("login","pay"),
                new Pattern("login","order")
        );

        //定义广播状态描述器
        MapStateDescriptor<Void, Pattern> descriptor = new MapStateDescriptor<>("pattern", Types.VOID, Types.POJO(Pattern.class));
        BroadcastStream<Pattern> broadcastStream = patternStream.broadcast(descriptor);

        //连接两条流进行处理
        SingleOutputStreamOperator<Tuple2<String,Pattern>> matches = actionStream.keyBy(data -> data.userId)
                .connect(broadcastStream)
                .process(new PatternDetector());
```

###### 9.4.2 代码实现

```
public static void main(String[] args) throws Exception {
    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
    env.setParallelism(1);

    //用户的行为数据流
    DataStreamSource<Action> actionStream = env.fromElements(
            new Action("Alice", "login"),
            new Action("Alice", "pay"),
            new Action("Bob", "login"),
            new Action("Bob", "order")
    );

    //行为模式流，构建广播流
    DataStreamSource<Pattern> patternStream = env.fromElements(
            new Pattern("login","pay"),
            new Pattern("login","order")
    );

    //定义广播状态描述器
    MapStateDescriptor<Void, Pattern> descriptor = new MapStateDescriptor<>("pattern", Types.VOID, Types.POJO(Pattern.class));
    BroadcastStream<Pattern> broadcastStream = patternStream.broadcast(descriptor);

    //连接两条流进行处理
    SingleOutputStreamOperator<Tuple2<String,Pattern>> matches = actionStream.keyBy(data -> data.userId)
            .connect(broadcastStream)
            .process(new PatternDetector());

    matches.print();

    env.execute();

}

//定义用户行为事件和模式的POJO类
public static class Action{
    public String userId;
    public String action;

    public Action() {
    }

    public Action(String userId, String action) {
        this.userId = userId;
        this.action = action;
    }

    @Override
    public String toString() {
        return "Action{" +
                "userId='" + userId + '\'' +
                ", action='" + action + '\'' +
                '}';
    }
}

public static class Pattern{
    public String action1;
    public String action2;

    public Pattern() {
    }

    public Pattern(String action1, String action2) {
        this.action1 = action1;
        this.action2 = action2;
    }

    @Override
    public String toString() {
        return "Pattern{" +
                "action1='" + action1 + '\'' +
                ", action2='" + action2 + '\'' +
                '}';
    }
}

//实现自定义KeyBroadcastProcessFunction
public static class PatternDetector extends KeyedBroadcastProcessFunction<String,Action,Pattern,Tuple2<String,Pattern>>{
    //定义一个KeyedState，保存上一次用户的行为
    ValueState<String> preAvtionState;

    @Override
    public void open(Configuration parameters) throws Exception {
        preAvtionState = getRuntimeContext().getState(new ValueStateDescriptor<String>("last-action",String.class));
    }

    @Override
    public void processElement(Action value, KeyedBroadcastProcessFunction<String, Action, Pattern, Tuple2<String, Pattern>>.ReadOnlyContext ctx, Collector<Tuple2<String, Pattern>> out) throws Exception {
        //从广播状态中获取匹配模式
        ReadOnlyBroadcastState<Void, Pattern> patternState = ctx.getBroadcastState(new MapStateDescriptor<>("pattern", Types.VOID, Types.POJO(Pattern.class)));
        Pattern pattern = patternState.get(null);

        //获取用户上一次的行为
        String prevAction = preAvtionState.value();

        //判断是否匹配
        if (pattern != null && prevAction != null){
            if (pattern.action1.equals(prevAction) && pattern.action2.equals(value.action))
                out.collect(new Tuple2<>(ctx.getCurrentKey(),pattern));
        }

        //更新状态
        preAvtionState.update(value.action);
    }

    @Override
    public void processBroadcastElement(Pattern value, KeyedBroadcastProcessFunction<String, Action, Pattern, Tuple2<String, Pattern>>.Context ctx, Collector<Tuple2<String, Pattern>> out) throws Exception {
        //从上下文中获取广播状态，并用当前数据更新广播状态
        BroadcastState<Void, Pattern> patternState = ctx.getBroadcastState(new MapStateDescriptor<>("pattern", Types.VOID, Types.POJO(Pattern.class)));

        patternState.put(null,value);
    }
```

##### 9.5 状态持久化和状态后端

​		在 Flink 的状态管理机制中，很重要的一个功能就是**对状态进行持久化（persistence）保存，这样就可以在发生故障后进行重启恢复。**Flink 对状态进行持久化的方式，就是将当前所有分布式状态进行“快照”保存，写入一个“检查点”（checkpoint）或者保存点（savepoint）保存到外部存储系统中。

###### 9.5.1 检查点（Checkpoint）

​		有状态流应用中的检查点（checkpoint），其实就是所有任务的状态在某个时间点的一个快照（一份拷贝）。简单来讲，就是一次“存盘”，让我们之前处理数据的进度不要丢掉。在一个流应用程序运行时，Flink 会定期保存检查点，在检查点中会记录每个算子的 id 和状态；如果发生故障，Flink就会用最近一次成功保存的检查点来恢复应用的状态，重新启动处理流程，就如同“读档”一样。

​		默认情况下，检查点是被禁用的，需要在代码中手动开启。直接调用执行环境 的.enableCheckpointing()方法就可以开启检查点。

```
env.enableCheckpointing(1000);
```

###### 9.5.2 状态后端（State Backends）

​		检查点的保存离不开JobManager和TaskManager，以及外部存储系统的协调。

​		在应用进行检查点保存时，**首先会由 JobManager 向所有 TaskManager发出触发检查点的命令； TaskManger 收到之后，将当前任务的所有状态进行快照保存，持久化到远程的存储介质中； 完成之后向 JobManager 返回确认信息**。这个过程是分布式的，当 JobManger收到所有 TaskManager 的返回信息后，就会确认当前检查点成功保存。

![image-20230515203106981](C:\Users\王海涛\AppData\Roaming\Typora\typora-user-images\image-20230515203106981.png)

​		在 Flink 中，状态的存储、访问以及维护，都是由一个可插拔的组件决定的，这个组件就叫作状态后端（state backend）。

​		状态后端主要负责两件事：一是本地的状态管理，二是将检查点（checkpoint）写入远程的持久化存储。

1. 状态后端的分类

（1）哈希表状态后端（HashMapStateBackend）

​		**把状态存放在内存里**。具体实现上，哈希表状态后端在内部会直接把状态当作对象（objects），保存在 Taskmanager 的 JVM 堆（heap）上。

​		**对于检查点的保存，一般是放在持久化的分布式文件系统**（file system）中，也可以通过 配置“检查点存储”（CheckpointStorage）来另外指定。

​		**HashMapStateBackend 是将本地状态全部放入内存的，这样可以获得最快的读写速度，使 计算性能达到最佳；代价则是内存的占用。**它适用于具有大状态、长窗口、大键值状态的作业， 对所有高可用性设置也是有效的。

（2）内嵌 RocksDB 状态后端

​		**RocksDB 是一种内嵌的 key-value 存储介质，可以把数据持久化到本地硬盘。**

​		配置 EmbeddedRocksDBStateBackend 后，会将处理中的数据全部放入 RocksDB 数据库中，RocksDB 默认存储在 TaskManager 的本地数据目录里。

​		与 HashMapStateBackend 直接在堆内存中存储对象不同，这种方式下状态主要是放在 RocksDB 中的。数据被存储为序列化的字节数组（Byte Arrays），读写操作需要序列化/反序列化，因此状态的访问性能要差一些。

2. 如何选择正确的状态后端

​		HashMap 和 RocksDB 两种状态后端最大的区别，就在于本地状态存放在哪里：前者是内存，后者是 RocksDB。

​		HashMapStateBackend 是内存计算，读写速度非常快；但是，状态的大小会受到集群可用内存的限制，如果应用的状态随着时间不停地增长，就会耗尽内存资源。

​		而 RocksDB 是硬盘存储，所以可以根据可用的磁盘空间进行扩展，而且是唯一支持增量 检查点的状态后端，所以它非常适合于超级海量状态的存储。不过由于每个状态的读写都需要做序列化/反序列化，而且可能需要直接从磁盘读取数据，这就会导致性能的降低，平均读写性能要比 HashMapStateBackend 慢一个数量级。

3. 状态后端的配置

（1）配置默认的状态后端

​		在 flink-conf.yaml 中，可以使用 state.backend 来配置默认状态后端。**配置项的可能值为 hashmap，这样配置的就是 HashMapStateBackend；也可以是 rocksdb， 277 这样配置的就是 EmbeddedRocksDBStateBackend。**

```
# 默认状态后端
state.backend: hashmap
# 存放检查点的文件路径
state.checkpoints.dir: hdfs://namenode:40010/flink/checkpoints
```

（2）为每个作业单独配置状态后端，代码如下：

```
//设置HashMapStateBackend
env.setStateBackend(new HashMapStateBackend());

//设置EmbeddedRocksDBStateBackend
env.setStateBackend(new EmbeddedRocksDBStateBackend());
```

