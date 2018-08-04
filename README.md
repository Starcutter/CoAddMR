# 基于MapReduce的天文图像叠加

项目报告，2018年8月

## 项目目标

### 问题与目标

斯隆数字化巡天（简称SDSS）是使用位于新墨西哥州阿帕奇点天文台的2.5米口径望远镜进行的红移巡天项目，该项目计划观测25%的天空，获取超过一百万个天体的多色测光资料和光谱数据。测光系统配以分别位于u、g、r、i、z波段的五个滤镜，对`Stripe 82`天区拍摄了相同大小的巨量相互重叠的天文照片。

该项目得到的巨量天文图像存在着如下问题：

* 照片的大小固定，显示的范围固定
* 限制于单张照片曝光以及图像中存在噪声，导致星体模糊或是过暗

本项目致力于在天文望远镜拍摄的图像基础上，通过图像加权叠加等处理，提供更加灵活的范围查询；降低噪声，提供给定范围内更高质量的图像输出。

在查阅相关资料后，小组决定复现`Wiley K`[1]等人所实现的处理系统。论文中提及，该处理系统采用旧的SDSS数据集，通过图像的加权叠加，实现的效果如下（左图为原始图像，右图为处理后的图像）：

<img src="https://raw.githubusercontent.com/Starcutter/CoAddMR/master/images/paper_img.png">

### 主要功能

经过分析，小组完成了如下几点主要的功能：

* 给定查询范围内多张图片的截取与拼接
* 图像加权叠加
* 应用PSF算法减轻天文望远镜拍摄过程中由于运动产生的模糊问题

## 与分布式的关系

### 数据存储

本项目选取了`Stripe 82`天区的数据集，其中赤经（RA）范围在37 ~ 40度之间。总大小约为107GB（bz2压缩）。实验将该数据存储在`HDFS`上，方便后续的`MapReduce`计算过程。

### 叠加计算

本次实验的核心算法为图像的叠加计算，根据论文中提及的算法，小组采用`MapReduce`框架。其中`Map`负责截取给定查询范围与数据集中图片的相交部分，并进行像素级的映射， 生成可供叠加的图片；而`Reduce`则负责图像的叠加工作。具体的原理与设计参见后文。

此外，为了支持并行查询搜索，系统支持多个`Reduce`任务并行。

## 项目设计与理论基础

### 原始数据组织

原始数据是天文台相机拍摄的大量FITS格式的图像，要设计项目读取与预处理数据的方法，则必须了解它的组织形式。

<img src="https://raw.githubusercontent.com/Starcutter/CoAddMR/master/images/camera.png" width="40%">

如图是天文台相机的示意图，是一个 $6 \times 5$ 的阵列。Stripe 82 天区是天赤道上的东西向条带，相机随地球自转，向图中的下方运动。横向6列的相机分别拍摄赤纬不同的区域，而纵向的5行相机分别拍摄不同的光谱波段（表示为5个字母：ugriz）。由于这一硬件形式，原始数据的路径与文件名需要标示必要的信息，如：

```
data/4797/6/frame-g-004797-6-0018.fits.bz2
```

其中，`4797`是 run 的编号（一个 run 代表一次扫描），`6`指第6列的相机（称为 camcol），`g`是波段（band），`18`表示是这次run的第18张图（field）。

### FITS文件格式

本项目所使用的数据为 FITS 格式。每一个`.fits`文件都包含三个模块：`Header-Data Units`、`Header`、`Data`。

1. Header-Data Units

   一个FITS文件由一个或多个Header-Data Units（HDU）组成，每一个HDU包含Header和Data两部分。在次项目中，我们只用到了第一个HDU，即二维图像数据。

2. Header

   该部分包含了当前HDU的meta-data，包括图像的赤经、赤纬范围，图像的大小等信息。

3. Data

   这一部分是float类型的二维图像数据，其含义为光通密度而非像素值，在后续的处理中需要进行归一化。

对于FITS格式文件的处理，我们采用了Java自带的`nom-tam-fits`库，可以较为简易地实现FITS文件的读入、根据`key`值从Header中提取出赤经、赤纬范围等关键信息，以及最终输出为FITS格式的文件。具体的过程如下：

* 根据`key = CRPIX1`提取出参考点的赤纬像素位置`referRa`，`key = CRPIX2`提取出参考点的赤经像素位置`referDec`
* 根据`key = CRVAL1`提取出参考点的赤经坐标`referY`，`key = CRVAL2`提取出参考点的赤纬坐标`referX`
* 根据`key = CD1_2`提取出每个像素的赤经宽度`raDeg`，`key = CD2_1`提取出每个像素的赤纬宽度`decDeg`
* 所读入的图片赤经、赤纬范围计算如下：
  * $maxRa = referRa + (1489 - referY) × raDeg$
  * $minRa = referRa - referY × raDeg $
  * $maxDec = referDec + (2048 - referX) × decDeg$
  * $minDec = referDec - referX × decDeg$
* 统计每一个`camcol`的赤纬范围，以方便后续与查询范围比较

### 项目总体架构

基于我们的查询需求以及原始数据情况，系统设计为下面的流程：

1. 数据预处理

   从大量的FITS文件中，读取元信息、图像数据，然后重新组织成更加利于 Hadoop 处理的形式。

2. 输入预过滤

   根据查询输入及少量先验信息，缩小 MapReduce 过程的输入数据量。

3. Mapper

   计算候选图像与查询范围的相交情况，截取相交的部分，然后映射到最终的输出图像的坐标系下，再发射给 Reducer。

4. Reducer

   每个 Reducer 处理一条查询。接收到的都是经过映射后的待叠加图像，Reducer 将它们加权叠加（目前是求平均）形成要输出的二维矩阵数据。

5. 输出

   Reducer 输出的矩阵不是直接的像素值，需要经过 scale 以落在0-255之间。这里还可以执行PSF以获得更好的图像质量。最后，可以选择输出为图片还是FITS文件。

用参考文献[1]中的示意图说明整体架构：

<img src="https://raw.githubusercontent.com/Starcutter/CoAddMR/master/images/mapreduce.png" width="80%">

## 项目实现

### 数据预处理

涉及的类：`FITSCombineFileInputFormat`,  `FITS2Sequence`,  `SequenceFITSOutputFormat`

原始数据总共有33900个文件，而 Hadoop 不擅长处理大量的小文件。所以，我们首先把原始的数据转换成少量的 Sequence File，有利于显著降低查询时的 Mapper 数量，提高性能。

<img src="https://raw.githubusercontent.com/Starcutter/CoAddMR/master/images/seq.png" width="50%">

在查询时的预过滤过程中，仅有 band 与 camcol 是重要的，所以数据也是按照这两个参数被重新组织的。如图，理想上会将数据重组为30个 Sequence File，就和相机的阵列形式一致。实际中由于我们硬件条件的落后，Reducer无法承受大量数据，于是进一步将每个 Sequence File 又分成6个，总共180个文件。

`FITSCombineFileInputFormat`继承自`CombineFileInputFormat`，使多个文件对应一个Mapper，而不是一一对应，可以减少 Mapper 数量。对于每一个原始文件，发射的 Key 是形式如`camcol-band-randint`的字符串，`randint`是0到5的随机整数，目的是前述的进一步分裂文件；Value 是文件路径。

`FITS2Sequence`中，Mapper不做任何事。Reducer接收到如上形式的 K-V 对后，按照路径打开文件，从中读取第一个 HDU 的 Header，计算这张图片的赤经-赤纬范围，形成如下字符串形式的Key：`camcol-band-randint-minRa,maxRa,minDec,maxDec`；读取 $2048 \times 1489$ 的单精度浮点数图像矩阵，形成`ArrayWritable`实例的 Value。

`SequenceFITSOutputFormat`继承自`SequenceFileOutputFormat`，将 K-V 对输出为 Sequence File 格式，唯一重写的功能是，在输出完毕后，读取每个文件的一条记录，以此为依据将文件重命名为`camcol-band-randint`形式，取代 Hadoop 的默认命名。

程序中设置了Reducer数量为180个，并重写了`Partitioner`的规则，保证一个 Reducer 仅处理1种 Key，并输出到对应的单个文件中。

### 查询过程

涉及的类：

- 没有经过数据预处理的旧版查询：`FITSInputFormat`,  `Mosaic`,  `ImageOutputFormat`或`FITSOutputFormat`,  `Deblur`
- 利用了预处理后数据的新版查询：`SequenceFITSInputFormat`,  `MosaicNew`,   `ImageOutputFormat`或`FITSOutputFormat`,  `Deblur`

下面叙述的是新版的查询。

#### 1. 输入预过滤

`SequenceFITSInputFormat`继承自`SequenceFileInputFormat`，仅仅重写了`listStatus()`函数，实现基于文件名的简单预过滤。

用户输入的查询范围大多数情况下不是很大，相机阵列的6列也都有比较确定的覆盖范围（赤纬），于是我们可以根据文件名做预过滤，只处理特定 camcol 与用户指定波段的数据。我们根据预先的统计，先验地确定了每个 camcol 的最大范围（赤纬的最小值与最大值），然后与查询的赤纬范围相对比，仅留下那些重合的 camcol。

这一预过滤应当是保证不遗漏的，所以我们尽可能大地规定了 camcol 的先验范围。当然，由于只考虑了赤纬，所以过滤后的文件中依然存在“假阳性”数据，就是那些虽然在重叠的 camcol 中，但赤经范围完全与查询范围不相交的图像。这是数据预处理所决定的，只能在后续的 Mapper 中再次过滤。

#### 2. Mapper

由于读取的是预处理后的 Sequence File，Mapper 拿到的 K-V 对遵循着预处理的 Reducer 输出的形式：Key 是`camcol-band-randint-minRa,maxRa,minDec,maxDec`，Value 是`ArrayWritable`实例的图像数据。

系统支持并行查询，也即同时处理多条查询请求。这就要求 Mapper 对于收到的每张图像，都需要拿去考察所有的查询请求。下面的过程对每条查询都要做一遍：

先执行前述的再过滤：判断当前图像的赤经-赤纬范围与这条查询是否相交。如果相交，那么再执行映射算法：我们规定最终输出图像的宽度固定为2048，高度由查询范围的长宽比确定，那么首先截取出当前图像与查询范围相交的部分，计算它在最终输出图像上的位置与大小，然后将它整个映射到输出图像上那个位置去。这个映射也就是一种图像放缩，输出图像上的每个像素，是由原图对应位置周围的若干像素插值而成的。

最后，将本次查询编号作为 Key，映射后的图像数据作为 Value，发射给 Reducer。

<img src="https://raw.githubusercontent.com/Starcutter/CoAddMR/master/images/map_shape.png" width="50%">

#### 3. Reducer

系统接收输入时，设置了 Reducer 数量为查询总数。如果有 $n$ 个查询，那么就有 $n$ 个 Reducer 在并行执行。Mapper 发射的 Key 是查询编号，这意味着一条查询所需要的全部图像数据，最终都会汇集到一个 Reducer 内，就可以由这个 Reducer 来执行叠加了。目前的叠加只是简单的平均，理论上这里有很多种决定各图像权重的算法，本次没有实现。

<img src="https://raw.githubusercontent.com/Starcutter/CoAddMR/master/images/reduce_shape.png" width="50%">

#### 4. 输出为图像

原始数据的浮点数矩阵并不是像素矩阵，而是光通密度值，所以经过上面的处理，输出的依然不是最终的像素值。这里需要一个 scale 函数，来把这些值限制在0-255之内，并最终输出为图像。

我们首先将数据归一化，然后使用了如下的 scale 函数：
$$
y = \frac{\log(ax+1)}{\log(a)},\ x \in [0, 1]
$$
其中参数 $a$ 选择为1000. 实践证明这样能得到比较好的效果。

当然我们也可以直接将 scale 前的数据输出为FITS文件，只需要借助前面提到的`nom-tam-fits`库。

### PSF

简述一下PSF算法。在图片拍摄的过程中会由于运动产生一定的运动模糊，与此同时，泊松效应也会让一些通过凸透镜拍摄的图片，如显微镜，天文望远镜等产生衍射。针对于这样的问题，PSF算法首先对图片进行FFT变换。FFT可以勾勒出图片的边缘，对于泊松效应产生的光晕式模糊，在这里已经可以被弱化。对于运动轨迹产生的模糊，我们认为他在时域上的变换是如正弦函数波一般有规律的。因此，当他变换到频域谱上的时候，产生的是一种较为独立的波形。当我们提取出此波形，并还原到时域上的时候，我们可以对还原出来的图片做截取工作。换句话说，就是只截取靠近原点部分的波形，而忽略掉由于此波形的移动而变换出来的波形。这样，运动模糊部分的图像就可以被抹去，我们也就达到了还原出相对真实图片的效果。

## 系统的测试与评价

因为本项目的目标是合成图像，所以这部分只能展示最终的效果来说明它确实有效。星星比较暗淡，可能需要上调屏幕亮度来看清楚。

`frame-g-000094-6-0427`的原始图像是这样的：

<img src="https://raw.githubusercontent.com/Starcutter/CoAddMR/master/images/frame-g-000094-6-0427.png">

系统对这个范围执行叠加，输出的图像是这样的：

<img src="https://raw.githubusercontent.com/Starcutter/CoAddMR/master/images/mosaic-frame-g-000094-6-0427.png">

可以看到，大量图像的堆叠可以克服单张图片的曝光程度限制，让星体更加清晰，并在保持图像质量的前提下，显示出更多暗淡的星体。

`frame-g-000094-3-0419`的原图，中间有颗亮星：

<img src="https://raw.githubusercontent.com/Starcutter/CoAddMR/master/images/frame-g-000094-3-0419.png">

对这张图的周边稍大范围进行查询，输出：

<img src="https://raw.githubusercontent.com/Starcutter/CoAddMR/master/images/mosaic-frame-g-000094-3-0419.png">

尝试使用系统对几张原图组成的大范围区域进行拼接，两个结果如下：

<img src="https://raw.githubusercontent.com/Starcutter/CoAddMR/master/images/large_area.png">

<img src="https://raw.githubusercontent.com/Starcutter/CoAddMR/master/images/mosaic-4in1.png">



运行环境：由于中途失去了集群，我们最终在单机上配置伪分布式 Hadoop 系统来运行项目。软硬件配置如下：

```
Intel(R) Core(TM) i7-3770 CPU @ 3.40GHz, 8 processors, 16GB RAM
Ubuntu 18.04 LTS x86_64
openjdk version "1.8.0_171"
Hadoop 2.9.1
```

运行时间：

- 数据预处理：约 7 hrs
- 查询：根据查询范围大小不同，时间相差很大。如果与原始数据一张图的大小相近，一次查询大约需要10 min.

## 总结与展望



## 参考文献

[1] Wiley K, Connolly A, Gardner J, et al. Astronomy in the cloud: using mapreduce for image co-addition[J]. Publications of the Astronomical Society of the Pacific, 2011, 123(901): 366. 