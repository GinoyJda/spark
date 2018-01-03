import org.apache.spark.api.java.function.*;
import scala.Tuple2;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.regex.Pattern;

public class WordCount {
    private static final Pattern SPACE = Pattern.compile(" ");

    public static void main(String[] args) throws Exception {

        if (args.length < 1) {
            System.err.println("Usage: JavaWordCount <file>");
            System.exit(1);
        }

        /**
         * 对于所有的spark程序所言，要进行所有的操作，首先要创建一个spark上下文。
         * 在创建上下文的过程中，程序会向集群申请资源及构建相应的运行环境。
         * 设置spark应用程序名称
         * 创建的 sarpkContext 唯一需要的参数就是 sparkConf，它是一组 K-V 属性对。
         */
        SparkConf sparkConf = new SparkConf().setAppName("WordCount").setMaster("local");
        JavaSparkContext ctx = new JavaSparkContext(sparkConf);

        /**
         * 利用textFile接口从文件系统中读入指定的文件，返回一个RDD实例对象。
         * RDD的初始创建都是由SparkContext来负责的，将内存中的集合或者外部文件系统作为输入源。
         * RDD：弹性分布式数据集，即一个 RDD 代表一个被分区的只读数据集。一个 RDD 的生成只有两种途径，
         * 一是来自于内存集合和外部存储系统，另一种是通过转换操作来自于其他 RDD，比如 Map、Filter、Join，等等。
         * textFile()方法可将本地文件或HDFS文件转换成RDD，读取本地文件需要各节点上都存在，或者通过网络共享该文件
         *读取一行
         */
        JavaRDD<String> lines = ctx.textFile(args[0], 1);
        /**
         *
         * new FlatMapFunction<String, String>两个string分别代表输入和输出类型
         * Override的call方法需要自己实现一个转换的方法，并返回一个Iterable的结构
         *
         * flatmap属于一类非常常用的spark函数，简单的说作用就是将一条rdd数据使用你定义的函数给分解成多条rdd数据
         * 例如，当前状态下，lines这个rdd类型的变量中，每一条数据都是一行String，我们现在想把他拆分成1个个的词的话，
         * 可以这样写 ：
         */
        //flatMap与map的区别是，对每个输入，flatMap会生成一个或多个的输出，而map只是生成单一的输出
        //用空格分割各个单词,输入一行,输出多个对象,所以用flatMap
        JavaRDD<String> words = lines.flatMap(new FlatMapFunction<String, String>() {
            public Iterator<String> call(String s) {
                return Arrays.asList(SPACE.split(s)).iterator();
            }
        });
        /**
         * map 键值对 ，类似于MR的map方法
         * pairFunction<T,K,V>: T:输入类型；K,V：输出键值对
         * 表示输入类型为T,生成的key-value对中的key类型为k,value类型为v,对本例,T=String, K=String, V=Integer(计数)
         * 需要重写call方法实现转换
         */
        JavaPairRDD<String, Integer> ones = words.mapToPair(new PairFunction<String, String, Integer>() {
            //scala.Tuple2<K,V> call(T t)
            //Tuple2为scala中的一个对象,call方法的输入参数为T,即输入一个单词s,新的Tuple2对象的key为这个单词,计数为1
            public Tuple2<String, Integer> call(String s) {
                return new Tuple2<String, Integer>(s, 1);
            }
        });
        //A two-argument function that takes arguments
        // of type T1 and T2 and returns an R.
        /**
         * 调用reduceByKey方法,按key值进行reduce
         *  reduceByKey方法，类似于MR的reduce
         *  要求被操作的数据（即下面实例中的ones）是KV键值对形式，该方法会按照key相同的进行聚合，在两两运算
         *  若ones有<"one", 1>, <"one", 1>,会根据"one"将相同的pair单词个数进行统计,输入为Integer,输出也为Integer
         *输出<"one", 2>
         */
        JavaPairRDD<String, Integer> counts = ones.reduceByKey(new Function2<Integer, Integer, Integer>() {
            //reduce阶段，key相同的value怎么处理的问题
            public Integer call(Integer i1, Integer i2) {
                return i1 + i2;
            }
        });
        //备注：spark也有reduce方法，输入数据是RDD类型就可以，不需要键值对，
        // reduce方法会对输入进来的所有数据进行两两运算

        /**
         * collect方法用于将spark的RDD类型转化为我们熟知的java常见类型
         */
        List<Tuple2<String, Integer>> output = counts.collect();
        for (Tuple2<?,?> tuple : output) {
            System.out.println(tuple._1() + ": " + tuple._2());
        }
        ctx.stop();
    }

    /**
     * Map主要是对数据进行处理，不进行数据集的增减
     *
     * 本案例实现，打印所有数据
     *
     * @param rdd
     */

    private static void testSparkCoreApiMap(JavaRDD<String> rdd){
        JavaRDD<String> logData1=rdd.map(new Function<String,String>(){
            public String call(String s){
                return s;
            }
        });
        List list = logData1.collect();
        for (int i = 0; i < list.size(); i++) {
            System.out.println(list.get(i));


        }
    }

   /*
    * filter主要是过滤数据的功能
    * 本案例实现：过滤含有a的那行数据
    */
    private static void testSparkCoreApiFilter(JavaRDD<String> rdd){
        JavaRDD<String> logData1=rdd.filter(new Function<String,Boolean>(){
            public Boolean call(String s){
                return (s.split(" "))[0].equals("a");
            }
        });
        List list = logData1.collect();
        for (int i = 0; i < list.size(); i++) {
            System.out.println(list.get(i));
        }
    }

    /**
     * flatMap  用户行转列
     * 本案例实现：打印所有的字符
     *
     */
    private static void testSparkCoreApiFlatMap(JavaRDD<String> rdd){
        JavaRDD<String> words=rdd.flatMap(
                new FlatMapFunction<String, String>() {
                    public Iterator<String> call(String s) throws Exception {
                        return Arrays.asList(s.split(" ")).iterator();
                    }
                }
        );
        List list = words.collect();
        for (int i = 0; i < list.size(); i++) {
            System.out.println(list.get(i));


        }

    }



    /**
     * testSparkCoreApiUnion
     * 合并两个RDD
     * @param rdd
     */
    private static void testSparkCoreApiUnion(JavaRDD<String> rdd){
        JavaRDD<String> unionRdd=rdd.union(rdd);
        unionRdd.foreach(new VoidFunction<String>(){
            public void call(String lines){
                System.out.println(lines);
            }
        });
    }


    /**
     * testSparkCoreApiDistinct Test
     * 对RDD去重
     * @param rdd
     */
    private static void testSparkCoreApiDistinct(JavaRDD<String> rdd){
        JavaRDD<String> unionRdd=rdd.union(rdd).distinct();
        unionRdd.foreach(new VoidFunction<String>(){
            public void call(String lines){
                System.out.println(lines);
            }
        });
    }


    /**
     * testSparkCoreApiMaptoPair Test
     * 把RDD映射为键值对类型的数据
     * @param rdd
     */
    private static void testSparkCoreApiMaptoPair(JavaRDD<String> rdd){

        JavaPairRDD<String, Integer> pairRdd=rdd.mapToPair(new PairFunction<String,String,Integer>(){
            public Tuple2<String, Integer> call(String t) throws Exception {
                String[] st=t.split(" ");
                return new Tuple2(st[0], st[1]);
            }

        });

        pairRdd.foreach(new VoidFunction<Tuple2<String, Integer>>(){
            public void call(Tuple2<String, Integer> t) throws Exception {
                System.out.println(t._2());

            }
        });

    }



    /**
     * testSparkCoreApiGroupByKey Test
     * 对键值对类型的数据进行按键值合并
     * @param rdd
     */

    private static void testSparkCoreApiGroupByKey(JavaRDD<String> rdd){

        JavaPairRDD<String, Integer> pairRdd=rdd.mapToPair(new PairFunction<String,String,Integer>(){
            public Tuple2<String, Integer> call(String t) throws Exception {
                String[] st=t.split(" ");
                return new Tuple2(st[0], Integer.valueOf(st[1]));
            }

        });

        JavaPairRDD<String, Iterable<Integer>> pairrdd2= pairRdd.union(pairRdd).groupByKey();
        pairrdd2.foreach(new VoidFunction<Tuple2<String, Iterable<Integer>>>(){
            public void call(Tuple2<String, Iterable<Integer>> t) throws Exception {
                Iterable<Integer> iter = t._2();
                for (Integer integer : iter) {
                    System.out.println(integer);
                }


            }
        });
    }


    /**
     * testSparkCoreApiReduceByKey
     * 对键值对进行按键相同的对值进行操作
     * @param rdd
     */
    private static void testSparkCoreApiReduceByKey(JavaRDD<String> rdd){

        JavaPairRDD<String, Integer> pairRdd=rdd.mapToPair(new PairFunction<String,String,Integer>(){
            public Tuple2<String, Integer> call(String t) throws Exception {
                String[] st=t.split(" ");
                return new Tuple2(st[0], Integer.valueOf(st[1]));
            }

        });

        JavaPairRDD<String, Integer> pairrdd2 =pairRdd.union(pairRdd).reduceByKey(
                new Function2<Integer,Integer,Integer>(){
                    public Integer call(Integer v1, Integer v2) throws Exception {

                        return v1+v2;
                    }
                }
        ).sortByKey() ;

        pairrdd2.foreach(new VoidFunction<Tuple2<String, Integer>>(){
            public void call(Tuple2<String, Integer> t) throws Exception {
                System.out.println(t._2());

            }
        });
    }


    /**
     * testSparkCoreApiReduce
     * 对RDD进行递归调用
     * @param rdd
     */
    private static void testSparkCoreApiReduce(JavaRDD<String> rdd){
        //由于原数据是String，需要转为Integer才能进行reduce递归
        JavaRDD<Integer> rdd1=rdd.map(new Function<String,Integer>(){

            public Integer call(String v1) throws Exception {
            // TODO Auto-generated method stub
                return Integer.valueOf(v1.split(" ")[1]);
            }
        });
        Integer a= rdd1.reduce(new Function2<Integer,Integer,Integer>(){
            public Integer call(Integer v1,Integer v2) throws Exception {
                return v1+v2;
            }
        });
        System.out.println(a);

    }

}

