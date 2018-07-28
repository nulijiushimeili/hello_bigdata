package spark01.sql

/**
  * scala class 和 case class 的区别:
  *
  * 在Scala中存在case class，它其实就是一个普通的class。
  * 但是它又和普通的class略有区别，如下：
  *
  *  　　 1、初始化的时候可以不用new，当然你也可以加上，普通类一定需要加new；
  *      2. toString 方法实现的更漂亮
  *      3. 默认实现了equals和hashcode方法
  *      4. 默认是可以序列化的,也就是实现了Serializable
  *      5. 自动从scala.Product中继承了一些函数
  *      6. case class的构造函数是public级别的,可以直接访问
  *      7. 支持模式匹配
  */
case class Student(id:Int,stuname:String,stuage:Int,subject_no:Int) {

}

